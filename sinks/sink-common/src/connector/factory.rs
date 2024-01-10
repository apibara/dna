use std::marker::PhantomData;

use apibara_core::{filter::Filter, node::v1alpha2::Cursor};
use apibara_script::Script;
use apibara_sdk::{Configuration, DataMessage, ImmutableDataStream};
use error_stack::{Result, ResultExt};
use prost::Message;
use serde::Serialize;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    error::SinkError, sink::Sink, Context, CursorAction, DisplayCursor, PersistedState,
    SinkErrorReportExt, SinkErrorResultExt,
};

use super::{
    sink::SinkWithBackoff,
    state::StateManager,
    stream::{StreamAction, StreamClientFactory},
};

pub struct FactoryConnector<S, F, B>
where
    S: Sink + Send + Sync,
    F: Filter,
    B: Message + Default + Serialize,
{
    script: Script,
    sink: SinkWithBackoff<S>,
    stream_client_factory: StreamClientFactory,
    state_manager: StateManager,
    ending_block: Option<u64>,
    starting_configuration: Configuration<F>,
    needs_invalidation: bool,
    skip_factory: bool,
    _data: PhantomData<B>,
}

impl<S, F, B> FactoryConnector<S, F, B>
where
    S: Sink + Send + Sync,
    F: Filter,
    B: Message + Default + Serialize,
{
    pub fn new(
        script: Script,
        sink: SinkWithBackoff<S>,
        ending_block: Option<u64>,
        starting_configuration: Configuration<F>,
        stream_client_factory: StreamClientFactory,
        state_manager: StateManager,
    ) -> Self {
        Self {
            script,
            sink,
            ending_block,
            starting_configuration,
            stream_client_factory,
            state_manager,
            needs_invalidation: false,
            skip_factory: false,
            _data: Default::default(),
        }
    }

    pub async fn start(&mut self, ct: CancellationToken) -> Result<(), SinkError> {
        self.state_manager.lock(ct.clone()).await?;

        let mut state = self.state_manager.get_state::<F>().await?;

        if state.cursor.is_some() {
            info!(cursor = %DisplayCursor(&state.cursor), "restarting from last cursor");
            self.handle_invalidate(state.cursor.clone(), &mut state, ct.clone())
                .await?;
        }

        let mut data_stream = self.start_stream_with_state(&state).await?;

        self.needs_invalidation = false;
        self.skip_factory = false;

        let mut ret = Ok(());
        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    break;
                }
                maybe_message = data_stream.try_next() => {
                    match maybe_message {
                        Err(err) => {
                            ret = Err(err).map_err(|err| err.temporary("data stream error"));
                            break;
                        }
                        Ok(None) => {
                            ret = Err(SinkError::Temporary)
                                .attach_printable("data stream closed");
                            break;
                        }
                        Ok(Some(message)) => {
                            let (cursor_action, stream_action) = self.handle_message(message, &mut state, ct.clone()).await?;
                            self.skip_factory = false;
                            self.state_manager.put_state(state.clone(), cursor_action).await?;
                            match stream_action {
                                StreamAction::Stop => {
                                    break;
                                }
                                StreamAction::Reconnect => {
                                    self.skip_factory = true;
                                    data_stream = self.start_stream_with_state(&state).await?;
                                }
                                StreamAction::Continue => {}
                            }
                        }
                    }
                }
            }
        }

        self.sink.cleanup().await?;

        self.state_manager.cleanup().await?;

        ret
    }

    async fn start_stream_with_state(
        &mut self,
        state: &PersistedState<F>,
    ) -> Result<ImmutableDataStream<B>, SinkError> {
        let mut configuration = self.starting_configuration.clone();
        // Force batch size to 1 for factory mode.
        configuration.batch_size = 1;

        if let Some(cursor) = state.cursor.clone() {
            configuration.starting_cursor = Some(cursor);
        }

        if let Some(additional_filter) = state.filter.clone() {
            debug!("start consume factory + data stream");
            let data_stream = self
                .stream_client_factory
                .new_stream_client()
                .await?
                .start_stream_immutable_with_additional_filters::<F, B>(
                    configuration,
                    vec![additional_filter],
                )
                .await
                .map_err(|err| err.temporary("failed to start stream"))?;

            Ok(data_stream)
        } else {
            debug!("start consume factory stream only");

            let data_stream = self
                .stream_client_factory
                .new_stream_client()
                .await?
                .start_stream_immutable::<F, B>(configuration)
                .await
                .map_err(|err| err.temporary("failed to start stream"))?;

            Ok(data_stream)
        }
    }

    async fn handle_message(
        &mut self,
        message: DataMessage<B>,
        state: &mut PersistedState<F>,
        ct: CancellationToken,
    ) -> Result<(CursorAction, StreamAction), SinkError> {
        match message {
            DataMessage::Data {
                cursor,
                end_cursor,
                finality,
                batch,
            } => {
                info!(
                    block = end_cursor.order_key,
                    status = %finality,
                    "handle block batch"
                );
                let mut batch = batch.into_iter();
                let context = Context {
                    cursor,
                    end_cursor,
                    finality,
                };

                let block_end_cursor = context.end_cursor.order_key;
                if let Some(ending_block) = self.ending_block {
                    if block_end_cursor >= ending_block {
                        info!(
                            block = block_end_cursor,
                            ending_block = ending_block,
                            "ending block reached"
                        );
                        return Ok((CursorAction::Persist, StreamAction::Stop));
                    }
                }

                if let Some(factory_data) = batch.next() {
                    if let Some(filter) = self
                        .handle_factory(&context, factory_data, ct.clone())
                        .await?
                    {
                        state.cursor = context.cursor;
                        state.filter = if let Some(mut existing) = state.filter.take() {
                            existing.merge_filter(filter);
                            Some(existing)
                        } else {
                            Some(filter)
                        };

                        return Ok((CursorAction::Skip, StreamAction::Reconnect));
                    }
                }
                if let Some(data) = batch.next() {
                    self.handle_data(context, data, state, ct).await
                } else {
                    Ok((CursorAction::Skip, StreamAction::Continue))
                }
            }
            DataMessage::Invalidate { cursor } => {
                info!(block = %DisplayCursor(&cursor), "handle invalidate");
                self.handle_invalidate(cursor, state, ct).await
            }
            DataMessage::Heartbeat => {
                self.sink.handle_heartbeat().await?;
                self.state_manager.heartbeat().await?;
                Ok((CursorAction::Skip, StreamAction::Continue))
            }
        }
    }

    async fn handle_factory(
        &mut self,
        context: &Context,
        data: B,
        ct: CancellationToken,
    ) -> Result<Option<F>, SinkError> {
        debug!(block = context.end_cursor.order_key, "handle factory");

        if context.finality.is_pending() {
            return Ok(None);
        }

        if self.skip_factory {
            return Ok(None);
        }

        // fatal error since if the sink is restarted it will receive the same data again.
        let json_data = serde_json::to_value(data).fatal("failed to serialize factory data")?;

        let result = self
            .script
            .factory::<F>(json_data)
            .await
            .map_err(|err| err.fatal("failed to transform batch data"))?;

        if let Some(data) = result.data {
            self.sink.handle_data(context, &data, ct).await?;
        }

        Ok(result.filter)
    }

    async fn handle_data(
        &mut self,
        context: Context,
        data: B,
        state: &mut PersistedState<F>,
        ct: CancellationToken,
    ) -> Result<(CursorAction, StreamAction), SinkError> {
        if self.needs_invalidation {
            self.handle_invalidate(context.cursor.clone(), state, ct.clone())
                .await?;
            self.needs_invalidation = false;
        }

        // fatal error since if the sink is restarted it will receive the same data again.
        let json_data = serde_json::to_value(data).fatal("failed to serialize batch data")?;
        let json_batch = vec![json_data];
        let data = self
            .script
            .transform(json_batch)
            .await
            .map_err(|err| err.fatal("failed to transform batch data"))?;

        info!(block = context.end_cursor.order_key, "handle data");
        let mut action = self.sink.handle_data(&context, &data, ct).await?;

        // If it's pending, don't store the cursor.
        if context.finality.is_pending() {
            self.needs_invalidation = true;
            action = CursorAction::Skip;
        }

        state.cursor = Some(context.end_cursor.clone());

        Ok((action, StreamAction::Continue))
    }

    async fn handle_invalidate(
        &mut self,
        cursor: Option<Cursor>,
        state: &mut PersistedState<F>,
        ct: CancellationToken,
    ) -> Result<(CursorAction, StreamAction), SinkError> {
        self.sink.handle_invalidate(&cursor, ct).await?;
        state.cursor = cursor;
        Ok((CursorAction::Persist, StreamAction::Continue))
    }
}
