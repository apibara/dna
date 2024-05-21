use std::marker::PhantomData;

use apibara_dna_protocol::{
    client::{DataStream, StreamMessage},
    dna::{common::Cursor, stream::StreamDataRequest},
};
use apibara_indexer_script::Script;
use error_stack::{Result, ResultExt};
use prost::Message;
use serde::Serialize;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    error::SinkError, filter::Filter, sink::Sink, Context, CursorAction, DisplayCursor,
    NetworkFilterOptions, PersistedState, SinkErrorReportExt, SinkErrorResultExt,
    StreamConfigurationOptions,
};

use super::{
    sink::SinkWithBackoff,
    state::StateManager,
    stream::{StreamAction, StreamClientFactory},
    system_message::log_system_message,
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
    starting_configuration: StreamConfigurationOptions,
    needs_invalidation: bool,
    skip_factory: bool,
    _filter: PhantomData<F>,
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
        starting_configuration: StreamConfigurationOptions,
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
            _filter: Default::default(),
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
                    info!("sink stopped: cancelled");
                    break;
                }
                maybe_message = data_stream.try_next() => {
                    match maybe_message {
                        Err(err) => {
                            ret = Err(err).temporary("data stream error");
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
    ) -> Result<DataStream, SinkError> {
        let configuration = self.starting_configuration.clone();

        let starting_cursor = if let Some(cursor) = state.cursor.clone() {
            Some(cursor)
        } else {
            configuration.starting_block.map(|order_key| Cursor {
                order_key,
                unique_key: vec![],
            })
        };

        let serialized_filter = match configuration.filter {
            NetworkFilterOptions::Evm(inner) => inner.encode_to_vec(),
        };

        let mut request = StreamDataRequest {
            filter: vec![serialized_filter],
            finality: configuration.finality.map(|f| f as i32),
            starting_cursor,
        };

        if let Some(existing_filter) = state.filter.clone() {
            request.filter.push(existing_filter.encode_to_vec());
        }

        let mut client = self.stream_client_factory.new_stream_client().await?;
        let data_stream = client
            .stream_data(request)
            .await
            .change_context(SinkError::Temporary)
            .attach_printable("failed to start stream")?;

        Ok(data_stream)
    }

    async fn handle_message(
        &mut self,
        message: StreamMessage,
        state: &mut PersistedState<F>,
        ct: CancellationToken,
    ) -> Result<(CursorAction, StreamAction), SinkError> {
        match message {
            StreamMessage::Data(data) => {
                let finality = data.finality();
                let cursor = data.cursor;
                let end_cursor = data
                    .end_cursor
                    .ok_or(SinkError::Fatal)
                    .attach_printable("received data without end cursor")?;

                info!(
                    block = end_cursor.order_key,
                    status = %finality,
                    "handle block batch"
                );

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

                let mut batch = data.data.into_iter();

                if let Some(factory_data) = batch.next() {
                    let factory_data = B::decode(factory_data.as_slice())
                        .fatal("failed to decode block data (factory)")?;

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
                    let data = B::decode(data.as_slice()).fatal("failed to decode block data")?;
                    self.handle_data(context, data, state, ct).await
                } else {
                    Ok((CursorAction::Skip, StreamAction::Continue))
                }
            }
            StreamMessage::Invalidate(invalidate) => {
                let cursor = invalidate.cursor;
                info!(block = %DisplayCursor(&cursor), "handle invalidate");
                self.handle_invalidate(cursor, state, ct).await
            }
            StreamMessage::Heartbeat(_inner) => {
                self.sink.handle_heartbeat().await?;
                self.state_manager.heartbeat().await?;
                Ok((CursorAction::Skip, StreamAction::Continue))
            }
            StreamMessage::SystemMessage(system_message) => {
                log_system_message(system_message);
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
        let data = self
            .script
            .transform(json_data)
            .await
            .map_err(|err| err.fatal("failed to transform batch data"))?;

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
