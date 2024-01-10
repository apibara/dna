use std::marker::PhantomData;

use apibara_core::{filter::Filter, node::v1alpha2::Cursor};
use apibara_script::Script;
use apibara_sdk::{Configuration, DataMessage};
use error_stack::{Result, ResultExt};
use prost::Message;
use serde::Serialize;
use serde_json::Value;
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

pub struct DefaultConnector<S, F, B>
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
    _data: PhantomData<B>,
}

impl<S, F, B> DefaultConnector<S, F, B>
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
            _data: Default::default(),
        }
    }

    pub async fn start(&mut self, ct: CancellationToken) -> Result<(), SinkError> {
        self.state_manager.lock(ct.clone()).await?;

        let mut state = self.state_manager.get_state::<F>().await?;

        let starting_cursor = state.cursor.clone();

        let mut configuration = self.starting_configuration.clone();
        if starting_cursor.is_some() {
            info!(cursor = %DisplayCursor(&starting_cursor), "restarting from last cursor");
            configuration.starting_cursor = starting_cursor.clone();
            self.handle_invalidate(starting_cursor, &mut state, ct.clone())
                .await?;
        }

        debug!("start consume stream");

        let mut data_stream = self
            .stream_client_factory
            .new_stream_client()
            .await?
            .start_stream_immutable::<F, B>(configuration)
            .await
            .change_context(SinkError::Temporary)
            .attach_printable("failed to start stream")?;

        self.needs_invalidation = false;

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
                            self.state_manager.put_state(state.clone(), cursor_action).await?;
                            if stream_action == StreamAction::Stop {
                                break;
                            }
                        }
                    }
                }
            }
        }

        self.sink
            .cleanup()
            .await
            .map_err(|err| err.temporary("failed to cleanup sink"))?;

        self.state_manager.cleanup().await?;

        ret
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
                let context = Context {
                    cursor,
                    end_cursor,
                    finality,
                };
                self.handle_data(context, batch, state, ct).await
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

    async fn handle_data(
        &mut self,
        context: Context,
        batch: Vec<B>,
        state: &mut PersistedState<F>,
        ct: CancellationToken,
    ) -> Result<(CursorAction, StreamAction), SinkError> {
        if self.needs_invalidation {
            self.handle_invalidate(context.cursor.clone(), state, ct.clone())
                .await?;
            self.needs_invalidation = false;
        }

        // fatal error since if the sink is restarted it will receive the same data again.
        let json_batch = batch
            .into_iter()
            .map(|b| serde_json::to_value(b).fatal("failed to serialize batch data"))
            .collect::<Result<Vec<Value>, _>>()?;
        let data = self
            .script
            .transform(json_batch)
            .await
            .map_err(|err| err.fatal("failed to transform batch data"))?;

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

        info!(block = block_end_cursor, "handle data");

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
