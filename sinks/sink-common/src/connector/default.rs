use std::marker::PhantomData;

use apibara_dna_protocol::{
    client::StreamMessage,
    dna::{common::Cursor, stream::StreamDataRequest},
};
use apibara_script::Script;
use error_stack::{Result, ResultExt};
use prost::Message;
use serde::Serialize;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::{
    connector::system_message::log_system_message, error::SinkError, filter::Filter, sink::Sink,
    Context, CursorAction, DisplayCursor, PersistedState, SinkErrorReportExt, SinkErrorResultExt,
    StreamConfigurationOptions,
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
    starting_configuration: StreamConfigurationOptions,
    needs_invalidation: bool,
    _filter: PhantomData<F>,
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
            _filter: Default::default(),
            _data: Default::default(),
        }
    }

    pub async fn start(&mut self, ct: CancellationToken) -> Result<(), SinkError> {
        self.state_manager.lock(ct.clone()).await?;

        let mut state = self.state_manager.get_state::<F>().await?;

        let starting_cursor = if let Some(cursor) = state.cursor.clone() {
            info!(cursor = %cursor, "restarting from last cursor");
            self.handle_invalidate(Some(cursor.clone()), &mut state, ct.clone())
                .await?;
            Some(cursor)
        } else {
            self.starting_configuration.starting_cursor()
        };

        debug!("start consume stream");

        let request = StreamDataRequest {
            starting_cursor,
            finality: self.starting_configuration.finality.map(|f| f as i32),
            filter: vec![self.starting_configuration.filter.to_bytes()],
        };

        let mut data_stream = self
            .stream_client_factory
            .new_stream_client()
            .await?
            .stream_data(request)
            .await
            .change_context(SinkError::Temporary)
            .attach_printable("failed to start stream")?;

        self.needs_invalidation = false;

        let mut ret = Ok(());
        loop {
            tokio::select! {
                _ = ct.cancelled() => {
                    info!("sink stopped: cancelled");
                    break;
                }
                stream_message = data_stream.try_next() => {
                    match stream_message {
                        Err(err) => {
                            ret = Err(err).change_context(SinkError::Temporary).attach_printable("data stream timeout");
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

    #[tracing::instrument(skip_all, err(Debug))]
    async fn handle_message(
        &mut self,
        message: StreamMessage,
        state: &mut PersistedState<F>,
        ct: CancellationToken,
    ) -> Result<(CursorAction, StreamAction), SinkError> {
        use apibara_dna_protocol::dna::stream::stream_data_response::Message;

        match message {
            Message::Data(data) => {
                let finality = data.finality();
                let end_cursor = data.end_cursor.unwrap_or_default();

                info!(
                    block = end_cursor.order_key,
                    status = %finality,
                    "handle block batch"
                );
                let context = Context {
                    cursor: data.cursor,
                    end_cursor,
                    finality,
                };
                self.handle_data(context, data.data, state, ct).await
            }
            Message::Invalidate(invalidate) => {
                let cursor = invalidate.cursor;
                info!(block = %DisplayCursor(&cursor), "handle invalidate");
                self.handle_invalidate(cursor, state, ct).await
            }
            Message::Heartbeat(_) => {
                self.sink.handle_heartbeat().await?;
                self.state_manager.heartbeat().await?;
                Ok((CursorAction::Skip, StreamAction::Continue))
            }
            Message::SystemMessage(system_message) => {
                log_system_message(system_message);
                Ok((CursorAction::Skip, StreamAction::Continue))
            }
        }
    }

    #[tracing::instrument(skip_all, err(Debug))]
    async fn handle_data(
        &mut self,
        context: Context,
        data: Vec<Vec<u8>>,
        state: &mut PersistedState<F>,
        ct: CancellationToken,
    ) -> Result<(CursorAction, StreamAction), SinkError> {
        // fatal error since if the sink is restarted it will receive the same data again.
        let block_end_cursor = context.end_cursor.order_key;

        if let Some(ending_block) = self.ending_block {
            if block_end_cursor > ending_block {
                info!(
                    block = block_end_cursor,
                    ending_block = ending_block,
                    "ending block reached"
                );
                return Ok((CursorAction::Persist, StreamAction::Stop));
            }
        }

        // In default mode we expect exactly one item per block.
        let Some(block_data) = data.iter().next() else {
            return Ok((CursorAction::Persist, StreamAction::Continue));
        };

        if block_data.is_empty() {
            return Ok((CursorAction::Persist, StreamAction::Continue));
        }

        // fatal error since if the sink is restarted it will receive the same data again.                  │    │
        let block = B::decode(block_data.as_slice()).fatal("failed to parse binary block data")?;
        let json_block = serde_json::to_value(block).fatal("failed to serialize block data")?;

        let output_data = self
            .script
            .transform(json_block)
            .await
            .map_err(|err| err.fatal("failed to transform block data"))?;

        let mut cursor_action = if self.needs_invalidation {
            self.needs_invalidation = false;
            self.sink.handle_replace(&context, &output_data, ct).await?
        } else {
            self.sink.handle_data(&context, &output_data, ct).await?
        };

        // If it's pending, don't store the cursor.
        if context.finality.is_pending() {
            self.needs_invalidation = true;
            cursor_action = CursorAction::Skip;
        }

        state.cursor = Some(context.end_cursor.clone());
        Ok((cursor_action, StreamAction::Continue))
    }

    #[tracing::instrument(skip_all, err(Debug))]
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
