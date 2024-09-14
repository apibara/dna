use apibara_dna_protocol::dna::stream::{
    stream_data_response::Message, Data, DataFinality, Invalidate, StreamDataResponse,
};
use error_stack::{Result, ResultExt};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    chain_view::{ChainView, NextCursor},
    Cursor,
};

use super::scanner::Scanner;

#[derive(Debug)]
pub struct DataStreamError;

pub struct DataStream<S>
where
    S: Scanner + Send,
{
    scanner: S,
    current: Option<Cursor>,
    _finality: DataFinality,
    chain_view: ChainView,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

type DataStreamMessage = tonic::Result<StreamDataResponse, tonic::Status>;

impl<S> DataStream<S>
where
    S: Scanner + Send,
{
    pub fn new(
        scanner: S,
        starting: Option<Cursor>,
        finality: DataFinality,
        chain_view: ChainView,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        Self {
            scanner,
            current: starting,
            _finality: finality,
            chain_view,
            _permit: permit,
        }
    }

    pub async fn start(
        mut self,
        tx: mpsc::Sender<DataStreamMessage>,
        ct: CancellationToken,
    ) -> Result<(), DataStreamError> {
        while !ct.is_cancelled() && !tx.is_closed() {
            if let Err(err) = self.tick(&tx, &ct).await {
                warn!(error = ?err, "data stream error");
                tx.send(Err(tonic::Status::internal("internal server error")))
                    .await
                    .change_context(DataStreamError)?;
                return Err(err).change_context(DataStreamError);
            }
        }

        Ok(())
    }

    async fn tick(
        &mut self,
        tx: &mpsc::Sender<DataStreamMessage>,
        ct: &CancellationToken,
    ) -> Result<(), DataStreamError> {
        if let Some(_cursor) = self
            .chain_view
            .get_segment_group_cursor()
            .await
            .change_context(DataStreamError)?
        {
            todo!();
        }

        if let Some(_cursor) = self
            .chain_view
            .get_segment_cursor()
            .await
            .change_context(DataStreamError)?
        {
            todo!();
        }

        let head = self
            .chain_view
            .get_head()
            .await
            .change_context(DataStreamError)?;

        let at_head = self.current.as_ref().map(|c| c == &head).unwrap_or(false);

        if !at_head {
            return self.tick_single(tx, ct).await;
        }

        debug!("head reached");
        let Some(_) = ct.run_until_cancelled(self.chain_view.head_changed()).await else {
            return Ok(());
        };
        debug!("head changed");

        Ok(())
    }

    async fn tick_single(
        &mut self,
        tx: &mpsc::Sender<DataStreamMessage>,
        ct: &CancellationToken,
    ) -> Result<(), DataStreamError> {
        debug!("tick: single block");
        let Some(Ok(permit)) = ct.run_until_cancelled(tx.reserve()).await else {
            return Ok(());
        };

        match self
            .chain_view
            .get_next_cursor(&self.current)
            .await
            .change_context(DataStreamError)?
        {
            NextCursor::Continue(cursor) => {
                use apibara_dna_protocol::dna::stream::Cursor as ProtoCursor;

                debug!(cursor = ?self.current, end_cursor = %cursor, "sending data");

                let proto_cursor: Option<ProtoCursor> = self.current.clone().map(Into::into);
                let proto_end_cursor: Option<ProtoCursor> = Some(cursor.clone().into());

                self.scanner
                    .scan_single(&cursor, |blocks| {
                        let data = Message::Data(Data {
                            cursor: proto_cursor.clone(),
                            end_cursor: proto_end_cursor.clone(),
                            data: blocks,
                            ..Default::default()
                        });

                        permit.send(Ok(StreamDataResponse {
                            message: Some(data),
                        }));
                    })
                    .await
                    .change_context(DataStreamError)
                    .attach_printable("failed to scan single block")?;

                self.current = Some(cursor);
            }
            NextCursor::Invalidate(cursor) => {
                debug!(cursor = %cursor, "invalidating data");

                let invalidate = Message::Invalidate(Invalidate {
                    ..Default::default()
                });

                permit.send(Ok(StreamDataResponse {
                    message: Some(invalidate),
                }));

                self.current = Some(cursor);
            }
            NextCursor::AtHead => {}
        }

        Ok(())
    }
}

impl error_stack::Context for DataStreamError {}

impl std::fmt::Display for DataStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "data stream error")
    }
}
