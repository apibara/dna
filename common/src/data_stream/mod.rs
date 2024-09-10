use apibara_dna_protocol::dna::stream::{
    stream_data_response::Message, Data, DataFinality, Invalidate, StreamDataResponse,
};
use error_stack::{Result, ResultExt};
use tokio::sync::mpsc::{self, Permit};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    chain_view::{ChainView, NextCursor},
    Cursor,
};

#[derive(Debug)]
pub struct DataStreamError;

pub struct DataStream {
    current: Option<Cursor>,
    _finality: DataFinality,
    chain_view: ChainView,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

type DataStreamMessage = tonic::Result<StreamDataResponse, tonic::Status>;

impl DataStream {
    pub fn new(
        starting: Option<Cursor>,
        finality: DataFinality,
        chain_view: ChainView,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        Self {
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
            let Ok(permit) = tx.reserve().await else {
                return Ok(());
            };

            if let Err(err) = self.tick(permit).await {
                warn!(error = ?err, "data stream error");
                tx.send(Err(tonic::Status::internal("internal server error")))
                    .await
                    .change_context(DataStreamError)?;
                return Err(err).change_context(DataStreamError);
            }
        }

        Ok(())
    }

    async fn tick(&mut self, tx: Permit<'_, DataStreamMessage>) -> Result<(), DataStreamError> {
        match self
            .chain_view
            .get_next_cursor(&self.current)
            .await
            .change_context(DataStreamError)?
        {
            NextCursor::Continue(cursor) => {
                debug!(cursor = ?self.current, end_cursor = %cursor, "sending data");

                let data = Message::Data(Data {
                    cursor: self.current.clone().map(Into::into),
                    end_cursor: Some(cursor.clone().into()),
                    ..Default::default()
                });

                tx.send(Ok(StreamDataResponse {
                    message: Some(data),
                }));

                self.current = Some(cursor);
            }
            NextCursor::Invalidate(cursor) => {
                debug!(cursor = %cursor, "invalidating data");

                let invalidate = Message::Invalidate(Invalidate {
                    ..Default::default()
                });

                tx.send(Ok(StreamDataResponse {
                    message: Some(invalidate),
                }));

                self.current = Some(cursor);
            }
            NextCursor::AtHead => {
                // TODO: this should not happen.
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
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
