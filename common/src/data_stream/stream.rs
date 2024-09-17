use std::time::Duration;

use apibara_dna_protocol::dna::stream::{
    stream_data_response::Message, Data, DataFinality, Finalize, Heartbeat, Invalidate,
    StreamDataResponse,
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
    finalized: Cursor,
    _finality: DataFinality,
    chain_view: ChainView,
    heartbeat_interval: tokio::time::Interval,
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
        finalized: Cursor,
        finality: DataFinality,
        heartbeat_interval: Duration,
        chain_view: ChainView,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        let heartbeat_interval = tokio::time::interval(heartbeat_interval);
        Self {
            scanner,
            current: starting,
            finalized,
            _finality: finality,
            heartbeat_interval,
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
            .get_grouped_cursor()
            .await
            .change_context(DataStreamError)?
        {
            self.heartbeat_interval.reset();
            todo!();
        }

        if let Some(_cursor) = self
            .chain_view
            .get_segmented_cursor()
            .await
            .change_context(DataStreamError)?
        {
            self.heartbeat_interval.reset();
            todo!();
        }

        let head = self
            .chain_view
            .get_head()
            .await
            .change_context(DataStreamError)?;

        let at_head = self.current.as_ref().map(|c| c == &head).unwrap_or(false);

        if !at_head {
            self.heartbeat_interval.reset();
            return self.tick_single(tx, ct).await;
        }

        debug!("head reached");

        tokio::select! {
            _ = ct.cancelled() => Ok(()),
            _ = self.heartbeat_interval.tick() => {
                debug!("heartbeat");
                self.send_heartbeat_message(tx, ct).await
            }
            _ = self.chain_view.head_changed() => {
                debug!("head changed");
                Ok(())
            },
            _ = self.chain_view.finalized_changed() => {
                debug!("finalized changed");
                self.send_finalize_message(tx, ct).await
            },
        }
    }

    async fn send_heartbeat_message(
        &mut self,
        tx: &mpsc::Sender<DataStreamMessage>,
        ct: &CancellationToken,
    ) -> Result<(), DataStreamError> {
        debug!("tick: send heartbeat message");
        let Some(Ok(permit)) = ct.run_until_cancelled(tx.reserve()).await else {
            return Ok(());
        };

        let heartbeat = Message::Heartbeat(Heartbeat {});

        permit.send(Ok(StreamDataResponse {
            message: Some(heartbeat),
        }));

        Ok(())
    }

    async fn send_finalize_message(
        &mut self,
        tx: &mpsc::Sender<DataStreamMessage>,
        ct: &CancellationToken,
    ) -> Result<(), DataStreamError> {
        debug!("tick: send finalize message");
        let Some(Ok(permit)) = ct.run_until_cancelled(tx.reserve()).await else {
            return Ok(());
        };

        let finalize = Message::Finalize(Finalize {
            cursor: Some(self.finalized.clone().into()),
        });

        permit.send(Ok(StreamDataResponse {
            message: Some(finalize),
        }));

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

                // TODO: collect removed blocks.
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
