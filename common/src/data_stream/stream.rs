use std::time::Duration;

use apibara_dna_protocol::dna::stream::{
    stream_data_response::Message, Data, DataFinality, Finalize, Heartbeat, Invalidate,
    StreamDataResponse,
};
use error_stack::{Result, ResultExt};
use futures::FutureExt;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::{
    chain_view::{CanonicalCursor, ChainView, NextCursor},
    data_stream::{ScannerAction, SegmentBlock},
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
        let next_cursor = match self
            .chain_view
            .get_next_cursor(&self.current)
            .await
            .change_context(DataStreamError)?
        {
            NextCursor::Continue(cursor) => cursor,
            NextCursor::Invalidate(cursor) => {
                debug!(cursor = %cursor, "invalidating data");

                // TODO: collect removed blocks.
                let invalidate = Message::Invalidate(Invalidate {
                    ..Default::default()
                });

                let Some(Ok(permit)) = ct.run_until_cancelled(tx.reserve()).await else {
                    return Ok(());
                };

                permit.send(Ok(StreamDataResponse {
                    message: Some(invalidate),
                }));

                self.heartbeat_interval.reset();
                self.current = Some(cursor);

                return Ok(());
            }
            NextCursor::AtHead => {
                debug!("head reached. waiting for new head");
                tokio::select! {
                    _ = ct.cancelled() => return Ok(()),
                    _ = self.heartbeat_interval.tick() => {
                        debug!("heartbeat");
                        return self.send_heartbeat_message(tx, ct).await;
                    }
                    _ = self.chain_view.head_changed() => {
                        debug!("head changed");
                        return Ok(());
                    },
                    _ = self.chain_view.finalized_changed() => {
                        debug!("finalized changed");
                        return self.send_finalize_message(tx, ct).await;
                    },
                }
            }
        };

        if self
            .chain_view
            .has_segment_for_block(next_cursor.number)
            .await
        {
            return self.tick_segment(next_cursor, tx, ct).await;
        }

        self.tick_single(next_cursor, tx, ct).await
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

    async fn tick_segment(
        &mut self,
        cursor: Cursor,
        tx: &mpsc::Sender<DataStreamMessage>,
        ct: &CancellationToken,
    ) -> Result<(), DataStreamError> {
        let mut current = cursor.clone();

        let segment_size = self.chain_view.get_segment_size().await;
        let segment_start = self
            .chain_view
            .get_segment_start_block(current.number)
            .await;
        let segment_end = self.chain_view.get_segment_end_block(current.number).await;

        let starting_block_number = cursor.number;
        // Notice that we could be starting from anywhere in the segment.
        let base_offset = current.number - segment_start;

        let mut blocks = vec![SegmentBlock {
            cursor: self.current.clone(),
            end_cursor: current.clone(),
            offset: base_offset as usize,
        }];

        for i in 1..segment_size {
            if current.number >= segment_end {
                break;
            }

            let CanonicalCursor::Canonical(next_cursor) = self
                .chain_view
                .get_canonical(starting_block_number + i)
                .await
                .change_context(DataStreamError)?
            else {
                return Err(DataStreamError).attach_printable("missing canonical block");
            };

            blocks.push(SegmentBlock {
                cursor: current.clone().into(),
                end_cursor: next_cursor.clone(),
                offset: (base_offset + i) as usize,
            });

            current = next_cursor;
        }

        let segment_cursor = Cursor::new_finalized(segment_start);

        self.scanner
            .scan_segment(segment_cursor, blocks, |data| async move {
                use apibara_dna_protocol::dna::stream::Cursor as ProtoCursor;

                let Some(Ok(permit)) = ct.run_until_cancelled(tx.reserve()).await else {
                    return ScannerAction::Stop;
                };

                let proto_cursor: Option<ProtoCursor> = data.cursor.map(Into::into);
                let proto_end_cursor: Option<ProtoCursor> = Some(data.end_cursor.into());

                let data = Message::Data(Data {
                    cursor: proto_cursor.clone(),
                    end_cursor: proto_end_cursor.clone(),
                    data: data.data,
                    finality: DataFinality::Finalized as i32,
                });

                permit.send(Ok(StreamDataResponse {
                    message: Some(data),
                }));

                ScannerAction::Continue
            })
            .boxed()
            .await
            .change_context(DataStreamError)?;

        self.heartbeat_interval.reset();
        self.current = current.into();

        Ok(())
    }

    async fn tick_single(
        &mut self,
        cursor: Cursor,
        tx: &mpsc::Sender<DataStreamMessage>,
        ct: &CancellationToken,
    ) -> Result<(), DataStreamError> {
        use apibara_dna_protocol::dna::stream::Cursor as ProtoCursor;

        debug!("tick: single block");
        let Some(Ok(permit)) = ct.run_until_cancelled(tx.reserve()).await else {
            return Ok(());
        };

        debug!(cursor = ?self.current, end_cursor = %cursor, "sending data");

        let proto_cursor: Option<ProtoCursor> = self.current.clone().map(Into::into);
        let proto_end_cursor: Option<ProtoCursor> = Some(cursor.clone().into());

        let finalized = self
            .chain_view
            .get_finalized_cursor()
            .await
            .change_context(DataStreamError)?;

        let finality = if finalized.strict_after(&cursor) {
            DataFinality::Finalized
        } else {
            DataFinality::Accepted
        };

        self.scanner
            .scan_single(&cursor, |blocks| {
                let data = Message::Data(Data {
                    cursor: proto_cursor.clone(),
                    end_cursor: proto_end_cursor.clone(),
                    data: blocks,
                    finality: finality.into(),
                });

                permit.send(Ok(StreamDataResponse {
                    message: Some(data),
                }));
            })
            .await
            .change_context(DataStreamError)
            .attach_printable("failed to scan single block")?;

        self.heartbeat_interval.reset();
        self.current = Some(cursor);

        Ok(())
    }
}

impl error_stack::Context for DataStreamError {}

impl std::fmt::Display for DataStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "data stream error")
    }
}
