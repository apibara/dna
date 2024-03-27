use apibara_dna_common::error::DnaError;
use apibara_dna_common::server::SnapshotSyncClient;
use apibara_dna_common::storage::StorageBackend;
use apibara_dna_common::{error::Result, segment::SnapshotReader};
use apibara_dna_protocol::dna::{stream_data_response, Cursor, Data, DataFinality};
use apibara_dna_protocol::{
    dna::{
        dna_stream_server, StatusRequest, StatusResponse, StreamDataRequest, StreamDataResponse,
    },
    evm,
};
use error_stack::ResultExt;
use futures_util::{Future, TryFutureExt};
use prost::Message;
use roaring::RoaringBitmap;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tracing::{debug, error, info, warn};

use crate::segment::{
    BlockHeaderSegmentReader, LogSegmentReader, SegmentGroupReader, TransactionSegmentReader,
};

type TonicResult<T> = std::result::Result<T, tonic::Status>;

pub struct Service<S>
where
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    storage: S,
    snapshot_client: SnapshotSyncClient,
}

impl<S> Service<S>
where
    S: StorageBackend + Send + Sync + 'static + Clone,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    pub fn new(storage: S, snapshot_client: SnapshotSyncClient) -> Self {
        Self {
            storage,
            snapshot_client,
        }
    }

    pub fn into_service(self) -> dna_stream_server::DnaStreamServer<Service<S>> {
        dna_stream_server::DnaStreamServer::new(self)
    }
}

#[tonic::async_trait]
impl<S> dna_stream_server::DnaStream for Service<S>
where
    S: StorageBackend + Send + Sync + 'static + Clone,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    type StreamDataStream = ReceiverStream<TonicResult<StreamDataResponse>>;

    async fn stream_data(
        &self,
        request: Request<StreamDataRequest>,
    ) -> TonicResult<tonic::Response<Self::StreamDataStream>> {
        let (tx, rx) = mpsc::channel(128);
        let request = request.into_inner();

        let starting_block_number = request
            .starting_cursor
            .map(|c| c.order_key + 1)
            .unwrap_or_default();

        let filters = request
            .filter
            .into_iter()
            .map(|f| <evm::Filter as prost::Message>::decode(f.as_slice()))
            .collect::<std::result::Result<Vec<evm::Filter>, _>>()
            .map_err(|_| tonic::Status::invalid_argument("failed to decode filter"))?;

        // TODO: use this + retry.
        let xxx = self.snapshot_client.subscribe().await.unwrap();

        tokio::spawn(
            do_stream_data(self.storage.clone(), starting_block_number, filters, tx).inspect_err(
                |err| {
                    warn!(?err, "stream data error");
                },
            ),
        );

        Ok(tonic::Response::new(ReceiverStream::new(rx)))
    }

    async fn status(
        &self,
        _request: Request<StatusRequest>,
    ) -> TonicResult<tonic::Response<StatusResponse>> {
        error!("status not implemented");
        Ok(tonic::Response::new(StatusResponse::default()))
    }
}

async fn do_stream_data<S>(
    storage: S,
    starting_block_number: u64,
    filters: Vec<evm::Filter>,
    tx: mpsc::Sender<TonicResult<StreamDataResponse>>,
) -> Result<()>
where
    S: StorageBackend + Send + Sync + 'static + Clone,
    <S as StorageBackend>::Reader: Unpin + Send + 'static,
{
    info!(starting_block_number, num_filters = %filters.len(), "starting stream");
    let mut snapshot_reader = SnapshotReader::new(storage.clone());
    let snapshot = snapshot_reader.snapshot_state().await?;

    let segment_options = snapshot.segment_options;
    let starting_block_number = segment_options.segment_group_start(snapshot.first_block_number);
    let ending_block_number = starting_block_number
        + snapshot.group_count as u64 * segment_options.segment_group_blocks() as u64;

    let mut segment_group_reader =
        SegmentGroupReader::new(storage.clone(), segment_options.clone(), 1024 * 1024 * 1024);
    let mut header_segment_reader =
        BlockHeaderSegmentReader::new(storage.clone(), segment_options.clone(), 1024 * 1024 * 1024);
    let mut log_segment_reader =
        LogSegmentReader::new(storage.clone(), segment_options.clone(), 1024 * 1024 * 1024);
    let mut transaction_segment_reader =
        TransactionSegmentReader::new(storage.clone(), segment_options.clone(), 1024 * 1024 * 1024);

    let mut current_block_number = starting_block_number;
    let mut block_bitmap = RoaringBitmap::new();
    while current_block_number < ending_block_number {
        let current_segment_group_start = segment_options.segment_group_start(current_block_number);
        info!(current_segment_group_start, "reading new segment group");
        let segment_group = segment_group_reader
            .read(current_segment_group_start)
            .await
            .change_context(DnaError::Fatal)
            .attach_printable("failed to read segment group")?;

        assert_eq!(
            segment_group.first_block_number(),
            current_segment_group_start
        );

        let segment_group_blocks = segment_options.segment_group_blocks();
        let segment_group_end = current_segment_group_start + segment_group_blocks;

        block_bitmap.clear();

        // TODO: read block bitmap
        for filter in &filters {
            if let Some(header) = filter.header.as_ref() {
                if !header.weak() {
                    block_bitmap
                        .insert_range(current_segment_group_start as u32..segment_group_end as u32);
                    break;
                }
            }

            todo!();
        }

        // Skip as many segments in the group as possible.
        if let Some(starting_block) = block_bitmap.min() {
            current_block_number = starting_block as u64;
        } else {
            debug!(segment_group_end, "no blocks to read. skip ahead");
            current_block_number = segment_group_end;
            continue;
        }

        let mut current_segment_start = segment_options.segment_start(current_block_number);
        debug!(current_segment_start, "reading starting segment");

        let mut header_segment = header_segment_reader
            .read(current_segment_start)
            .await
            .change_context(DnaError::Fatal)
            .attach_printable("failed to read header segment")?;

        for block_number in block_bitmap.iter() {
            if current_segment_start < segment_options.segment_start(block_number as u64) {
                current_segment_start = segment_options.segment_start(block_number as u64);
                debug!(current_segment_start, "reading new segment");
                header_segment = header_segment_reader
                    .read(current_segment_start)
                    .await
                    .change_context(DnaError::Fatal)
                    .attach_printable("failed to read header segment")?;
            }
            debug!(block_number, "inspect block");

            let header = {
                let index = block_number - header_segment.first_block_number() as u32;
                let header = header_segment
                    .headers()
                    .unwrap_or_default()
                    .get(index as usize);

                evm::BlockHeader {
                    number: header.number(),
                    hash: header.hash().map(|h| evm::B256::from_bytes(&h.0)),
                    nonce: header.nonce(),
                    ..Default::default()
                }
            };

            let block = evm::Block {
                header: Some(header),
                ..Default::default()
            };

            let data = Data {
                data: vec![block.encode_to_vec()],
                finality: DataFinality::Finalized as i32,
                cursor: Some(Cursor {
                    order_key: block_number as u64,
                    unique_key: vec![],
                }),
                end_cursor: Some(Cursor {
                    order_key: block_number as u64,
                    unique_key: vec![],
                }),
            };

            let Ok(_) = tx
                .send(Ok(StreamDataResponse {
                    message: Some(stream_data_response::Message::Data(data)),
                }))
                .await
            else {
                // channel closed. stop streaming.
                return Ok(());
            };
        }

        current_block_number = segment_group_end;
    }

    Ok(())
}
