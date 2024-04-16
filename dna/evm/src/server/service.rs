use std::collections::{BTreeMap, HashSet};

use apibara_dna_common::error::DnaError;
use apibara_dna_common::segment::SegmentOptions;
use apibara_dna_common::server::{SnapshotState, SnapshotSyncClient};
use apibara_dna_common::storage::StorageBackend;
use apibara_dna_common::{error::Result, segment::SnapshotReader};
use apibara_dna_protocol::dna::{stream_data_response, Cursor, Data, DataFinality};
use apibara_dna_protocol::{
    dna::{
        dna_stream_server, StatusRequest, StatusResponse, StreamDataRequest, StreamDataResponse,
    },
    evm,
};
use error_stack::{Report, ResultExt};
use futures_util::TryFutureExt;
use prost::Message;
use roaring::RoaringBitmap;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tracing::{debug, error, info, warn};

use crate::segment::{
    store, BlockHeaderSegmentReader, LogSegmentReader, ReceiptSegmentReader, SegmentGroupExt,
    SegmentGroupReader, TransactionSegmentReader,
};

use super::filter::SegmentFilter;

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

    #[tracing::instrument(skip_all, err(Debug))]
    async fn subscribe_to_snapshot(
        &self,
    ) -> Result<(SnapshotState, broadcast::Receiver<SnapshotState>)> {
        // TODO: add retry logic.
        self.snapshot_client
            .subscribe()
            .await
            .ok_or::<Report<DnaError>>(DnaError::Fatal.into())
            .attach_printable("failed to subscribe to snapshot")
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
        let (starting_snapshot, snapshot_rx) =
            self.subscribe_to_snapshot().await.map_err(|err| {
                warn!(err = ?err, "failed to subscribe to snapshot");
                tonic::Status::internal("internal server error")
            })?;

        let producer = StreamProducer::init(self.storage.clone(), filters, snapshot_rx, tx.clone())
            .await
            .map_err(|err| {
                warn!(err = ?err, "failed to initialize stream producer");
                tonic::Status::internal("internal server error")
            })?;

        tokio::spawn(
            producer
                .start(starting_block_number, starting_snapshot)
                .inspect_err(|err| {
                    error!(err = ?err, "stream_data error");
                }),
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

struct StreamProducer<S>
where
    S: StorageBackend + Send + Sync + 'static + Clone,
    <S as StorageBackend>::Reader: Unpin + Send + 'static,
{
    storage: S,
    filters: Vec<SegmentFilter>,
    segment_options: SegmentOptions,
    snapshot_rx: broadcast::Receiver<SnapshotState>,
    response_tx: mpsc::Sender<TonicResult<StreamDataResponse>>,
}

impl<S> StreamProducer<S>
where
    S: StorageBackend + Send + Sync + 'static + Clone,
    <S as StorageBackend>::Reader: Unpin + Send + 'static,
{
    pub async fn init(
        storage: S,
        filters: Vec<evm::Filter>,
        snapshot_rx: broadcast::Receiver<SnapshotState>,
        response_tx: mpsc::Sender<TonicResult<StreamDataResponse>>,
    ) -> Result<Self> {
        let segment_options = {
            let mut snapshot_reader = SnapshotReader::new(storage.clone());
            let snapshot = snapshot_reader.snapshot_state().await?;
            snapshot.segment_options
        };

        let filters = filters.into_iter().map(SegmentFilter::new).collect();

        let producer = StreamProducer {
            storage,
            filters,
            snapshot_rx,
            response_tx,
            segment_options,
        };

        Ok(producer)
    }

    #[tracing::instrument(name = "stream_data", skip_all, err(Debug))]
    pub async fn start(
        self,
        starting_block_number: u64,
        starting_snapshot: SnapshotState,
    ) -> Result<()> {
        info!(num_filters = %self.filters.len(), "starting data stream");

        // TODO: this needs to be configurable.
        let buffer_size = 1024 * 1024 * 1024;

        let mut segment_group_reader = SegmentGroupReader::new(
            self.storage.clone(),
            self.segment_options.clone(),
            buffer_size,
        );

        let mut current_block_number = starting_block_number;
        let mut block_bitmap = RoaringBitmap::new();

        let mut header_segment_reader = BlockHeaderSegmentReader::new(
            self.storage.clone(),
            self.segment_options.clone(),
            buffer_size,
        );

        let mut log_segment_reader = LogSegmentReader::new(
            self.storage.clone(),
            self.segment_options.clone(),
            buffer_size,
        );

        let mut transaction_segment_reader = TransactionSegmentReader::new(
            self.storage.clone(),
            self.segment_options.clone(),
            buffer_size,
        );

        let mut receipt_segment_reader = ReceiptSegmentReader::new(
            self.storage.clone(),
            self.segment_options.clone(),
            buffer_size,
        );

        while current_block_number < starting_snapshot.sealed_block_number {
            let segment_group_end = {
                let current_segment_group_start = self
                    .segment_options
                    .segment_group_start(current_block_number);

                debug!(current_segment_group_start, "reading new segment group");

                let segment_group = segment_group_reader
                    .read(current_segment_group_start)
                    .await
                    .change_context(DnaError::Fatal)
                    .attach_printable("failed to read segment group")?;

                assert_eq!(
                    segment_group.first_block_number(),
                    current_segment_group_start
                );

                self.fill_segment_group_bitmap(&segment_group, &mut block_bitmap)
                    .await?
            };

            // Skip as many segments in the group as possible.
            if let Some(starting_block) = block_bitmap.min() {
                current_block_number = starting_block as u64;
            } else {
                debug!(segment_group_end, "no blocks to read. skip ahead");
                current_block_number = segment_group_end;
                continue;
            }

            let mut current_segment_start =
                self.segment_options.segment_start(current_block_number);
            debug!(current_segment_start, "reading starting segment");

            let mut header_segment = header_segment_reader
                .read(current_segment_start)
                .await
                .change_context(DnaError::Fatal)
                .attach_printable("failed to read header segment")?;

            let mut log_segment = log_segment_reader
                .read(current_segment_start)
                .await
                .change_context(DnaError::Fatal)
                .attach_printable("failed to read log segment")?;

            let mut transaction_segment = transaction_segment_reader
                .read(current_segment_start)
                .await
                .change_context(DnaError::Fatal)
                .attach_printable("failed to read transaction segment")?;

            let mut receipt_segment = receipt_segment_reader
                .read(current_segment_start)
                .await
                .change_context(DnaError::Fatal)
                .attach_printable("failed to read receipt segment")?;

            for block_number in block_bitmap.iter() {
                if current_segment_start < self.segment_options.segment_start(block_number as u64) {
                    current_segment_start = self.segment_options.segment_start(block_number as u64);
                    debug!(current_segment_start, "reading new segment");
                    header_segment = header_segment_reader
                        .read(current_segment_start)
                        .await
                        .change_context(DnaError::Fatal)
                        .attach_printable("failed to read header segment")?;

                    log_segment = log_segment_reader
                        .read(current_segment_start)
                        .await
                        .change_context(DnaError::Fatal)
                        .attach_printable("failed to read log segment")?;

                    transaction_segment = transaction_segment_reader
                        .read(current_segment_start)
                        .await
                        .change_context(DnaError::Fatal)
                        .attach_printable("failed to read transaction segment")?;

                    receipt_segment = receipt_segment_reader
                        .read(current_segment_start)
                        .await
                        .change_context(DnaError::Fatal)
                        .attach_printable("failed to read receipt segment")?;
                }

                debug!(block_number, "inspect block");

                // Read the header to extract the block number and hash (needed by the cursor).
                let header_index = block_number - header_segment.first_block_number() as u32;
                let header = header_segment
                    .headers()
                    .unwrap_or_default()
                    .get(header_index as usize);

                let block_transactions = transaction_segment
                    .blocks()
                    .unwrap_or_default()
                    .get(header_index as usize);

                assert_eq!(block_transactions.block_number(), block_number as u64);

                let block_logs = log_segment
                    .blocks()
                    .unwrap_or_default()
                    .get(header_index as usize);

                assert_eq!(block_logs.block_number(), block_number as u64);

                let block_receipts = receipt_segment
                    .blocks()
                    .unwrap_or_default()
                    .get(header_index as usize);

                assert_eq!(block_receipts.block_number(), block_number as u64);

                let mut blocks = vec![];
                for filter in &self.filters {
                    // Withdrawals
                    let mut withdrawals = Vec::new();
                    if filter.has_withdrawals() {
                        'withdrawal: for (withdrawal_index, w) in
                            header.withdrawals().unwrap_or_default().iter().enumerate()
                        {
                            let withdrawal_address =
                                w.address().expect("withdrawal must have address").into();
                            let withdrawal_validator = w.validator_index();

                            for withdrawal_filter in filter.withdrawals() {
                                let should_include_by_address = withdrawal_filter
                                    .address
                                    .as_ref()
                                    .map(|addr| addr == &withdrawal_address);

                                let should_include_by_validator = withdrawal_filter
                                    .validator_index
                                    .as_ref()
                                    .map(|validator| *validator == withdrawal_validator);

                                let should_include = match (
                                    should_include_by_address,
                                    should_include_by_validator,
                                ) {
                                    (None, None) => true,
                                    (Some(a), Some(b)) => a && b,
                                    (Some(a), None) => a,
                                    (None, Some(b)) => b,
                                };

                                if should_include {
                                    let mut w: evm::Withdrawal = w.into();
                                    w.withdrawal_index = withdrawal_index as u64;
                                    withdrawals.push(w);
                                    continue 'withdrawal;
                                }
                            }
                        }
                    }

                    // Since transactions, logs, and receipts can reference each other, we need to
                    // keep track of the indices of data we need to include in the response.
                    let mut required_transactions: HashSet<u64> = HashSet::new();
                    let mut required_logs: HashSet<u64> = HashSet::new();
                    let mut required_receipts: HashSet<u64> = HashSet::new();

                    let mut transactions = BTreeMap::new();

                    if filter.has_transactions() {
                        for (transaction_index, t) in block_transactions
                            .transactions()
                            .unwrap_or_default()
                            .iter()
                            .enumerate()
                        {
                            let transaction_index = transaction_index as u64;
                            let from = t.from().expect("transaction must have from").into();
                            let to = t.to().map(Into::into);

                            for transaction_filter in filter.transactions() {
                                if transactions.contains_key(&transaction_index) {
                                    if transaction_filter.include_logs.unwrap_or(false) {
                                        required_logs.insert(transaction_index);
                                    }
                                    if transaction_filter.include_receipt.unwrap_or(false) {
                                        required_receipts.insert(transaction_index);
                                    }

                                    continue;
                                }

                                let should_include_by_from =
                                    transaction_filter.from.as_ref().map(|f| f == &from);

                                let should_include_by_to = match (&transaction_filter.to, &to) {
                                    (None, _) => None,
                                    (Some(a), Some(b)) => Some(a == b),
                                    _ => Some(false),
                                };

                                let should_include =
                                    match (should_include_by_from, should_include_by_to) {
                                        (None, None) => true,
                                        (Some(a), Some(b)) => a && b,
                                        (Some(a), None) => a,
                                        (None, Some(b)) => b,
                                    };

                                if should_include {
                                    let t: evm::Transaction = t.into();
                                    assert_eq!(t.transaction_index, transaction_index);
                                    transactions.insert(transaction_index, t);

                                    if transaction_filter.include_logs.unwrap_or(false) {
                                        required_logs.insert(transaction_index);
                                    }
                                    if transaction_filter.include_receipt.unwrap_or(false) {
                                        required_receipts.insert(transaction_index);
                                    }

                                    continue;
                                }
                            }
                        }
                    }

                    let mut logs = BTreeMap::<u64, _>::new();
                    if filter.has_logs() {
                        for log in block_logs.logs().unwrap_or_default() {
                            let address = log.address().expect("log must have address").into();
                            let topics: Vec<evm::B256> = log
                                .topics()
                                .unwrap_or_default()
                                .iter()
                                .map(Into::into)
                                .collect();

                            for log_filter in filter.logs() {
                                if logs.contains_key(&log.log_index()) {
                                    if log_filter.include_transaction.unwrap_or(false) {
                                        required_transactions.insert(log.transaction_index());
                                    }

                                    if log_filter.include_receipt.unwrap_or(false) {
                                        required_receipts.insert(log.transaction_index());
                                    }
                                    continue;
                                }

                                let should_include_by_address =
                                    log_filter.address.as_ref().map(|addr| addr == &address);

                                let should_include_by_topics = {
                                    if log_filter.topics.is_empty() {
                                        None
                                    } else if log_filter.topics.len() > topics.len() {
                                        Some(false)
                                    } else {
                                        Some(log_filter.topics.iter().zip(topics.iter()).all(
                                            |(f, t)| {
                                                f.value.as_ref().map(|fv| fv == t).unwrap_or(true)
                                            },
                                        ))
                                    }
                                };

                                let should_include =
                                    match (should_include_by_address, should_include_by_topics) {
                                        (None, None) => true,
                                        (Some(a), Some(b)) => a && b,
                                        (Some(a), None) => a,
                                        (None, Some(b)) => b,
                                    };

                                if should_include {
                                    let l: evm::Log = log.into();
                                    logs.insert(log.log_index(), l);

                                    if log_filter.include_transaction.unwrap_or(false) {
                                        required_transactions.insert(log.transaction_index());
                                    }

                                    if log_filter.include_receipt.unwrap_or(false) {
                                        required_receipts.insert(log.transaction_index());
                                    }

                                    continue;
                                }
                            }
                        }
                    }

                    // Fill additional data required by the filters.
                    for transaction_index in required_transactions {
                        for transaction in block_transactions.transactions().unwrap_or_default() {
                            let should_include = transaction.transaction_index()
                                == transaction_index
                                && !transactions.contains_key(&transaction_index);
                            if should_include {
                                let t: evm::Transaction = transaction.into();
                                transactions.insert(transaction_index, t);
                            }
                        }
                    }

                    for transaction_index in required_logs {
                        for log in block_logs.logs().unwrap_or_default() {
                            let should_include = log.transaction_index() == transaction_index
                                && !logs.contains_key(&log.log_index());
                            if should_include {
                                let l: evm::Log = log.into();
                                logs.insert(log.log_index(), l);
                            }
                        }
                    }

                    let mut receipts = BTreeMap::<u64, _>::new();
                    for transaction_index in required_receipts {
                        for receipt in block_receipts.receipts().unwrap_or_default() {
                            if receipt.transaction_index() == transaction_index {
                                let r: evm::TransactionReceipt = receipt.into();
                                receipts.insert(transaction_index, r);
                            }
                        }
                    }

                    let block = {
                        let header = header.into();
                        evm::Block {
                            header: Some(header),
                            withdrawals,
                            transactions: transactions.into_values().collect(),
                            logs: logs.into_values().collect(),
                            receipts: receipts.into_values().collect(),
                        }
                    };

                    blocks.push(block);
                }

                let data = Data {
                    data: blocks.into_iter().map(|b| b.encode_to_vec()).collect(),
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

                let Ok(_) = self
                    .response_tx
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

    async fn fill_segment_group_bitmap<'a>(
        &self,
        segment_group: &store::SegmentGroup<'a>,
        block_bitmap: &mut RoaringBitmap,
    ) -> Result<u64> {
        block_bitmap.clear();

        let group_start = segment_group.first_block_number();
        let segment_group_blocks = self.segment_options.segment_group_blocks();
        let group_end = group_start + segment_group_blocks;

        // Use the indices stored in the segment group to build the block bitmap.
        // TODO: what if an index is missing? We should fill the bitmap densely.
        'filter: for filter in &self.filters {
            if filter.has_required_header() || filter.has_withdrawals() || filter.has_transactions()
            {
                block_bitmap.insert_range(group_start as u32..group_end as u32);
                break 'filter;
            }

            for log in filter.logs() {
                let any_address = log.address.is_none();
                let any_topic =
                    log.topics.is_empty() || log.topics.iter().any(|t| t.value.is_none());

                if any_address && any_topic {
                    block_bitmap.insert_range(group_start as u32..group_end as u32);
                    break 'filter;
                }

                if let Some(address) = &log.address {
                    let address_bitmap = segment_group
                        .get_log_by_address(&address.into())
                        .unwrap_or_default();
                    *block_bitmap |= address_bitmap;
                }

                for topic in &log.topics {
                    if let Some(topic) = &topic.value {
                        let topic_bitmap = segment_group
                            .get_log_by_topic(&topic.into())
                            .unwrap_or_default();
                        *block_bitmap |= topic_bitmap;
                    }
                }
            }
        }

        Ok(group_end)
    }
}
