use std::{
    collections::{BTreeMap, HashSet},
    time::Duration,
};

use apibara_dna_common::{
    core::Cursor,
    error::{DnaError, Result},
    ingestion::Snapshot,
    segment::SegmentOptions,
    storage::{LocalStorageBackend, StorageBackend},
};
use apibara_dna_protocol::{
    dna::{
        common::{StatusRequest, StatusResponse},
        stream::{
            self, dna_stream_server, stream_data_response, StreamDataRequest, StreamDataResponse,
        },
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

use crate::{
    segment::{
        store, BlockHeaderSegmentReader, LogSegmentReader, ReceiptSegmentReader, SegmentGroupExt,
        SegmentGroupReader, TransactionSegmentReader,
    },
    server::cursor_producer::NextBlock,
};

use super::cursor_producer::{self, BlockNumberOrCursor, CursorProducer};
use super::filter::SegmentFilter;

type TonicResult<T> = std::result::Result<T, tonic::Status>;

const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024 * 1024;

pub struct DnaService<S>
where
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    storage: S,
    local_storage: LocalStorageBackend,
    cursor_producer: CursorProducer,
}

impl<S> DnaService<S>
where
    S: StorageBackend + Send + Sync + 'static + Clone,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    pub fn new(
        storage: S,
        local_storage: LocalStorageBackend,
        cursor_producer: CursorProducer,
    ) -> Self {
        Self {
            storage,
            local_storage,
            cursor_producer,
        }
    }

    pub fn into_service(self) -> dna_stream_server::DnaStreamServer<DnaService<S>> {
        dna_stream_server::DnaStreamServer::new(self)
    }
}

#[tonic::async_trait]
impl<S> dna_stream_server::DnaStream for DnaService<S>
where
    S: StorageBackend + Send + Sync + 'static + Clone,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    type StreamDataStream = ReceiverStream<TonicResult<StreamDataResponse>>;

    async fn stream_data(
        &self,
        request: Request<StreamDataRequest>,
    ) -> TonicResult<tonic::Response<Self::StreamDataStream>> {
        let cursor_producer = self.cursor_producer.clone();

        let (tx, rx) = mpsc::channel(128);
        let request = request.into_inner();

        let starting_block = match request.starting_cursor {
            Some(cursor) => {
                let cursor: Cursor = cursor.into();
                if cursor.hash.is_empty() {
                    (cursor.number + 1).into()
                } else {
                    cursor.into()
                }
            }
            None => BlockNumberOrCursor::Number(0),
        };

        if !cursor_producer.is_block_available(&starting_block).await {
            let most_recent = cursor_producer.most_recent_available_block().await;
            match most_recent {
                None => {
                    return Err(tonic::Status::failed_precondition(
                        "no block is available yet",
                    ));
                }
                Some(block) => {
                    return Err(tonic::Status::invalid_argument(format!(
                        "starting block not yet available. most recent block: {}",
                        block.number()
                    )));
                }
            }
        }

        let filters = request
            .filter
            .into_iter()
            .map(|f| <evm::Filter as prost::Message>::decode(f.as_slice()))
            .collect::<std::result::Result<Vec<evm::Filter>, _>>()
            .map_err(|_| tonic::Status::invalid_argument("failed to decode filter"))?;

        let producer = StreamProducer::init(
            filters,
            tx,
            self.storage.clone(),
            self.local_storage.clone(),
            cursor_producer,
        );

        tokio::spawn(producer.start(starting_block).inspect_err(|err| {
            error!(err = ?err, "stream_data error");
        }));

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
    local_storage: LocalStorageBackend,
    filters: Vec<SegmentFilter>,
    cursor_producer: CursorProducer,
    response_tx: mpsc::Sender<TonicResult<StreamDataResponse>>,
}

impl<S> StreamProducer<S>
where
    S: StorageBackend + Send + Sync + 'static + Clone,
    <S as StorageBackend>::Reader: Unpin + Send + 'static,
{
    pub fn init(
        filters: Vec<evm::Filter>,
        response_tx: mpsc::Sender<TonicResult<StreamDataResponse>>,
        storage: S,
        local_storage: LocalStorageBackend,
        cursor_producer: CursorProducer,
    ) -> Self {
        let filters = filters.into_iter().map(SegmentFilter::new).collect();

        StreamProducer {
            filters,
            storage,
            local_storage,
            cursor_producer,
            response_tx,
        }
    }

    async fn send_message(&self, message: stream_data_response::Message) {
        let response = StreamDataResponse {
            message: Some(message),
        };
        let _ = self.response_tx.send(Ok(response)).await;
    }

    pub async fn send_system_message(&self, message: impl Into<String>, is_err: bool) {
        use stream::{system_message::Output, SystemMessage};
        let inner = if is_err {
            Output::Stderr(message.into())
        } else {
            Output::Stdout(message.into())
        };
        let message = SystemMessage {
            output: Some(inner),
        };

        self.send_message(stream_data_response::Message::SystemMessage(message))
            .await;
    }

    #[tracing::instrument(name = "stream_data", skip_all, err(Debug))]
    pub async fn start(self, starting_block: BlockNumberOrCursor) -> Result<()> {
        info!(num_filters = %self.filters.len(), "starting data stream");

        // TODO: this needs to be configurable.

        let mut current_block = starting_block;

        loop {
            // NOTICE THAT IT SKIPS ONE BLOCK WHEN GOING FROM SEGMENT TO SINGLE BLOCKS.
            match self.cursor_producer.next_block(&current_block).await? {
                NextBlock::NotReady => {
                    self.send_system_message(
                        "server is starting up. stream will begin shortly",
                        false,
                    )
                    .await;
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                NextBlock::HeadReached => {
                    self.send_system_message("head reached", false).await;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                NextBlock::SegmentGroup(starting_block, segment_options) => {
                    self.send_system_message(format!("segment group {}", starting_block), false)
                        .await;

                    current_block =
                        (starting_block + segment_options.segment_group_blocks()).into();
                    // self.stream_segment_group(starting_block).await?;
                }
                NextBlock::Segment(starting_block, segment_options) => {
                    self.send_system_message(format!("segment {}", starting_block), false)
                        .await;

                    current_block = (starting_block + segment_options.segment_size as u64).into();
                    // self.stream_segment(starting_block).await?;
                }
                NextBlock::Block(cursor) => {
                    self.send_system_message(format!("single block {}", cursor), false)
                        .await;
                    current_block = cursor.into();
                }
                NextBlock::Invalidate => {}
            }
        }
        /*
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
        */

        Ok(())
    }

    /*
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
    */
}
