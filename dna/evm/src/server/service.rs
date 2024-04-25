use std::{ops::RangeInclusive, time::Duration};

use apibara_dna_common::{
    core::Cursor,
    error::{DnaError, Result},
    storage::{LocalStorageBackend, StorageBackend},
};
use apibara_dna_protocol::{
    dna::{
        common::{StatusRequest, StatusResponse},
        stream::{
            self, dna_stream_server, stream_data_response, Data, DataFinality, StreamDataRequest,
            StreamDataResponse,
        },
    },
    evm,
};
use error_stack::ResultExt;
use futures_util::TryFutureExt;
use prost::Message;
use roaring::RoaringBitmap;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tracing::{debug, error};

use crate::{
    segment::{
        store::{self, SingleBlock},
        BlockSegment, BlockSegmentReader, BlockSegmentReaderOptions, SegmentGroupExt,
        SegmentGroupReader, SingleBlockReader,
    },
    server::cursor_producer::NextBlock,
};

use super::cursor_producer::{BlockNumberOrCursor, CursorProducer};
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
        match self.cursor_producer.most_recent_available_block().await {
            None => {
                return Err(tonic::Status::failed_precondition(
                    "no block is available yet",
                ));
            }
            Some(cursor_or_number) => {
                let cursor: Cursor = cursor_or_number.into();
                let response = StatusResponse {
                    current_head: Some(cursor.clone().into()),
                    last_ingested: Some(cursor.into()),
                };

                Ok(tonic::Response::new(response))
            }
        }
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

    pub async fn send_data(&self, data: Data) {
        self.send_message(stream_data_response::Message::Data(data))
            .await;
    }

    #[tracing::instrument(name = "stream_data", skip_all, err(Debug))]
    pub async fn start(self, stream_starting_block: BlockNumberOrCursor) -> Result<()> {
        debug!(num_filters = %self.filters.len(), "starting data stream");

        let mut current_block = stream_starting_block.clone();

        let segment_options = 'outer: loop {
            for _ in 0..5 {
                if let Some(segment_options) = self.cursor_producer.segment_options().await {
                    break 'outer segment_options;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }

            return Err(DnaError::Fatal).attach_printable("failed to get segment options");
        };

        let mut segment_group_reader = SegmentGroupReader::new(
            self.storage.clone(),
            segment_options.clone(),
            DEFAULT_BUFFER_SIZE,
        );

        let mut block_segment_reader = {
            let options = self
                .filters
                .iter()
                .fold(BlockSegmentReaderOptions::default(), |options, filter| {
                    filter.block_segment_reader_options().merge(&options)
                });
            debug!(?options, "create block segment reader");
            BlockSegmentReader::new(self.storage.clone(), segment_options.clone(), options)
        };

        let mut single_block_reader =
            SingleBlockReader::new(self.local_storage.clone(), DEFAULT_BUFFER_SIZE);

        let mut block_bitmap = RoaringBitmap::new();

        loop {
            if self.response_tx.is_closed() {
                debug!("response channel closed. stopping stream");
                break;
            }

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
                    debug!("head reached. waiting for new head");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                NextBlock::SegmentGroup(starting_block, segment_options) => {
                    let ending_block = starting_block + segment_options.segment_group_blocks() - 1;

                    let segment_group = segment_group_reader
                        .read(starting_block)
                        .await
                        .change_context(DnaError::Fatal)
                        .attach_printable("failed to read segment group")?;

                    // assert_eq!(segment_group.first_block_number(), starting_block);

                    // Notice that if the user requested data from block X + Y, the segment
                    // group may contain blocks between X and X + Y.
                    // Here we filter those blocks out.
                    let real_starting_block =
                        u64::max(stream_starting_block.number(), starting_block);
                    let block_range = real_starting_block as u32..=ending_block as u32;
                    self.fill_segment_group_bitmap(&segment_group, &mut block_bitmap, block_range);

                    let mut current_segment_start =
                        segment_options.segment_start(real_starting_block);

                    let mut block_segment = block_segment_reader
                        .read(current_segment_start)
                        .await
                        .change_context(DnaError::Fatal)
                        .attach_printable("failed to read block segment")?;

                    'block_iter: for block_number in block_bitmap.iter() {
                        debug!(
                            block_number,
                            current_segment_start,
                            new_start = %segment_options.segment_start(block_number as u64),
                            "sending group from block"
                        );

                        // Avoid useless work.
                        if self.response_tx.is_closed() {
                            break 'block_iter;
                        }

                        if current_segment_start
                            < segment_options.segment_start(block_number as u64)
                        {
                            debug!("reading new segment");
                            current_segment_start =
                                segment_options.segment_start(block_number as u64);
                            block_segment = block_segment_reader
                                .read(current_segment_start)
                                .await
                                .change_context(DnaError::Fatal)
                                .attach_printable("failed to read block segment")?;
                        }

                        let relative_index =
                            block_number - block_segment.header.first_block_number() as u32;

                        self.filter_and_send_segment(
                            block_number as u64,
                            relative_index as usize,
                            &block_segment,
                        )
                        .await;
                    }

                    current_block = ending_block.into();
                }
                NextBlock::Segment(starting_block, segment_options) => {
                    let ending_block = starting_block + segment_options.segment_size as u64 - 1;
                    let real_starting_block =
                        u64::max(stream_starting_block.number(), starting_block);

                    let current_segment_start = segment_options.segment_start(real_starting_block);

                    let block_segment = block_segment_reader
                        .read(current_segment_start)
                        .await
                        .change_context(DnaError::Fatal)
                        .attach_printable("failed to read block segment")?;

                    'block_iter: for block_number in real_starting_block..=ending_block {
                        // Avoid useless work.
                        if self.response_tx.is_closed() {
                            break 'block_iter;
                        }

                        let relative_index =
                            block_number - block_segment.header.first_block_number();

                        self.filter_and_send_segment(
                            block_number as u64,
                            relative_index as usize,
                            &block_segment,
                        )
                        .await;
                    }

                    current_block = ending_block.into();
                }
                NextBlock::Block(cursor) => {
                    let block = single_block_reader
                        .read(&cursor)
                        .await
                        .change_context(DnaError::Fatal)
                        .attach_printable("failed to read single block")?;

                    self.filter_and_send_single_block(&cursor, &block).await;

                    current_block = cursor.clone().into();
                }
                NextBlock::Invalidate => {
                    self.send_system_message("chain reorganization not handled. bye.", true)
                        .await;
                    todo!();
                }
            }
        }

        debug!("streaming complete");

        Ok(())
    }

    fn fill_segment_group_bitmap<'a>(
        &self,
        segment_group: &store::SegmentGroup<'a>,
        block_bitmap: &mut RoaringBitmap,
        block_range: RangeInclusive<u32>,
    ) {
        block_bitmap.clear();

        let first_block_in_range = *block_range.start();
        // Use the indices stored in the segment group to build the block bitmap.
        // TODO: what if an index is missing? We should fill the bitmap densely.
        'filter: for filter in &self.filters {
            if filter.has_required_header() || filter.has_withdrawals() || filter.has_transactions()
            {
                block_bitmap.insert_range(block_range);
                break 'filter;
            }

            for log in filter.logs() {
                let any_address = log.address.is_none();
                let any_topic =
                    log.topics.is_empty() || log.topics.iter().any(|t| t.value.is_none());

                if any_address && any_topic {
                    block_bitmap.insert_range(block_range);
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

        if let Some(min_in_range) = block_bitmap.min() {
            if min_in_range < first_block_in_range {
                block_bitmap.remove_range(..first_block_in_range);
            }
        }
    }

    async fn filter_and_send_segment<'a>(
        &self,
        block_number: u64,
        relative_index: usize,
        block_segment: &BlockSegment<'a>,
    ) {
        let mut data = Vec::with_capacity(self.filters.len());
        let mut has_block = false;

        for filter in &self.filters {
            match filter.filter_segment_block_data(relative_index as usize, &block_segment) {
                None => data.push(Vec::new()),
                Some(block) => {
                    has_block = true;
                    data.push(block.encode_to_vec());
                }
            }
        }

        if has_block {
            let data = Data {
                data,
                finality: DataFinality::Finalized as i32,
                cursor: None,
                end_cursor: Some(Cursor::new_finalized(block_number).into()),
            };

            self.send_data(data).await;
        }
    }

    async fn filter_and_send_single_block<'a>(&self, cursor: &Cursor, block: &SingleBlock<'a>) {
        let mut data = Vec::with_capacity(self.filters.len());

        for filter in &self.filters {
            match filter.filter_single_block_data(block) {
                None => data.push(Vec::new()),
                Some(block) => {
                    data.push(block.encode_to_vec());
                }
            }
        }

        let finality = if self.cursor_producer.is_block_finalized(cursor).await {
            DataFinality::Finalized
        } else {
            DataFinality::Accepted
        };

        let data = Data {
            data,
            finality: finality as i32,
            cursor: None,
            end_cursor: Some(cursor.clone().into()),
        };

        self.send_data(data).await;
    }
}
