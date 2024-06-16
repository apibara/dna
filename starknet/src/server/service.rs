use std::time::Duration;

use apibara_dna_common::{
    core::Cursor,
    error::{DnaError, Result},
    segment::SegmentOptions,
    server::{BlockNumberOrCursor, CursorProducer, NextBlock},
    storage::{CachedStorage, LocalStorageBackend, StorageBackend},
};
use apibara_dna_protocol::{
    dna::{
        common::{StatusRequest, StatusResponse},
        stream::{
            self, dna_stream_server, stream_data_response, Data, DataFinality, StreamDataRequest,
            StreamDataResponse,
        },
    },
    starknet,
};
use error_stack::ResultExt;
use futures_util::TryFutureExt;
use prost::Message;
use roaring::RoaringBitmap;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use tracing::{debug, error};

use super::filter::SegmentFilter;

type TonicResult<T> = std::result::Result<T, tonic::Status>;

pub struct DnaService<S>
where
    S: StorageBackend + Send + Sync + 'static,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    storage: CachedStorage<S>,
    local_storage: LocalStorageBackend,
    cursor_producer: CursorProducer,
}

impl<S> DnaService<S>
where
    S: StorageBackend + Send + Sync + 'static + Clone,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    pub fn new(
        storage: CachedStorage<S>,
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

    #[tracing::instrument(skip_all)]
    pub async fn segment_options(&self) -> Result<SegmentOptions> {
        for _ in 0..5 {
            if let Some(segment_options) = self.cursor_producer.segment_options().await {
                return Ok(segment_options);
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Err(DnaError::Fatal).attach_printable("failed to get segment options")
    }
}

#[tonic::async_trait]
impl<S> dna_stream_server::DnaStream for DnaService<S>
where
    S: StorageBackend + Send + Sync + 'static + Clone,
    <S as StorageBackend>::Reader: Unpin + Send,
{
    type StreamDataStream = ReceiverStream<TonicResult<StreamDataResponse>>;

    #[tracing::instrument(name = "stream_data_request", skip_all)]
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
            .map(|f| <starknet::Filter as prost::Message>::decode(f.as_slice()))
            .collect::<std::result::Result<Vec<starknet::Filter>, _>>()
            .map_err(|_| tonic::Status::invalid_argument("failed to decode filter"))?;

        let segment_options = self
            .segment_options()
            .await
            .map_err(|_| tonic::Status::unavailable("DNA server is not ready yet"))?;

        let filter = SegmentFilter::new(
            filters,
            self.storage.clone(),
            self.local_storage.clone(),
            segment_options,
        );

        let producer = StreamProducer::init(
            filter,
            starting_block,
            tx,
            self.storage.clone(),
            self.local_storage.clone(),
            cursor_producer,
        );

        tokio::spawn(producer.start().inspect_err(|err| {
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
    storage: CachedStorage<S>,
    local_storage: LocalStorageBackend,
    filter: SegmentFilter<S>,
    cursor_producer: CursorProducer,
    response_tx: mpsc::Sender<TonicResult<StreamDataResponse>>,
    stream_starting_block: BlockNumberOrCursor,
}

impl<S> StreamProducer<S>
where
    S: StorageBackend + Send + Sync + 'static + Clone,
    <S as StorageBackend>::Reader: Unpin + Send + 'static,
{
    pub fn init(
        filter: SegmentFilter<S>,
        stream_starting_block: BlockNumberOrCursor,
        response_tx: mpsc::Sender<TonicResult<StreamDataResponse>>,
        storage: CachedStorage<S>,
        local_storage: LocalStorageBackend,
        cursor_producer: CursorProducer,
    ) -> Self {
        StreamProducer {
            filter,
            stream_starting_block,
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

    pub async fn start(mut self) -> Result<()> {
        debug!(num_filters = %self.filter.filter_len(), "starting data stream");

        let mut current_block = self.stream_starting_block.clone();

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
                    let ending_block = self
                        .send_segment_group(starting_block, segment_options)
                        .await?;
                    current_block = ending_block.into();
                }
                NextBlock::Segment(starting_block, segment_options) => {
                    let ending_block = self
                        .send_single_segment(starting_block, segment_options)
                        .await?;
                    current_block = ending_block.into();
                }
                NextBlock::Block(cursor) => {
                    /*
                    let block = single_block_reader
                        .read(&cursor)
                        .await
                        .change_context(DnaError::Fatal)
                        .attach_printable("failed to read single block")?;

                    self.filter_and_send_single_block(&cursor, &block).await;
                    */
                    self.send_system_message(format!("block {cursor}"), false)
                        .await;

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

    #[tracing::instrument(skip(self, segment_options), err(Debug))]
    async fn send_segment_group(
        &mut self,
        starting_block: u64,
        segment_options: SegmentOptions,
    ) -> Result<u64> {
        let ending_block = starting_block + segment_options.segment_group_blocks() - 1;

        // Notice that if the user requested data from block X + Y, the segment
        // group may contain blocks between X and X + Y.
        // Here we filter those blocks out.
        let real_starting_block = u64::max(self.stream_starting_block.number(), starting_block);
        let block_range = real_starting_block as u32..=ending_block as u32;

        debug!(block_range = ?block_range, "filling block bitmap");

        let mut block_bitmap = RoaringBitmap::new();

        self.filter
            .fill_block_bitmap(&mut block_bitmap, starting_block, block_range)
            .await?;

        for block_number in block_bitmap.iter() {
            // Avoid useless work.
            if self.response_tx.is_closed() {
                break;
            }

            let block_number = block_number as u64;

            if let Some(blocks) = self.filter.filter_segment_block(block_number).await? {
                let cursor = if block_number == 0 {
                    None
                } else {
                    Some(Cursor::new_finalized(block_number - 1).into())
                };

                let data = blocks
                    .into_iter()
                    .map(|block| block.encode_to_vec())
                    .collect();

                let data = Data {
                    data,
                    finality: DataFinality::Finalized as i32,
                    cursor,
                    end_cursor: Some(Cursor::new_finalized(block_number).into()),
                };

                self.send_data(data).await;
            }
        }

        Ok(ending_block)
    }

    #[tracing::instrument(skip(self, segment_options), err(Debug))]
    async fn send_single_segment(
        &mut self,
        starting_block: u64,
        segment_options: SegmentOptions,
    ) -> Result<u64> {
        let ending_block = starting_block + segment_options.segment_size as u64 - 1;
        let real_starting_block = u64::max(self.stream_starting_block.number(), starting_block);

        for block_number in real_starting_block..=ending_block {
            // Avoid useless work.
            if self.response_tx.is_closed() {
                break;
            }

            let block_number = block_number as u64;

            if let Some(blocks) = self.filter.filter_segment_block(block_number).await? {
                let cursor = if block_number == 0 {
                    None
                } else {
                    Some(Cursor::new_finalized(block_number - 1).into())
                };

                let data = blocks
                    .into_iter()
                    .map(|block| block.encode_to_vec())
                    .collect();

                let data = Data {
                    data,
                    finality: DataFinality::Finalized as i32,
                    cursor,
                    end_cursor: Some(Cursor::new_finalized(block_number).into()),
                };

                self.send_data(data).await;
            }
        }

        Ok(ending_block)
    }
}
