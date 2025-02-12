use std::collections::{BTreeMap, HashMap};

use apibara_dna_protocol::dna::stream::{
    stream_data_response::Message, Data, DataFinality, DataProduction, Finalize, Invalidate,
    StreamDataResponse,
};
use apibara_observability::{Histogram, KeyValue, UpDownCounter};
use bytes::{BufMut, Bytes, BytesMut};
use error_stack::{Result, ResultExt};
use futures::FutureExt;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{
    block_store::BlockStoreReader,
    chain_view::{ChainView, NextCursor},
    data_stream::{fragment_access::BlockAccess, FilterMatch, FragmentAccess, SegmentStream},
    file_cache::FileCacheError,
    fragment::{FragmentId, HEADER_FRAGMENT_ID},
    join::ArchivedJoinTo,
    query::{BlockFilter, HeaderFilter},
    Cursor,
};

#[derive(Debug)]
pub struct DataStreamError;

#[derive(Debug, Clone)]
pub struct DataStreamMetrics {
    pub active: UpDownCounter<i64>,
    pub block_size: Histogram<u64>,
    pub fragment_size: Histogram<u64>,
}

pub struct DataStream {
    block_filter: Vec<BlockFilter>,
    current: Option<Cursor>,
    finalized: Cursor,
    finality: DataFinality,
    chain_view: ChainView,
    store: BlockStoreReader,
    fragment_id_to_name: HashMap<FragmentId, String>,
    metrics: DataStreamMetrics,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

type DataStreamMessage = tonic::Result<StreamDataResponse, tonic::Status>;

const DEFAULT_BLOCKS_BUFFER_SIZE: usize = 1024 * 1024;
const SEGMENT_CHANNEL_SIZE: usize = 128;

impl DataStream {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        block_filter: Vec<BlockFilter>,
        starting: Option<Cursor>,
        finalized: Cursor,
        finality: DataFinality,
        chain_view: ChainView,
        fragment_id_to_name: HashMap<FragmentId, String>,
        store: BlockStoreReader,
        permit: tokio::sync::OwnedSemaphorePermit,
        metrics: DataStreamMetrics,
    ) -> Self {
        Self {
            block_filter,
            current: starting,
            finalized,
            finality,
            chain_view,
            fragment_id_to_name,
            store,
            metrics,
            _permit: permit,
        }
    }

    pub async fn start(
        mut self,
        tx: mpsc::Sender<DataStreamMessage>,
        ct: CancellationToken,
    ) -> Result<(), DataStreamError> {
        self.metrics.active.add(1, &[]);

        while !ct.is_cancelled() && !tx.is_closed() {
            tokio::select! {
                biased;

                _ = ct.cancelled() => break,
                res = self.tick(&tx, &ct) => {
                    res.change_context(DataStreamError)
                        .attach_printable("failed to tick data stream")?;
                },
            }
        }

        Ok(())
    }

    async fn tick(
        &mut self,
        tx: &mpsc::Sender<DataStreamMessage>,
        ct: &CancellationToken,
    ) -> Result<(), DataStreamError> {
        debug!(current = ?self.current,"tick: main loop");
        let (next_cursor, is_head) = match self
            .chain_view
            .get_next_cursor(&self.current)
            .await
            .change_context(DataStreamError)?
        {
            NextCursor::Continue { cursor, is_head } => (cursor, is_head),
            NextCursor::Invalidate(cursor) => {
                debug!(cursor = %cursor, "invalidating data");

                // TODO: collect removed blocks.
                let invalidate = Message::Invalidate(Invalidate {
                    cursor: Some(cursor.clone().into()),
                    ..Default::default()
                });

                let Some(Ok(permit)) = ct.run_until_cancelled(tx.reserve()).await else {
                    return Ok(());
                };

                permit.send(Ok(StreamDataResponse {
                    message: Some(invalidate),
                }));

                self.current = Some(cursor);

                return Ok(());
            }
            NextCursor::AtHead => {
                debug!("head reached. waiting for new head");

                if self.finality == DataFinality::Pending {
                    return self.tick_at_head_pending(tx, ct).await;
                } else {
                    return self.tick_at_head(tx, ct).await;
                }
            }
        };

        if self.finality == DataFinality::Finalized && next_cursor.strict_after(&self.finalized) {
            // Wait for the finalized cursor to catch up and then try again.
            tokio::select! {
                _ = ct.cancelled() => return Ok(()),
                _ = self.chain_view.finalized_changed() => {
                    debug!("finalized changed (finalized)");
                    self.finalized = self.chain_view.get_finalized_cursor().await.change_context(DataStreamError)?;
                    return Ok(());
                },
            }
        }

        if self
            .chain_view
            .has_segment_for_block(next_cursor.number)
            .await
        {
            return self.tick_segment_stream(next_cursor, tx, ct).await;
        }

        self.tick_single(next_cursor, is_head, tx, ct).await
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

    async fn tick_segment_stream(
        &mut self,
        cursor: Cursor,
        tx: &mpsc::Sender<DataStreamMessage>,
        ct: &CancellationToken,
    ) -> Result<(), DataStreamError> {
        debug!(cursor = %cursor, "tick: segment stream");

        let segment_stream = SegmentStream::new(
            self.block_filter.clone(),
            self.fragment_id_to_name.clone(),
            self.store.clone(),
            self.chain_view.clone(),
        );

        let (segment_tx, segment_rx) = mpsc::channel(SEGMENT_CHANNEL_SIZE);
        let segment_rx = ReceiverStream::new(segment_rx);
        tokio::pin!(segment_rx);

        let mut segment_stream_handle =
            tokio::spawn(segment_stream.start(cursor.clone(), segment_tx, ct.clone())).fuse();

        loop {
            tokio::select! {
                _ = ct.cancelled() => return Ok(()),
                segment_stream_result = &mut segment_stream_handle => {
                    debug!(result = ?segment_stream_result, "tick: segment stream finished");
                    segment_stream_result.change_context(DataStreamError)?.change_context(DataStreamError)?;
                }
                segment_result = segment_rx.next() => {
                    use apibara_dna_protocol::dna::stream::Cursor as ProtoCursor;

                    let Some(segment_fetch) = segment_result else {
                        debug!("tick: segment stream consumer finished");
                        return Ok(());
                    };
                    let segment_access = segment_fetch.wait().await.change_context(DataStreamError)
                        .attach_printable("Failed to wait for segment fetch")?;

                    let finality = DataFinality::Finalized;

                    for block_access in segment_access.iter() {
                        let block_end_cursor = block_access.cursor();

                        let proto_cursor = None;
                        let proto_end_cursor: Option<ProtoCursor> = Some(block_end_cursor.clone().into());

                        let fragment_access = FragmentAccess::Segment(block_access);
                        let mut blocks = Vec::new();
                        if self
                            .filter_fragment(fragment_access, &finality, false, &mut blocks)
                            .await?
                        {
                            let data = Message::Data(Data {
                                cursor: proto_cursor,
                                end_cursor: proto_end_cursor,
                                data: blocks,
                                finality: finality as i32,
                                production: DataProduction::Backfill.into(),
                            });

                            let Some(Ok(permit)) = ct.run_until_cancelled(tx.reserve()).await else {
                                return Ok(());
                            };

                            permit.send(Ok(StreamDataResponse {
                                message: Some(data),
                            }));
                        }

                        self.current = block_end_cursor.into();
                    }
                }
            }
        }
    }

    async fn tick_single(
        &mut self,
        cursor: Cursor,
        is_head: bool,
        tx: &mpsc::Sender<DataStreamMessage>,
        ct: &CancellationToken,
    ) -> Result<(), DataStreamError> {
        use apibara_dna_protocol::dna::stream::Cursor as ProtoCursor;

        debug!(cursor = %cursor, "tick: single block");

        let proto_cursor: Option<ProtoCursor> = self.current.clone().map(Into::into);
        let proto_end_cursor: Option<ProtoCursor> = Some(cursor.clone().into());

        let finalized = self
            .chain_view
            .get_finalized_cursor()
            .await
            .change_context(DataStreamError)?;

        let finality = if cursor.strict_after(&finalized) {
            DataFinality::Accepted
        } else {
            DataFinality::Finalized
        };

        let block_entry: BlockAccess = self
            .store
            .get_block(&cursor)
            .await
            .map_err(FileCacheError::Foyer)
            .change_context(DataStreamError)
            .attach_printable("failed to get single block")
            .attach_printable_lazy(|| format!("cursor: {}", cursor))?
            .into();

        let fragment_access = FragmentAccess::Block(block_entry);

        let mut blocks = Vec::new();

        if self
            .filter_fragment(fragment_access, &finality, is_head, &mut blocks)
            .await?
        {
            let data = Message::Data(Data {
                cursor: proto_cursor.clone(),
                end_cursor: proto_end_cursor.clone(),
                data: blocks,
                finality: finality.into(),
                production: if is_head {
                    DataProduction::Live.into()
                } else {
                    DataProduction::Backfill.into()
                },
            });

            let Some(Ok(permit)) = ct.run_until_cancelled(tx.reserve()).await else {
                return Ok(());
            };

            permit.send(Ok(StreamDataResponse {
                message: Some(data),
            }));
        }

        self.current = Some(cursor);

        Ok(())
    }

    async fn tick_at_head(
        &mut self,
        tx: &mpsc::Sender<DataStreamMessage>,
        ct: &CancellationToken,
    ) -> Result<(), DataStreamError> {
        loop {
            tokio::select! {
                _ = ct.cancelled() => return Ok(()),
                _ = self.chain_view.head_changed() => {
                    debug!("head changed (at head)");
                    return Ok(());
                },
                _ = self.chain_view.finalized_changed() => {
                    debug!("finalized changed (at head)");
                    self.finalized = self.chain_view.get_finalized_cursor().await.change_context(DataStreamError)?;
                    self.send_finalize_message(tx, ct).await?;
                },
            }
        }
    }

    async fn tick_at_head_pending(
        &mut self,
        tx: &mpsc::Sender<DataStreamMessage>,
        ct: &CancellationToken,
    ) -> Result<(), DataStreamError> {
        let mut pending_generation = self.chain_view.get_pending_generation().await;
        let mut content_hash = Vec::new();
        loop {
            if let Some(generation) = pending_generation.take() {
                if let Some(head) = &self.current {
                    self.send_pending_block(head, generation, &mut content_hash, tx, ct)
                        .await?;
                }
            }

            tokio::select! {
                biased;

                _ = ct.cancelled() => return Ok(()),
                _ = self.chain_view.head_changed() => {
                    debug!("head changed (pending)");
                    return Ok(());
                },
                _ = self.chain_view.finalized_changed() => {
                    debug!("finalized changed (pending)");
                    self.finalized = self.chain_view.get_finalized_cursor().await.change_context(DataStreamError)?;
                    self.send_finalize_message(tx, ct).await?;
                },
                _ = self.chain_view.pending_changed() => {
                    debug!("pending changed (pending)");
                    pending_generation = self.chain_view.get_pending_generation().await;
                }
            }
        }
    }

    async fn send_pending_block(
        &self,
        head: &Cursor,
        generation: u64,
        content_hash: &mut Vec<u8>,
        tx: &mpsc::Sender<DataStreamMessage>,
        ct: &CancellationToken,
    ) -> Result<(), DataStreamError> {
        use apibara_dna_protocol::dna::stream::Cursor as ProtoCursor;

        debug!("tick: pending block");

        let end_cursor = Cursor::new_pending(head.number + 1);

        debug!(cursor = %head, end_cursor = %end_cursor, "sending pending data");

        let proto_cursor: Option<ProtoCursor> = Some(head.clone().into());
        let proto_end_cursor: Option<ProtoCursor> = Some(end_cursor.clone().into());
        let finality = DataFinality::Pending;

        let block_entry: BlockAccess = self
            .store
            .get_pending_block(&end_cursor, generation)
            .await
            .map_err(FileCacheError::Foyer)
            .change_context(DataStreamError)
            .attach_printable("failed to get pending block")
            .attach_printable_lazy(|| format!("cursor: {}", end_cursor))
            .attach_printable_lazy(|| format!("generation: {}", generation))?
            .into();

        let fragment_access = FragmentAccess::Block(block_entry);

        let mut blocks = Vec::new();
        if self
            .filter_fragment(fragment_access, &finality, true, &mut blocks)
            .await?
        {
            use sha2::Digest;

            let mut hasher = sha2::Sha256::new();
            for block in &blocks {
                hasher.update(block.as_ref());
            }

            let new_content_hash = hasher.finalize().to_vec();

            if new_content_hash == *content_hash {
                return Ok(());
            }

            let data = Message::Data(Data {
                cursor: proto_cursor.clone(),
                end_cursor: proto_end_cursor.clone(),
                data: blocks,
                finality: finality.into(),
                production: DataProduction::Live.into(),
            });

            let Some(Ok(permit)) = ct.run_until_cancelled(tx.reserve()).await else {
                return Ok(());
            };

            permit.send(Ok(StreamDataResponse {
                message: Some(data),
            }));

            *content_hash = new_content_hash;
        }

        Ok(())
    }

    #[tracing::instrument(
        name = "send_data",
        skip_all,
        fields(blocks_count, blocks_size_bytes, fragments_count, fragments_size_bytes)
    )]
    async fn filter_fragment<'a>(
        &self,
        fragment_access: FragmentAccess<'a>,
        _finality: &DataFinality,
        is_live: bool,
        output: &mut Vec<Bytes>,
    ) -> Result<bool, DataStreamError> {
        let mut has_data = false;

        let mut total_fragments_size_bytes = Vec::with_capacity(self.block_filter.len());
        let mut total_blocks_size_bytes = Vec::with_capacity(self.block_filter.len());

        for block_filter in self.block_filter.iter() {
            let mut local_fragments_size_bytes = HashMap::<String, usize>::new();

            let mut data_buffer = BytesMut::with_capacity(DEFAULT_BLOCKS_BUFFER_SIZE);
            let mut fragment_matches = BTreeMap::default();

            let mut joins = BTreeMap::<(FragmentId, FragmentId), FilterMatch>::default();

            for (fragment_id, filters) in block_filter.iter() {
                let mut filter_match = FilterMatch::default();

                let indexes = fragment_access
                    .get_index_fragment(fragment_id)
                    .change_context(DataStreamError)
                    .attach_printable("failed to get fragment indexes")?;

                for filter in filters {
                    let rows = filter.filter(indexes).change_context(DataStreamError)?;
                    filter_match.add_match(filter.filter_id, &rows);

                    for join_with_fragment_id in filter.joins.iter() {
                        joins
                            .entry((*fragment_id, *join_with_fragment_id))
                            .or_default()
                            .add_match(filter.filter_id, &rows);
                    }
                }

                if filter_match.is_empty() {
                    continue;
                }

                fragment_matches.insert(*fragment_id, filter_match);
            }

            for ((source_fragment_id, target_fragment_id), filter_match) in joins.into_iter() {
                // Data is cached so it's fine to read it multiple times.
                // We could group by `source_fragment_id` to cleanup the code.
                let join_fragment = fragment_access
                    .get_join_fragment(&source_fragment_id)
                    .change_context(DataStreamError)
                    .attach_printable("failed to get join fragment")?;

                let Some(target_pos) = join_fragment
                    .joins
                    .iter()
                    .position(|f| f.to_fragment_id == target_fragment_id)
                else {
                    return Err(DataStreamError)
                        .attach_printable("join fragment not found")
                        .attach_printable_lazy(|| {
                            format!("source fragment id: {}", source_fragment_id)
                        })
                        .attach_printable_lazy(|| {
                            format!("target fragment id: {}", target_fragment_id)
                        });
                };
                let join = &join_fragment.joins[target_pos];

                let target_fragment_matches =
                    fragment_matches.entry(target_fragment_id).or_default();

                match &join.index {
                    ArchivedJoinTo::One(inner) => {
                        for match_ in filter_match.iter() {
                            if let Some(index) = inner.get(&match_.index) {
                                for filter_id in match_.filter_ids.iter() {
                                    target_fragment_matches.add_single_match(*filter_id, index);
                                }
                            }
                        }
                    }
                    ArchivedJoinTo::Many(inner) => {
                        for match_ in filter_match.iter() {
                            if let Some(bitmap) = inner.get(&match_.index) {
                                for filter_id in match_.filter_ids.iter() {
                                    target_fragment_matches.add_match(*filter_id, &bitmap);
                                }
                            }
                        }
                    }
                }
            }

            let should_send_header = match block_filter.header_filter {
                HeaderFilter::Always => true,
                HeaderFilter::OnData => !fragment_matches.is_empty(),
                HeaderFilter::OnDataOrOnNewBlock => !fragment_matches.is_empty() || is_live,
            };

            if should_send_header {
                let header = fragment_access
                    .get_header_fragment()
                    .change_context(DataStreamError)
                    .attach_printable("failed to get header fragment")?;

                prost::encoding::encode_key(
                    HEADER_FRAGMENT_ID as u32,
                    prost::encoding::WireType::LengthDelimited,
                    &mut data_buffer,
                );
                prost::encoding::encode_varint(header.data.len() as u64, &mut data_buffer);
                data_buffer.put(header.data.as_slice());
            }

            for (fragment_id, filter_match) in fragment_matches.into_iter() {
                let Some(fragment_name) = self.fragment_id_to_name.get(&fragment_id).cloned()
                else {
                    return Err(DataStreamError)
                        .attach_printable("unknown fragment id")
                        .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
                };

                let body = fragment_access
                    .get_body_fragment(&fragment_id)
                    .change_context(DataStreamError)
                    .attach_printable("failed to get body fragment")?;

                let starting_size = data_buffer.len();
                for match_ in filter_match.iter() {
                    const FILTER_IDS_TAG: u32 = 1;

                    let message_bytes = &body.data[match_.index as usize];
                    let filter_ids_len = prost::encoding::uint32::encoded_len_packed(
                        FILTER_IDS_TAG,
                        &match_.filter_ids,
                    );

                    prost::encoding::encode_key(
                        fragment_id as u32,
                        prost::encoding::WireType::LengthDelimited,
                        &mut data_buffer,
                    );

                    prost::encoding::encode_varint(
                        (filter_ids_len + message_bytes.len()) as u64,
                        &mut data_buffer,
                    );

                    prost::encoding::uint32::encode_packed(
                        FILTER_IDS_TAG,
                        &match_.filter_ids,
                        &mut data_buffer,
                    );
                    data_buffer.put(message_bytes.as_slice());
                }

                let fragment_size = data_buffer.len() - starting_size;
                *local_fragments_size_bytes.entry(fragment_name).or_default() += fragment_size;
            }

            if !data_buffer.is_empty() {
                has_data = true;
            }

            total_blocks_size_bytes.push(data_buffer.len());
            total_fragments_size_bytes.push(local_fragments_size_bytes);

            output.push(data_buffer.freeze());
        }

        if has_data {
            for block_size in total_blocks_size_bytes {
                self.metrics.block_size.record(block_size as u64, &[]);
            }

            for block_fragment_size_bytes in total_fragments_size_bytes {
                for (fragment_name, fragment_size_bytes) in block_fragment_size_bytes {
                    self.metrics.fragment_size.record(
                        fragment_size_bytes as u64,
                        &[KeyValue::new("name", fragment_name)],
                    );
                }
            }
        }

        Ok(has_data)
    }
}

impl error_stack::Context for DataStreamError {}

impl std::fmt::Display for DataStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "data stream error")
    }
}

impl Default for DataStreamMetrics {
    fn default() -> Self {
        let meter = apibara_observability::meter("dna_data_stream");

        Self {
            active: meter
                .i64_up_down_counter("dna.data_stream.active")
                .with_description("number of active data streams")
                .with_unit("{connection}")
                .build(),
            block_size: meter
                .u64_histogram("dna.data_stream.block_size")
                .with_description("size (in bytes) of blocks sent to the client")
                .with_unit("By")
                .with_boundaries(vec![
                    1_000.0,
                    10_000.0,
                    100_000.0,
                    1_000_000.0,
                    5_000_000.0,
                    10_000_000.0,
                    25_000_000.0,
                    50_000_000.0,
                    100_000_000.0,
                    1_000_000_000.0,
                ])
                .build(),
            fragment_size: meter
                .u64_histogram("dna.data_stream.fragment_size")
                .with_description("size (in bytes) of fragments sent to the client")
                .with_unit("By")
                .with_boundaries(vec![
                    1_000.0,
                    10_000.0,
                    100_000.0,
                    1_000_000.0,
                    5_000_000.0,
                    10_000_000.0,
                    25_000_000.0,
                    50_000_000.0,
                    100_000_000.0,
                    1_000_000_000.0,
                ])
                .build(),
        }
    }
}

impl Drop for DataStream {
    fn drop(&mut self) {
        self.metrics.active.add(-1, &[]);
    }
}
