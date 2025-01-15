use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Duration,
};

use apibara_dna_protocol::dna::stream::{
    stream_data_response::Message, Data, DataFinality, Finalize, Heartbeat, Invalidate,
    StreamDataResponse,
};
use bytes::{BufMut, Bytes, BytesMut};
use error_stack::{Result, ResultExt};
use roaring::RoaringBitmap;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use valuable::Valuable;

use crate::{
    block_store::BlockStoreReader,
    chain_view::{CanonicalCursor, ChainView, NextCursor},
    data_stream::{FilterMatch, FragmentAccess},
    file_cache::FileCacheError,
    fragment::{FragmentId, HEADER_FRAGMENT_ID, INDEX_FRAGMENT_ID, JOIN_FRAGMENT_ID},
    join::ArchivedJoinTo,
    query::{BlockFilter, HeaderFilter},
    segment::SegmentGroup,
    Cursor,
};

#[derive(Debug)]
pub struct DataStreamError;

pub struct DataStream {
    block_filter: Vec<BlockFilter>,
    current: Option<Cursor>,
    finalized: Cursor,
    finality: DataFinality,
    chain_view: ChainView,
    store: BlockStoreReader,
    fragment_id_to_name: HashMap<FragmentId, String>,
    heartbeat_interval: tokio::time::Interval,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

type DataStreamMessage = tonic::Result<StreamDataResponse, tonic::Status>;

const DEFAULT_BLOCKS_BUFFER_SIZE: usize = 1024 * 1024;

/// Information about a block in a segment.
#[derive(Debug, Clone)]
struct SegmentBlock {
    /// The block's cursor.
    pub cursor: Option<Cursor>,
    /// The block's end cursor.
    pub end_cursor: Cursor,
    /// Offset of the block in the segment.
    pub offset: usize,
}

impl DataStream {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        block_filter: Vec<BlockFilter>,
        starting: Option<Cursor>,
        finalized: Cursor,
        finality: DataFinality,
        heartbeat_interval: Duration,
        chain_view: ChainView,
        fragment_id_to_name: HashMap<FragmentId, String>,
        store: BlockStoreReader,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Self {
        let heartbeat_interval = tokio::time::interval(heartbeat_interval);
        Self {
            block_filter,
            current: starting,
            finalized,
            finality,
            heartbeat_interval,
            chain_view,
            fragment_id_to_name,
            store,
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
                    cursor: Some(cursor.clone().into()),
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
                if self.finality == DataFinality::Pending {
                    let mut pending_generation = self.chain_view.get_pending_generation().await;
                    let mut content_hash = Vec::new();
                    loop {
                        if let Some(generation) = pending_generation.take() {
                            if let Some(head) = &self.current {
                                self.tick_pending(head, generation, &mut content_hash, tx, ct)
                                    .await?;
                                // Reset here so that tick_pending doesn't need to be mutable.
                                self.heartbeat_interval.reset();
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
                            _ = self.heartbeat_interval.tick() => {
                                debug!("heartbeat (pending)");
                                self.send_heartbeat_message(tx, ct).await?;
                            }
                        }
                    }
                } else {
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
                            self.finalized = self.chain_view.get_finalized_cursor().await.change_context(DataStreamError)?;
                            return self.send_finalize_message(tx, ct).await;
                        },
                    }
                }
            }
        };

        if self.finality == DataFinality::Finalized && next_cursor.strict_after(&self.finalized) {
            // Wait for the finalized cursor to catch up and then try again.
            // Keep sending heartbeats while waiting.
            loop {
                tokio::select! {
                    _ = ct.cancelled() => return Ok(()),
                    _ = self.heartbeat_interval.tick() => {
                        debug!("heartbeat (finalized)");
                        self.send_heartbeat_message(tx, ct).await?;
                    }
                    _ = self.chain_view.finalized_changed() => {
                        debug!("finalized changed (finalized)");
                        self.finalized = self.chain_view.get_finalized_cursor().await.change_context(DataStreamError)?;
                        return Ok(());
                    },
                }
            }
        }

        if self
            .chain_view
            .has_group_for_block(next_cursor.number)
            .await
        {
            return self.tick_group(next_cursor, tx, ct).await;
        }

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

    async fn tick_group(
        &mut self,
        cursor: Cursor,
        tx: &mpsc::Sender<DataStreamMessage>,
        ct: &CancellationToken,
    ) -> Result<(), DataStreamError> {
        debug!("tick: group");

        let group_start = self.chain_view.get_group_start_block(cursor.number).await;
        let group_end = self.chain_view.get_group_end_block(cursor.number).await;

        let group_start_cursor = Cursor::new_finalized(group_start);
        let mut data_bitmap = RoaringBitmap::default();
        let mut all_fragment_ids =
            HashSet::from([INDEX_FRAGMENT_ID, JOIN_FRAGMENT_ID, HEADER_FRAGMENT_ID]);

        {
            let group_bytes = self
                .store
                .get_group(&group_start_cursor)
                .await
                .map_err(FileCacheError::Foyer)
                .change_context(DataStreamError)
                .attach_printable("failed to get group")?;
            let group =
                unsafe { rkyv::access_unchecked::<rkyv::Archived<SegmentGroup>>(&group_bytes) };

            for block_filter in self.block_filter.iter() {
                for (fragment_id, filters) in block_filter.iter() {
                    let Some(pos) = group
                        .index
                        .indexes
                        .iter()
                        .position(|f| f.fragment_id == *fragment_id)
                    else {
                        return Err(DataStreamError)
                            .attach_printable("missing index")
                            .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
                    };

                    let indexes = &group.index.indexes[pos];

                    for filter in filters {
                        let rows = filter.filter(indexes).change_context(DataStreamError)?;
                        if rows.is_empty() {
                            continue;
                        }

                        data_bitmap |= &rows;
                        all_fragment_ids.insert(*fragment_id);
                    }
                }
            }
        }

        debug!(blocks = ?data_bitmap, "group bitmap");

        let mut segments = Vec::new();
        let mut current_segment_data = Vec::default();

        let mut current_segment_start = self.chain_view.get_segment_start_block(group_start).await;
        let mut current_segment_end = self.chain_view.get_segment_end_block(group_start).await;

        // let mut prefetch_tasks = JoinSet::new();
        for block_number in data_bitmap.iter() {
            if block_number < cursor.number as u32 {
                continue;
            }

            let block_number = block_number as u64;

            if block_number > current_segment_end {
                let blocks = std::mem::take(&mut current_segment_data);
                let current_segment_cursor = Cursor::new_finalized(current_segment_start);

                // Prefetch all segments for this group.
                for fragment_id in all_fragment_ids.iter() {
                    let Some(fragment_name) = self.fragment_id_to_name.get(fragment_id).cloned()
                    else {
                        return Err(DataStreamError)
                            .attach_printable("unknown fragment id")
                            .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
                    };

                    self.store
                        .get_segment(&current_segment_cursor, fragment_name);
                }

                segments.push((current_segment_cursor, blocks));

                current_segment_start = self.chain_view.get_segment_start_block(block_number).await;
                current_segment_end = self.chain_view.get_segment_end_block(block_number).await;
            }

            let CanonicalCursor::Canonical(block_cursor) = self
                .chain_view
                .get_canonical(block_number)
                .await
                .change_context(DataStreamError)?
            else {
                return Err(DataStreamError)
                    .attach_printable("missing canonical block")
                    .attach_printable_lazy(|| format!("block number: {}", block_number));
            };

            let previous_cursor = if block_number == 0 {
                None
            } else if let CanonicalCursor::Canonical(previous_cursor) = self
                .chain_view
                .get_canonical(block_number - 1)
                .await
                .change_context(DataStreamError)?
            {
                previous_cursor.into()
            } else {
                None
            };

            current_segment_data.push(SegmentBlock {
                cursor: previous_cursor,
                end_cursor: block_cursor.clone(),
                offset: (block_number - current_segment_start) as usize,
            });
        }

        let blocks = std::mem::take(&mut current_segment_data);
        let current_segment_cursor = Cursor::new_finalized(current_segment_start);

        for fragment_id in all_fragment_ids.iter() {
            let Some(fragment_name) = self.fragment_id_to_name.get(fragment_id).cloned() else {
                return Err(DataStreamError)
                    .attach_printable("unknown fragment id")
                    .attach_printable_lazy(|| format!("fragment id: {}", fragment_id));
            };

            self.store
                .get_segment(&current_segment_cursor, fragment_name);
        }

        segments.push((current_segment_cursor, blocks));

        // prefetch_tasks.join_all().await;
        let finality = DataFinality::Finalized;

        for (segment_cursor, segment_data) in segments {
            if ct.is_cancelled() || tx.is_closed() {
                return Ok(());
            }

            for block in segment_data {
                use apibara_dna_protocol::dna::stream::Cursor as ProtoCursor;

                let fragment_access = FragmentAccess::new_in_segment(
                    self.store.clone(),
                    segment_cursor.clone(),
                    block.offset,
                );

                let proto_cursor: Option<ProtoCursor> = block.cursor.map(Into::into);
                let proto_end_cursor: Option<ProtoCursor> = Some(block.end_cursor.clone().into());

                let mut blocks = Vec::new();
                if self
                    .filter_fragment(&fragment_access, &finality, &mut blocks)
                    .await?
                {
                    let data = Message::Data(Data {
                        cursor: proto_cursor.clone(),
                        end_cursor: proto_end_cursor.clone(),
                        data: blocks,
                        finality: finality as i32,
                    });

                    let Some(Ok(permit)) = ct.run_until_cancelled(tx.reserve()).await else {
                        return Ok(());
                    };

                    permit.send(Ok(StreamDataResponse {
                        message: Some(data),
                    }));
                }
            }
        }

        let CanonicalCursor::Canonical(group_end_cursor) = self
            .chain_view
            .get_canonical(group_end)
            .await
            .change_context(DataStreamError)?
        else {
            return Err(DataStreamError).attach_printable("missing canonical block");
        };

        self.heartbeat_interval.reset();
        self.current = group_end_cursor.into();

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

            let block_number = starting_block_number + i;

            if block_number < cursor.number {
                continue;
            }

            let CanonicalCursor::Canonical(next_cursor) = self
                .chain_view
                .get_canonical(block_number)
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

        let finality = DataFinality::Finalized;

        for block in blocks {
            use apibara_dna_protocol::dna::stream::Cursor as ProtoCursor;

            let fragment_access = FragmentAccess::new_in_segment(
                self.store.clone(),
                segment_cursor.clone(),
                block.offset,
            );
            let proto_cursor: Option<ProtoCursor> = block.cursor.map(Into::into);
            let proto_end_cursor: Option<ProtoCursor> = Some(block.end_cursor.clone().into());

            let mut blocks = Vec::new();
            if self
                .filter_fragment(&fragment_access, &finality, &mut blocks)
                .await?
            {
                let data = Message::Data(Data {
                    cursor: proto_cursor.clone(),
                    end_cursor: proto_end_cursor.clone(),
                    data: blocks,
                    finality: finality as i32,
                });

                let Some(Ok(permit)) = ct.run_until_cancelled(tx.reserve()).await else {
                    return Ok(());
                };

                permit.send(Ok(StreamDataResponse {
                    message: Some(data),
                }));
            }
        }

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

        debug!(cursor = ?self.current, end_cursor = %cursor, "sending data");

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

        let fragment_access = FragmentAccess::new_in_block(self.store.clone(), cursor.clone());

        let mut blocks = Vec::new();
        if self
            .filter_fragment(&fragment_access, &finality, &mut blocks)
            .await?
        {
            let data = Message::Data(Data {
                cursor: proto_cursor.clone(),
                end_cursor: proto_end_cursor.clone(),
                data: blocks,
                finality: finality.into(),
            });

            let Some(Ok(permit)) = ct.run_until_cancelled(tx.reserve()).await else {
                return Ok(());
            };

            permit.send(Ok(StreamDataResponse {
                message: Some(data),
            }));
        }

        self.heartbeat_interval.reset();
        self.current = Some(cursor);

        Ok(())
    }

    async fn tick_pending(
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

        let fragment_access =
            FragmentAccess::new_in_pending_block(self.store.clone(), end_cursor, generation);

        let mut blocks = Vec::new();
        if self
            .filter_fragment(&fragment_access, &finality, &mut blocks)
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
    async fn filter_fragment(
        &self,
        fragment_access: &FragmentAccess,
        finality: &DataFinality,
        output: &mut Vec<Bytes>,
    ) -> Result<bool, DataStreamError> {
        let current_span = tracing::Span::current();
        let mut has_data = false;

        let mut field_fragments_sent = HashMap::<String, usize>::new();
        let mut field_fragments_size_bytes = HashMap::<String, usize>::new();
        let mut field_blocks_size_bytes = 0;

        for block_filter in self.block_filter.iter() {
            let mut data_buffer = BytesMut::with_capacity(DEFAULT_BLOCKS_BUFFER_SIZE);
            let mut fragment_matches = BTreeMap::default();

            let mut joins = BTreeMap::<(FragmentId, FragmentId), FilterMatch>::default();

            for (fragment_id, filters) in block_filter.iter() {
                let mut filter_match = FilterMatch::default();

                let indexes = fragment_access
                    .get_fragment_indexes(*fragment_id)
                    .await
                    .change_context(DataStreamError)
                    .attach_printable("failed to get fragment indexes")?;
                let indexes = indexes.access().change_context(DataStreamError)?;

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
                    .get_fragment_joins(source_fragment_id)
                    .await
                    .change_context(DataStreamError)
                    .attach_printable("failed to get join fragment")?;
                let join_fragment = join_fragment.access().change_context(DataStreamError)?;

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
                HeaderFilter::OnDataOrOnNewBlock => {
                    !fragment_matches.is_empty() || *finality != DataFinality::Finalized
                }
            };

            if should_send_header {
                let header = fragment_access
                    .get_header_fragment()
                    .await
                    .change_context(DataStreamError)
                    .attach_printable("failed to get header fragment")?;
                let header = header.access().change_context(DataStreamError)?;

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
                    .get_body_fragment(fragment_id, fragment_name.clone())
                    .await
                    .change_context(DataStreamError)
                    .attach_printable("failed to get body fragment")?;
                let body = body.access().change_context(DataStreamError)?;

                *field_fragments_sent
                    .entry(fragment_name.clone())
                    .or_default() += filter_match.len();

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
                *field_fragments_size_bytes.entry(fragment_name).or_default() += fragment_size;
            }

            if !data_buffer.is_empty() {
                has_data = true;
            }

            field_blocks_size_bytes += data_buffer.len();
            output.push(data_buffer.freeze());
        }

        current_span.record("blocks_count", output.len());
        current_span.record("blocks_size_bytes", field_blocks_size_bytes);
        current_span.record("fragments_count", field_fragments_sent.as_value());
        current_span.record(
            "fragments_size_bytes",
            field_fragments_size_bytes.as_value(),
        );

        Ok(has_data)
    }
}

impl error_stack::Context for DataStreamError {}

impl std::fmt::Display for DataStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "data stream error")
    }
}
