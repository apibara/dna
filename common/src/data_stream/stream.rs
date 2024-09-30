use std::{
    collections::{BTreeMap, HashMap},
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

use crate::{
    block_store::BlockStoreReader,
    chain_view::{CanonicalCursor, ChainView, NextCursor},
    data_stream::{FilterMatch, FragmentAccess},
    fragment::{FragmentId, HEADER_FRAGMENT_ID},
    query::BlockFilter,
    segment::SegmentGroup,
    Cursor,
};

#[derive(Debug)]
pub struct DataStreamError;

pub struct DataStream {
    block_filter: Vec<BlockFilter>,
    current: Option<Cursor>,
    finalized: Cursor,
    _finality: DataFinality,
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
            _finality: finality,
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
                        self.finalized = self.chain_view.get_finalized_cursor().await.change_context(DataStreamError)?;
                        return self.send_finalize_message(tx, ct).await;
                    },
                }
            }
        };

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

        {
            let group_bytes = self
                .store
                .get_group(&group_start_cursor)
                .await
                .change_context(DataStreamError)
                .attach_printable("failed to get group")?;
            let group =
                rkyv::access::<rkyv::Archived<SegmentGroup>, rkyv::rancor::Error>(&group_bytes)
                    .change_context(DataStreamError)
                    .attach_printable("failed to access group")?;

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

                    let indexes =
                        rkyv::deserialize::<_, rkyv::rancor::Error>(&group.index.indexes[pos])
                            .change_context(DataStreamError)
                            .attach_printable("failed to deserialize index")?;

                    for filter in filters {
                        let rows = filter.filter(&indexes).change_context(DataStreamError)?;
                        data_bitmap |= &rows;
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
            let block_number = block_number as u64;

            if block_number > current_segment_end {
                let blocks = std::mem::take(&mut current_segment_data);
                let current_segment_cursor = Cursor::new_finalized(current_segment_start);

                // TODO: prefetch segments.
                // self.scanner
                //     .prefetch_segment(&mut prefetch_tasks, current_segment_cursor.clone())
                //     .change_context(DataStreamError)?;

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

        // TODO: prefetch segments.
        // self.scanner
        //     .prefetch_segment(&mut prefetch_tasks, current_segment_cursor.clone())
        //     .change_context(DataStreamError)?;

        segments.push((current_segment_cursor, blocks));

        // prefetch_tasks.join_all().await;

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
                if self.filter_fragment(&fragment_access, &mut blocks).await? {
                    let data = Message::Data(Data {
                        cursor: proto_cursor.clone(),
                        end_cursor: proto_end_cursor.clone(),
                        data: blocks,
                        finality: DataFinality::Finalized as i32,
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
            if self.filter_fragment(&fragment_access, &mut blocks).await? {
                let data = Message::Data(Data {
                    cursor: proto_cursor.clone(),
                    end_cursor: proto_end_cursor.clone(),
                    data: blocks,
                    finality: DataFinality::Finalized as i32,
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

        let finality = if finalized.strict_after(&cursor) {
            DataFinality::Finalized
        } else {
            DataFinality::Accepted
        };

        let fragment_access = FragmentAccess::new_in_block(self.store.clone(), cursor.clone());

        let mut blocks = Vec::new();
        if self.filter_fragment(&fragment_access, &mut blocks).await? {
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

    async fn filter_fragment(
        &mut self,
        fragment_access: &FragmentAccess,
        output: &mut Vec<Bytes>,
    ) -> Result<bool, DataStreamError> {
        let mut has_data = false;

        for block_filter in self.block_filter.iter() {
            let mut data_buffer = BytesMut::with_capacity(DEFAULT_BLOCKS_BUFFER_SIZE);
            let mut fragment_matches = BTreeMap::default();

            for (fragment_id, filters) in block_filter.iter() {
                let mut filter_match = FilterMatch::default();

                let indexes = fragment_access
                    .get_fragment_indexes(*fragment_id)
                    .await
                    .change_context(DataStreamError)
                    .attach_printable("failed to get fragment indexes")?;

                for filter in filters {
                    let rows = filter.filter(&indexes).change_context(DataStreamError)?;
                    filter_match.add_match(filter.filter_id, &rows);
                }

                if filter_match.is_empty() {
                    continue;
                }

                fragment_matches.insert(*fragment_id, filter_match);
            }

            if block_filter.always_include_header || !fragment_matches.is_empty() {
                let header = fragment_access
                    .get_header_fragment()
                    .await
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
                    .get_body_fragment(fragment_id, fragment_name)
                    .await
                    .change_context(DataStreamError)
                    .attach_printable("failed to get body fragment")?;

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
            }

            if !data_buffer.is_empty() {
                has_data = true;
            }

            output.push(data_buffer.freeze());
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
