use std::collections::{HashMap, HashSet};

use apibara_observability::{KeyValue, RecordRequest};
use error_stack::{Result, ResultExt};
use foyer::FetchState;
use futures::TryStreamExt;
use futures_buffered::FuturesOrderedBounded;
use roaring::RoaringBitmap;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{
    block_store::BlockStoreReader,
    chain_view::ChainView,
    data_stream::SegmentAccessFetch,
    file_cache::FileCacheError,
    fragment::{FragmentId, HEADER_FRAGMENT_ID, INDEX_FRAGMENT_ID, JOIN_FRAGMENT_ID},
    query::BlockFilter,
    segment::SegmentGroup,
    Cursor,
};

use super::{DataStreamError, DataStreamMetrics};

// Production workloads have ~10k blocks per group and size ~100MiB.
// Set the queue size to be small enough to not consume too much memory.
const GROUP_QUEUE_SIZE: usize = 4;

pub struct SegmentStream {
    block_filter: Vec<BlockFilter>,
    fragment_id_to_name: HashMap<FragmentId, String>,
    store: BlockStoreReader,
    chain_view: ChainView,
    metrics: DataStreamMetrics,
}

impl SegmentStream {
    pub fn new(
        block_filter: Vec<BlockFilter>,
        fragment_id_to_name: HashMap<FragmentId, String>,
        store: BlockStoreReader,
        chain_view: ChainView,
        metrics: DataStreamMetrics,
    ) -> Self {
        Self {
            block_filter,
            fragment_id_to_name,
            store,
            chain_view,
            metrics,
        }
    }

    pub async fn start(
        self,
        starting_cursor: Cursor,
        tx: mpsc::Sender<SegmentAccessFetch>,
        ct: CancellationToken,
    ) -> Result<(), DataStreamError> {
        let group_size = self.chain_view.get_group_size().await;
        let segment_size = self.chain_view.get_segment_size().await;

        debug!(starting_cursor = %starting_cursor, "segment_stream: starting");

        let mut current_block_number = starting_cursor.number;

        if let Some(mut grouped) = self
            .chain_view
            .get_grouped_cursor()
            .await
            .change_context(DataStreamError)?
        {
            // Align current block number with the group once at the beginning.
            // It's the job of the consumer to ensure that no block before the
            // starting one is sent.
            current_block_number = self
                .chain_view
                .get_group_start_block(starting_cursor.number)
                .await;

            let mut group_queue = FuturesOrderedBounded::new(GROUP_QUEUE_SIZE);
            let mut next_group_to_fetch = current_block_number;

            while current_block_number <= grouped.number {
                if ct.is_cancelled() {
                    return Ok(());
                }

                debug!(
                    current_block_number = %current_block_number,
                    "segment_stream: fetching block"
                );

                while group_queue.len() < GROUP_QUEUE_SIZE && next_group_to_fetch <= grouped.number
                {
                    debug!(next_group_to_fetch = %next_group_to_fetch, "segment_stream: pushing group future to queue");
                    group_queue.push_back({
                        let store = self.store.clone();
                        let group_download_metrics = self.metrics.group_download.clone();
                        async move {
                            let group_cursor = Cursor::new_finalized(next_group_to_fetch);
                            let entry = store.get_group(&group_cursor);
                            let cache_hit = entry.state() == FetchState::Hit;
                            let entry = entry
                                .record_request(group_download_metrics)
                                .await
                                .map_err(FileCacheError::Foyer)?;
                            Ok::<_, FileCacheError>((group_cursor, entry, cache_hit))
                        }
                    });

                    next_group_to_fetch += group_size * segment_size;
                }

                let expected_group_cursor = Cursor::new_finalized(current_block_number);

                let (fetched_group_cursor, group_entry, cache_hit) = group_queue
                    .try_next()
                    .record_request(self.metrics.group_wait.clone())
                    .await
                    .change_context(DataStreamError)
                    .attach_printable("failed to get group future")
                    .attach_printable_lazy(|| format!("cursor: {current_block_number}"))?
                    .ok_or(DataStreamError)
                    .attach_printable("group queue is empty")?;

                if expected_group_cursor != fetched_group_cursor {
                    return Err(DataStreamError).attach_printable("expected and fetched group cursors do not match")
                        .attach_printable_lazy(|| format!("expected: {expected_group_cursor}, fetched: {fetched_group_cursor}"));
                }

                if cache_hit {
                    self.metrics.group_cache_hit.add(1, &[]);
                }

                let group = unsafe {
                    rkyv::access_unchecked::<rkyv::Archived<SegmentGroup>>(group_entry.value())
                };

                let mut blocks_with_data = RoaringBitmap::default();

                // Some users stream block headers to benchmark the service. Avoid fetching useless index and join fragments.
                let mut fragment_ids_needed = if self.block_filter.is_empty() {
                    HashSet::from([HEADER_FRAGMENT_ID])
                } else {
                    HashSet::from([INDEX_FRAGMENT_ID, JOIN_FRAGMENT_ID, HEADER_FRAGMENT_ID])
                };

                // Use the group indices to compute which blocks have data for the client-provided filters.
                for block_filter in self.block_filter.iter() {
                    // If the client requested all headers include all blocks in the group.
                    if block_filter.always_include_header() {
                        let group_start = current_block_number as u32;
                        let group_end_non_inclusive =
                            (current_block_number + group_size * segment_size) as u32;
                        let group_block_range =
                            RoaringBitmap::from_sorted_iter(group_start..group_end_non_inclusive)
                                .expect("failed to create bitmap from sorted iter");
                        blocks_with_data |= group_block_range;
                    }

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

                            blocks_with_data |= &rows;
                            fragment_ids_needed.insert(*fragment_id);
                            for join_with_fragment_id in filter.joins.iter() {
                                fragment_ids_needed.insert(*join_with_fragment_id);
                            }
                        }
                    }
                }

                // Now start fetching data for all segments/fragments that are needed.
                for segment_offset in 0..group_size {
                    let segment_start = current_block_number + segment_offset * segment_size;
                    let segment_end_non_inclusive = (segment_start + segment_size) as u32;
                    let mut segment_block_range = RoaringBitmap::from_sorted_iter(
                        (segment_start as u32)..segment_end_non_inclusive,
                    )
                    .expect("failed to create bitmap from sorted iter");
                    segment_block_range &= &blocks_with_data;

                    if segment_block_range.is_empty() {
                        continue;
                    }

                    debug!(segment_start, "segment_stream: fetching segment");

                    let mut segment_fetch =
                        SegmentAccessFetch::new(segment_start, segment_block_range);

                    let segment_cursor = Cursor::new_finalized(segment_start);

                    for fragment_id in fragment_ids_needed.iter() {
                        let segment_name = self
                            .fragment_id_to_name
                            .get(fragment_id)
                            .ok_or(DataStreamError)
                            .attach_printable("expected fragment id to have a name")
                            .attach_printable_lazy(|| format!("fragment_id: {fragment_id}"))?;
                        let fragment_fetch = self
                            .store
                            .get_segment(&segment_cursor, segment_name)
                            .record_request_with_attributes(
                                self.metrics.segment_download.clone(),
                                &[KeyValue::new("name", segment_name.clone())],
                            );
                        segment_fetch.insert_fragment(*fragment_id, fragment_fetch);
                    }

                    let Ok(_) = tx.send(segment_fetch).await else {
                        return Ok(());
                    };
                }

                current_block_number += segment_size * group_size;

                // Since checking the grouped cursor locks the state, do it if it's really needed.
                if grouped.number == current_block_number {
                    let Some(new_grouped) = self
                        .chain_view
                        .get_grouped_cursor()
                        .await
                        .change_context(DataStreamError)?
                    else {
                        return Ok(());
                    };

                    grouped = new_grouped;
                }
            }
        }

        if let Some(segmented) = self
            .chain_view
            .get_segmented_cursor()
            .await
            .change_context(DataStreamError)?
        {
            while current_block_number <= segmented.number {
                if ct.is_cancelled() {
                    return Ok(());
                }

                debug!(
                    block = current_block_number,
                    "segment_stream: fetching segment"
                );

                let segment_start = current_block_number as u32;
                let segment_end_non_inclusive = (current_block_number + segment_size) as u32;
                let segment_block_range =
                    RoaringBitmap::from_sorted_iter(segment_start..segment_end_non_inclusive)
                        .expect("failed to create bitmap from sorted iter");

                let mut fragment_ids_needed =
                    HashSet::from([INDEX_FRAGMENT_ID, JOIN_FRAGMENT_ID, HEADER_FRAGMENT_ID]);

                // In this case we just fetch all fragments that may be used by the filter.
                for block_filter in self.block_filter.iter() {
                    fragment_ids_needed.extend(block_filter.all_fragment_ids());
                }

                let mut segment_fetch =
                    SegmentAccessFetch::new(current_block_number, segment_block_range);

                let segment_cursor = Cursor::new_finalized(current_block_number);
                for fragment_id in fragment_ids_needed.iter() {
                    let segment_name = self
                        .fragment_id_to_name
                        .get(fragment_id)
                        .ok_or(DataStreamError)
                        .attach_printable("expected fragment id to have a name")
                        .attach_printable_lazy(|| format!("fragment_id: {fragment_id}"))?;
                    let fragment_fetch = self
                        .store
                        .get_segment(&segment_cursor, segment_name)
                        .record_request_with_attributes(
                            self.metrics.segment_download.clone(),
                            &[KeyValue::new("name", segment_name.clone())],
                        );
                    segment_fetch.insert_fragment(*fragment_id, fragment_fetch);
                }

                let Ok(_) = tx.send(segment_fetch).await else {
                    return Ok(());
                };

                current_block_number += segment_size;
            }
        }

        debug!("segment_stream: done");

        Ok(())
    }
}
