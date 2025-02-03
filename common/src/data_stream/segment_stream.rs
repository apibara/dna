use std::collections::{HashMap, HashSet};

use error_stack::{Result, ResultExt};
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

use super::DataStreamError;

pub struct SegmentStream {
    block_filter: Vec<BlockFilter>,
    fragment_id_to_name: HashMap<FragmentId, String>,
    store: BlockStoreReader,
    chain_view: ChainView,
}

impl SegmentStream {
    pub fn new(
        block_filter: Vec<BlockFilter>,
        fragment_id_to_name: HashMap<FragmentId, String>,
        store: BlockStoreReader,
        chain_view: ChainView,
    ) -> Self {
        Self {
            block_filter,
            fragment_id_to_name,
            store,
            chain_view,
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

        let Some(mut grouped) = self
            .chain_view
            .get_grouped_cursor()
            .await
            .change_context(DataStreamError)?
        else {
            debug!("segment_stream: no grouped block");
            return Ok(());
        };

        // Align current block number with the group once at the beginning.
        // It's the job of the consumer to ensure that no block before the
        // starting one is sent.
        let mut current_block_number = self
            .chain_view
            .get_group_start_block(starting_cursor.number)
            .await;

        while current_block_number <= grouped.number {
            if ct.is_cancelled() {
                return Ok(());
            }

            debug!(
                current_block_number = %current_block_number,
                "segment_stream: fetching block"
            );

            // Yes, we could pre-fetch the group in a previous iteration and
            // just await it here, but for now let's keep it simple since groups
            // should be cached anyway.
            let group_cursor = Cursor::new_finalized(current_block_number);
            let group_entry = self
                .store
                .get_group(&group_cursor)
                .await
                .map_err(FileCacheError::Foyer)
                .change_context(DataStreamError)
                .attach_printable("failed to get group")
                .attach_printable_lazy(|| format!("cursor: {current_block_number}"))?;

            let group = unsafe {
                rkyv::access_unchecked::<rkyv::Archived<SegmentGroup>>(group_entry.value())
            };

            let mut blocks_with_data = RoaringBitmap::default();
            let mut fragment_ids_needed =
                HashSet::from([INDEX_FRAGMENT_ID, JOIN_FRAGMENT_ID, HEADER_FRAGMENT_ID]);

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

                let mut segment_fetch = SegmentAccessFetch::new(segment_start, segment_block_range);

                let segment_cursor = Cursor::new_finalized(segment_start);

                for fragment_id in fragment_ids_needed.iter() {
                    let segment_name = self
                        .fragment_id_to_name
                        .get(fragment_id)
                        .ok_or(DataStreamError)
                        .attach_printable("expected fragment id to have a name")
                        .attach_printable_lazy(|| format!("fragment_id: {fragment_id}"))?;
                    let fragment_fetch = self.store.get_segment(&segment_cursor, segment_name);
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

        let Some(segmented) = self
            .chain_view
            .get_segmented_cursor()
            .await
            .change_context(DataStreamError)?
        else {
            return Ok(());
        };

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
                let fragment_fetch = self.store.get_segment(&segment_cursor, segment_name);
                segment_fetch.insert_fragment(*fragment_id, fragment_fetch);
            }

            let Ok(_) = tx.send(segment_fetch).await else {
                return Ok(());
            };

            current_block_number += segment_size;
        }

        debug!("segment_stream: done");

        Ok(())
    }
}
