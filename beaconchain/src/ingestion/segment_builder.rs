use apibara_dna_common::{
    core::Cursor,
    ingestion::SegmentBuilder,
    segment::store::BlockData,
    storage::{block_prefix, segment_prefix, LocalStorageBackend, StorageBackend, BLOCK_NAME},
};
use error_stack::{Result, ResultExt};
use tokio::io::AsyncWriteExt;

use crate::segment::{
    store, SegmentGroupIndex, BLOB_SEGMENT_NAME, HEADER_SEGMENT_NAME, TRANSACTION_SEGMENT_NAME,
    VALIDATOR_SEGMENT_NAME,
};

#[derive(Debug)]
pub struct BeaconChainSegmentBuilderError;

pub struct BeaconChainSegmentBuilder {
    storage: LocalStorageBackend,
    headers: store::BlockHeaderSegment,
    transactions: store::TransactionSegment,
    validators: store::ValidatorSegment,
    blobs: store::BlobSegment,
    segment_group_index: SegmentGroupIndex,
}

impl BeaconChainSegmentBuilder {
    pub fn new(storage: LocalStorageBackend) -> Self {
        Self {
            storage,
            headers: Default::default(),
            transactions: Default::default(),
            validators: Default::default(),
            blobs: Default::default(),
            segment_group_index: Default::default(),
        }
    }

    fn add_single_block(&mut self, cursor: &Cursor, block: store::Slot<store::SingleBlock>) {
        match block {
            store::Slot::Missed => self.add_missed_block(cursor),
            store::Slot::Proposed(block) => self.add_proposed_block(cursor, block),
        }
    }

    fn add_missed_block(&mut self, _cursor: &Cursor) {
        use store::Slot::Missed;

        self.headers.blocks.push(Missed);
        self.transactions.blocks.push(Missed);
        self.validators.blocks.push(Missed);
        self.blobs.blocks.push(Missed);
    }

    fn add_proposed_block(&mut self, cursor: &Cursor, block: store::SingleBlock) {
        use store::Slot::Proposed;

        self.update_index(cursor, &block);

        let block_number = cursor.number;

        self.headers.blocks.push(Proposed(block.header));
        self.transactions.blocks.push(Proposed(BlockData {
            block_number,
            data: block.transactions,
        }));
        self.validators.blocks.push(Proposed(BlockData {
            block_number,
            data: block.validators,
        }));
        self.blobs.blocks.push(Proposed(BlockData {
            block_number,
            data: block.blobs,
        }));
    }

    fn update_index(&mut self, cursor: &Cursor, block: &store::SingleBlock) {
        let block_number = cursor.number as u32;

        // Contract (from | to) to block number.
        for transaction in &block.transactions {
            self.segment_group_index
                .transaction_by_from_address
                .entry(transaction.from)
                .or_default()
                .insert(block_number);

            if let Some(to) = transaction.to {
                self.segment_group_index
                    .transaction_by_to_address
                    .entry(to)
                    .or_default()
                    .insert(block_number);
            }
        }
    }
}

#[async_trait::async_trait]
impl SegmentBuilder for BeaconChainSegmentBuilder {
    type Error = BeaconChainSegmentBuilderError;

    fn create_segment(&mut self, cursors: &[Cursor]) -> Result<(), Self::Error> {
        for cursor in cursors {
            let bytes = self
                .storage
                .mmap(block_prefix(cursor), BLOCK_NAME)
                .change_context(BeaconChainSegmentBuilderError)
                .attach_printable("failed to mmap block")
                .attach_printable_lazy(|| format!("cursor: {cursor}"))?;
            // Completely deserialize the single block since we're going to access-copy
            // all of its fields.
            let block = rkyv::from_bytes::<store::Slot<store::SingleBlock>>(&bytes)
                .map_err(|_| BeaconChainSegmentBuilderError)
                .attach_printable("failed to deserialize block")
                .attach_printable_lazy(|| format!("cursor: {cursor}"))?;

            self.add_single_block(cursor, block);
        }

        Ok(())
    }

    async fn write_segment<S>(
        &mut self,
        segment_name: &str,
        storage: &mut S,
    ) -> Result<(), Self::Error>
    where
        S: StorageBackend + Send,
        <S as StorageBackend>::Writer: Send,
    {
        let bytes =
            rkyv::to_bytes::<_, 0>(&self.headers).change_context(BeaconChainSegmentBuilderError)?;
        write_bytes(storage, segment_name, HEADER_SEGMENT_NAME, &bytes).await?;

        let bytes = rkyv::to_bytes::<_, 0>(&self.transactions)
            .change_context(BeaconChainSegmentBuilderError)?;
        write_bytes(storage, segment_name, TRANSACTION_SEGMENT_NAME, &bytes).await?;

        let bytes = rkyv::to_bytes::<_, 0>(&self.validators)
            .change_context(BeaconChainSegmentBuilderError)?;
        write_bytes(storage, segment_name, VALIDATOR_SEGMENT_NAME, &bytes).await?;

        let bytes =
            rkyv::to_bytes::<_, 0>(&self.blobs).change_context(BeaconChainSegmentBuilderError)?;
        write_bytes(storage, segment_name, BLOB_SEGMENT_NAME, &bytes).await?;

        // Reset segment data for the next segment.
        self.headers.reset();
        self.transactions.reset();
        self.validators.reset();
        self.blobs.reset();

        Ok(())
    }

    async fn write_segment_group<S>(
        &mut self,
        segment_group_name: &str,
        storage: &mut S,
    ) -> Result<(), Self::Error>
    where
        S: StorageBackend + Send,
        <S as StorageBackend>::Writer: Send,
    {
        let index = std::mem::take(&mut self.segment_group_index);
        let index = store::SegmentGroupIndex::try_from(index)
            .change_context(BeaconChainSegmentBuilderError)
            .attach_printable("failed to convert index")?;
        let segment_group = store::SegmentGroup { index };

        let bytes = rkyv::to_bytes::<_, 0>(&segment_group)
            .change_context(BeaconChainSegmentBuilderError)?;

        write_bytes(storage, "group", segment_group_name, &bytes).await?;

        Ok(())
    }

    async fn cleanup_segment_data(&mut self, cursors: &[Cursor]) -> Result<(), Self::Error> {
        for cursor in cursors {
            self.storage
                .remove_prefix(block_prefix(cursor))
                .await
                .change_context(BeaconChainSegmentBuilderError)
                .attach_printable("failed to delete cursor")
                .attach_printable_lazy(|| format!("cursor: {cursor}"))?;
        }

        Ok(())
    }
}

async fn write_bytes<S>(
    storage: &mut S,
    segment_name: &str,
    filename: &str,
    data: &[u8],
) -> Result<(), BeaconChainSegmentBuilderError>
where
    S: StorageBackend + Send,
    <S as StorageBackend>::Writer: Send,
{
    let mut writer = storage
        .put(segment_prefix(segment_name), filename)
        .await
        .change_context(BeaconChainSegmentBuilderError)?;
    writer
        .write_all(data)
        .await
        .change_context(BeaconChainSegmentBuilderError)
        .attach_printable("failed to write segment")
        .attach_printable_lazy(|| format!("prefix: {segment_name}, filename: {filename}"))?;
    writer
        .shutdown()
        .await
        .change_context(BeaconChainSegmentBuilderError)?;

    Ok(())
}

impl error_stack::Context for BeaconChainSegmentBuilderError {}

impl std::fmt::Display for BeaconChainSegmentBuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to build segment")
    }
}
