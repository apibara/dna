use apibara_dna_common::{
    error::{DnaError, Result},
    storage::StorageBackend,
};
use error_stack::ResultExt;
use tokio::io::AsyncWriteExt;

use super::store;

#[derive(Default)]
pub struct SegmentBuilder {
    header: store::BlockHeaderSegment,
    transactions: store::TransactionSegment,
    receipts: store::TransactionReceiptSegment,
    events: store::EventSegment,
    messages: store::MessageSegment,
}

impl SegmentBuilder {
    pub fn size(&self) -> usize {
        self.header.blocks.len()
    }

    pub fn add_single_block(&mut self, block: store::SingleBlock) {
        let block_number = block.header.block_number;

        self.header.blocks.push(block.header);

        {
            let data = store::BlockData {
                block_number,
                data: block.transactions,
            };
            self.transactions.blocks.push(data);
        }
        {
            let data = store::BlockData {
                block_number,
                data: block.receipts,
            };
            self.receipts.blocks.push(data);
        }
        {
            let data = store::BlockData {
                block_number,
                data: block.events,
            };
            self.events.blocks.push(data);
        }
        {
            let data = store::BlockData {
                block_number,
                data: block.messages,
            };
            self.messages.blocks.push(data);
        }
    }

    pub async fn write<S: StorageBackend>(
        &mut self,
        segment_name: &str,
        storage: &mut S,
    ) -> Result<()> {
        {
            let mut writer = storage.put(segment_name, "header").await?;
            let bytes = rkyv::to_bytes::<_, 0>(&self.header).change_context(DnaError::Io)?;
            writer
                .write_all(&bytes)
                .await
                .change_context(DnaError::Io)?;
            writer.shutdown().await.change_context(DnaError::Io)?;
        }

        {
            let mut writer = storage.put(segment_name, "transaction").await?;
            let bytes = rkyv::to_bytes::<_, 0>(&self.transactions).change_context(DnaError::Io)?;
            writer
                .write_all(&bytes)
                .await
                .change_context(DnaError::Io)?;
            writer.shutdown().await.change_context(DnaError::Io)?;
        }

        {
            let mut writer = storage.put(segment_name, "receipt").await?;
            let bytes = rkyv::to_bytes::<_, 0>(&self.receipts).change_context(DnaError::Io)?;
            writer
                .write_all(&bytes)
                .await
                .change_context(DnaError::Io)?;
            writer.shutdown().await.change_context(DnaError::Io)?;
        }

        {
            let mut writer = storage.put(segment_name, "events").await?;
            let bytes = rkyv::to_bytes::<_, 0>(&self.events).change_context(DnaError::Io)?;
            writer
                .write_all(&bytes)
                .await
                .change_context(DnaError::Io)?;
            writer.shutdown().await.change_context(DnaError::Io)?;
        }

        {
            let mut writer = storage.put(segment_name, "messages").await?;
            let bytes = rkyv::to_bytes::<_, 0>(&self.messages).change_context(DnaError::Io)?;
            writer
                .write_all(&bytes)
                .await
                .change_context(DnaError::Io)?;
            writer.shutdown().await.change_context(DnaError::Io)?;
        }

        Ok(())
    }
}
