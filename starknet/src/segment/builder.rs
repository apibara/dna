use apibara_dna_common::{ingestion::SegmentData, segment::store::BlockData};
use error_stack::{Result, ResultExt};

use crate::ingestion::StarknetSegmentBuilderError;

use super::{
    index::Index, store, EVENT_SEGMENT_NAME, HEADER_SEGMENT_NAME, MESSAGE_SEGMENT_NAME,
    TRANSACTION_RECEIPT_SEGMENT_NAME, TRANSACTION_SEGMENT_NAME,
};

#[derive(Default)]
pub struct SegmentBuilder {
    header: store::BlockHeaderSegment,
    transactions: store::TransactionSegment,
    receipts: store::TransactionReceiptSegment,
    events: store::EventSegment,
    messages: store::MessageSegment,
    index: Index,
}

impl SegmentBuilder {
    pub fn reset(&mut self) {
        self.header.reset();
        self.transactions.reset();
        self.receipts.reset();
        self.events.reset();
        self.messages.reset();

        self.index = Index::default();
    }

    pub fn size(&self) -> usize {
        self.header.blocks.len()
    }

    pub fn add_single_block(&mut self, block: store::SingleBlock) {
        self.update_index(&block);

        let block_number = block.header.block_number;

        self.header.blocks.push(block.header);

        {
            let data = BlockData {
                block_number,
                data: block.transactions,
            };
            self.transactions.blocks.push(data);
        }
        {
            let data = BlockData {
                block_number,
                data: block.receipts,
            };
            self.receipts.blocks.push(data);
        }
        {
            let data = BlockData {
                block_number,
                data: block.events,
            };
            self.events.blocks.push(data);
        }
        {
            let data = BlockData {
                block_number,
                data: block.messages,
            };
            self.messages.blocks.push(data);
        }
    }

    pub fn take_segment_data(
        &mut self,
    ) -> Result<(Index, Vec<SegmentData>), StarknetSegmentBuilderError> {
        let index = std::mem::take(&mut self.index);
        let header = {
            let bytes =
                rkyv::to_bytes::<_, 0>(&self.header).change_context(StarknetSegmentBuilderError)?;
            SegmentData {
                filename: HEADER_SEGMENT_NAME.to_string(),
                data: bytes.to_vec(),
            }
        };

        let transaction = {
            let bytes = rkyv::to_bytes::<_, 0>(&self.transactions)
                .change_context(StarknetSegmentBuilderError)?;
            SegmentData {
                filename: TRANSACTION_SEGMENT_NAME.to_string(),
                data: bytes.to_vec(),
            }
        };

        let receipt = {
            let bytes = rkyv::to_bytes::<_, 0>(&self.receipts)
                .change_context(StarknetSegmentBuilderError)?;
            SegmentData {
                filename: TRANSACTION_RECEIPT_SEGMENT_NAME.to_string(),
                data: bytes.to_vec(),
            }
        };

        let event = {
            let bytes =
                rkyv::to_bytes::<_, 0>(&self.events).change_context(StarknetSegmentBuilderError)?;
            SegmentData {
                filename: EVENT_SEGMENT_NAME.to_string(),
                data: bytes.to_vec(),
            }
        };

        let message = {
            let bytes = rkyv::to_bytes::<_, 0>(&self.messages)
                .change_context(StarknetSegmentBuilderError)?;
            SegmentData {
                filename: MESSAGE_SEGMENT_NAME.to_string(),
                data: bytes.to_vec(),
            }
        };

        self.reset();

        Ok((index, vec![header, transaction, receipt, event, message]))
    }

    fn update_index(&mut self, block: &store::SingleBlock) {
        let block_number = block.header.block_number;
        for event in &block.events {
            self.index
                .event_by_address
                .entry(event.from_address)
                .or_default()
                .insert(block_number as u32);

            if let Some(key) = event.keys.first() {
                self.index
                    .event_by_key_0
                    .entry(*key)
                    .or_default()
                    .insert(block_number as u32);
            }
        }
    }
}
