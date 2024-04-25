mod segment;
mod segment_group;
mod single;

use apibara_dna_common::error::Result;
use apibara_dna_common::segment::SegmentOptions;
use apibara_dna_common::storage::StorageBackend;

pub use self::segment::{
    BlockHeaderSegmentReader, LogSegmentReader, ReceiptSegmentReader, TransactionSegmentReader,
};
pub use self::segment_group::SegmentGroupReader;
pub use self::single::SingleBlockReader;

use super::store::{BlockHeaderSegment, LogSegment, ReceiptSegment, TransactionSegment};

const DEFAULT_BUFFER_SIZE: usize = 1024 * 1024 * 1024;

#[derive(Debug, Copy, Clone)]
pub struct BlockSegmentReaderOptions {
    pub needs_logs: bool,
    pub needs_receipts: bool,
    pub needs_transactions: bool,
}

pub struct BlockSegmentReader<S>
where
    S: StorageBackend + Clone,
{
    header: BlockHeaderSegmentReader<S>,
    logs: Option<LogSegmentReader<S>>,
    receipts: Option<ReceiptSegmentReader<S>>,
    transactions: Option<TransactionSegmentReader<S>>,
}

pub struct BlockSegment<'a> {
    pub header: BlockHeaderSegment<'a>,
    pub logs: Option<LogSegment<'a>>,
    pub receipts: Option<ReceiptSegment<'a>>,
    pub transactions: Option<TransactionSegment<'a>>,
}

impl<S> BlockSegmentReader<S>
where
    S: StorageBackend + Clone,
    <S as StorageBackend>::Reader: Unpin,
{
    pub fn new(
        storage: S,
        segment_options: SegmentOptions,
        options: BlockSegmentReaderOptions,
    ) -> Self {
        let header = BlockHeaderSegmentReader::new(
            storage.clone(),
            segment_options.clone(),
            DEFAULT_BUFFER_SIZE,
        );

        let logs = if options.needs_logs {
            Some(LogSegmentReader::new(
                storage.clone(),
                segment_options.clone(),
                DEFAULT_BUFFER_SIZE,
            ))
        } else {
            None
        };

        let receipts = if options.needs_receipts {
            Some(ReceiptSegmentReader::new(
                storage.clone(),
                segment_options.clone(),
                DEFAULT_BUFFER_SIZE,
            ))
        } else {
            None
        };

        let transactions = if options.needs_transactions {
            Some(TransactionSegmentReader::new(
                storage.clone(),
                segment_options.clone(),
                DEFAULT_BUFFER_SIZE,
            ))
        } else {
            None
        };

        Self {
            header,
            logs,
            receipts,
            transactions,
        }
    }

    pub async fn read<'a>(&'a mut self, segment_start: u64) -> Result<BlockSegment<'a>> {
        let header = self.header.read(segment_start).await?;

        let logs = if let Some(reader) = self.logs.as_mut() {
            let segment = reader.read(segment_start).await?;
            Some(segment)
        } else {
            None
        };

        let receipts = if let Some(reader) = self.receipts.as_mut() {
            let segment = reader.read(segment_start).await?;
            Some(segment)
        } else {
            None
        };

        let transactions = if let Some(reader) = self.transactions.as_mut() {
            let segment = reader.read(segment_start).await?;
            Some(segment)
        } else {
            None
        };

        Ok(BlockSegment {
            header,
            logs,
            transactions,
            receipts,
        })
    }
}

impl BlockSegmentReaderOptions {
    pub fn merge(&self, other: &Self) -> Self {
        Self {
            needs_logs: self.needs_logs || other.needs_logs,
            needs_receipts: self.needs_receipts || other.needs_receipts,
            needs_transactions: self.needs_transactions || other.needs_transactions,
        }
    }
}

impl Default for BlockSegmentReaderOptions {
    fn default() -> Self {
        Self {
            needs_logs: false,
            needs_receipts: false,
            needs_transactions: false,
        }
    }
}
