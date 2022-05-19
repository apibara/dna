use async_trait::async_trait;
use super::error::Result;
use chrono::{NaiveDateTime};

/// Block header information needed to track information about the chain head.
#[derive(Debug)]
pub struct BlockHeader<BlockHash> {
    pub hash: BlockHash,
    pub parent_hash: Option<BlockHash>,
    pub number: u64,
    pub timestamp: NaiveDateTime,
}

pub enum BlockStreamMessage<BlockHash> {
    NewBlock(BlockHeader<BlockHash>),
    Rollback(u64),
}

pub type BlockStream<BlockHash> = dyn futures::Stream<Item = BlockStreamMessage<BlockHash>>;

#[async_trait]
pub trait BlockHeaderProvider {
    type BlockHash;

    async fn get_head_block(&self) -> Result<BlockHeader<Self::BlockHash>>;
    async fn get_block_by_hash(&self, hash: &Self::BlockHash) -> Result<Option<BlockHeader<Self::BlockHash>>>;
    async fn get_block_by_number(&self, number: u64) -> Result<Option<BlockHeader<Self::BlockHash>>>;
}

pub struct InMemoryBlockStream();

/*/
impl<BlockHash> futures::Stream for InMemoryBlockStream {
    type Item = BlockStreamMessage<BlockHash>;

    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        todo!()
    }
}
*/