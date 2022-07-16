//! Service that tracks the chain head.
use anyhow::{Context, Error, Result};
use async_recursion::async_recursion;
use futures::StreamExt;
use std::{collections::VecDeque, sync::Arc};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, trace};

use crate::chain::{BlockHeader, ChainProvider};

const REORG_BUFFER_SIZE: usize = 16;
const CHANNEL_BUFFER_SIZE: usize = 64;

/// Message sent by the head tracker.
#[derive(Debug, Clone)]
pub enum Message {
    /// New block produced. This is now the head.
    NewBlock(BlockHeader),
    /// Chain reorganization. The specified block is the new head.
    Reorg(BlockHeader),
}

pub async fn start_head_tracker(
    provider: Arc<dyn ChainProvider>,
) -> Result<(JoinHandle<Result<()>>, ReceiverStream<Message>)> {
    let (tx, rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);

    let tracker = HeadTracker::new(provider, tx);
    let join_handle = tokio::spawn(tracker.run_loop());

    let stream = ReceiverStream::new(rx);

    Ok((join_handle, stream))
}

pub struct HeadTracker {
    // rpc provider used to fetch/subscribe to blocks.
    provider: Arc<dyn ChainProvider>,
    // target size of `prev_block_buffer`.
    reorg_buffer_size: usize,
    // buffer of blocks in the current chain.
    prev_block_buffer: VecDeque<BlockHeader>,
    // buffer of blocks in the new chain. Used during reorgs.
    next_block_buffer: VecDeque<BlockHeader>,
    // sender for new blocks messages.
    tx: mpsc::Sender<Message>,
}

impl HeadTracker {
    pub fn new(provider: Arc<dyn ChainProvider>, tx: mpsc::Sender<Message>) -> Self {
        HeadTracker {
            provider,
            tx,
            reorg_buffer_size: REORG_BUFFER_SIZE,
            prev_block_buffer: VecDeque::new(),
            next_block_buffer: VecDeque::new(),
        }
    }

    async fn run_loop(mut self) -> Result<()> {
        let provider = self.provider.clone();
        let mut block_stream = provider.subscribe_blocks().await?;

        self.build_chain_reorg_buffer().await?;

        // send first block
        match self.prev_block_buffer.front() {
            None => return Err(Error::msg("head tracker buffer is empty")),
            Some(block) => {
                self.tx.send(Message::NewBlock(block.clone())).await?;
            }
        }

        while let Some(new_head) = block_stream.next().await {
            debug!(
                "received new head {} {} (p: {:?})",
                new_head.number, new_head.hash, new_head.parent_hash
            );

            self.apply_new_head(new_head).await?;

            // send blocks added to buffer while handling chain reorganization.
            while let Some(block) = self.next_block_buffer.pop_front() {
                self.tx.send(Message::NewBlock(block)).await?;
            }

            // only empty buffer every now and then
            while self.prev_block_buffer.len() > 2 * self.reorg_buffer_size {
                self.prev_block_buffer.pop_back();
            }
        }

        debug!("block stream closed");

        Ok(())
    }

    async fn build_chain_reorg_buffer(&mut self) -> Result<()> {
        trace!("building chain reorg block buffer");
        let current_head = self.provider.get_head_block().await?;
        debug!("fetched current head: {:?}", current_head);

        let mut next_hash = current_head.parent_hash.clone();
        let mut block_buffer = VecDeque::new();
        block_buffer.push_back(current_head);

        while block_buffer.len() < self.reorg_buffer_size {
            match next_hash {
                None => {
                    // genesis block
                    break;
                }
                Some(ref hash) => {
                    let block = self
                        .provider
                        .get_block_by_hash(hash)
                        .await?
                        .ok_or_else(|| {
                            Error::msg(format!("expected block with hash {} to exist", hash))
                        })?;
                    next_hash = block.parent_hash.clone();

                    // push to the _back_ of the queue because the loop is walking the
                    // chain backwards.
                    block_buffer.push_back(block);
                }
            }
        }

        self.prev_block_buffer = block_buffer;
        Ok(())
    }

    #[async_recursion]
    async fn apply_new_head(&mut self, new_head: BlockHeader) -> Result<()> {
        let prev_head = match self.prev_block_buffer.front() {
            None => return Err(Error::msg("head tracker buffer is empty")),
            Some(prev_head) if prev_head.hash == new_head.hash => {
                debug!("skip: block already seen");
                // stream sent the same block twice. this should not happen but it does
                return Ok(());
            }
            Some(prev_head) => prev_head.clone(),
        };

        let head_number_diff = (new_head.number as i64) - (prev_head.number as i64);

        if head_number_diff == 1 {
            let is_parent = new_head
                .parent_hash
                .clone()
                .map(|hash| hash == prev_head.hash)
                .unwrap_or(false);

            if is_parent {
                debug!("clean head application: {:?}", new_head);
                self.prev_block_buffer.push_front(new_head.clone());
                // send new head
                self.tx.send(Message::NewBlock(new_head)).await?;
                return Ok(());
            }

            // there's a chain reorganization and the old head is not part of
            // the new chain. drop it and restart reorg-detection.
            self.prev_block_buffer.pop_front();
            return self.apply_new_head(new_head).await;
        }

        // in this case it needs to reconcile two chains that are not cleanly connected.
        // there is a `new_head` chain, and a `prev_head` chain.
        // the `new_head` chain can be extended by fetching blocks going back in time,
        // while the `prev_head` chain can be shortened by dropping its head.
        //
        // now we extend one chain and shorten the other until they can be joined with a
        // clean head application.
        if head_number_diff > 1 {
            // block stream somehow missed sending the head tracker some blocks
            // need to backfill missing blocks and try to apply them as well.
            let new_head_parent_hash = new_head
                .parent_hash
                .clone()
                .ok_or_else(|| Error::msg("expected new head parent hash"))?;
            let prev_new_head = self
                .provider
                .get_block_by_hash(&new_head_parent_hash)
                .await
                .context("failed to fetch new head parent block")?
                .ok_or_else(|| Error::msg("expected new head parent block to exist"))?;

            self.next_block_buffer.push_front(new_head);

            // try to apply the new head to the previous chain.
            return self.apply_new_head(prev_new_head).await;
        } else {
            // chain rolled back. delete all extra blocks and try to apply head.
            debug!("rollback current chain to {:?}", new_head);
            while let Some(block) = self.prev_block_buffer.pop_front() {
                if block.number <= new_head.number {
                    break;
                }
            }
            return self.apply_new_head(new_head).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert_matches::assert_matches;
    use chrono::NaiveDateTime;
    use futures::StreamExt;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    use super::{start_head_tracker, Message};
    use crate::chain::{provider::MockChainProvider, BlockHash, BlockHeader};

    // `stream` represent alternative chains.
    fn make_block_header(number: u8, stream: u8, parent_stream: u8) -> BlockHeader {
        let hash = BlockHash::from_bytes(&[stream, number]);
        let parent_hash = BlockHash::from_bytes(&[parent_stream, number - 1]);
        let timestamp = NaiveDateTime::from_timestamp(1640995200 + (number as i64) * 3600, 0);

        BlockHeader {
            hash,
            parent_hash: Some(parent_hash),
            number: number as u64,
            timestamp,
        }
    }

    #[tokio::test]
    pub async fn test_clean_head_application() {
        let mut provider = MockChainProvider::new();

        // current head is at 100.
        provider
            .expect_get_head_block()
            .return_once(|| Ok(make_block_header(100, 0, 0)));

        provider.expect_get_block_by_hash().returning(|hash| {
            let mut bytes = [0u8; 1];
            // original number was a u8.
            bytes.copy_from_slice(&hash.as_bytes()[1..2]);
            let number = u8::from_be_bytes(bytes);
            Ok(Some(make_block_header(number, 0, 0)))
        });

        let (tx, rx) = mpsc::channel(100);

        provider
            .expect_subscribe_blocks()
            .return_once(|| Ok(Box::pin(ReceiverStream::new(rx))));

        let (handle, mut stream) = start_head_tracker(Arc::new(provider)).await.unwrap();

        let message = stream.next().await.unwrap();
        let head_hash = BlockHash::from_bytes(&[0, 100]);
        assert_matches!(message, Message::NewBlock(BlockHeader { hash, .. }) if hash == head_hash);

        let new_head = make_block_header(101, 0, 0);
        tx.send(new_head).await.unwrap();

        let message = stream.next().await.unwrap();
        let head_hash = BlockHash::from_bytes(&[0, 101]);
        assert_matches!(message, Message::NewBlock(BlockHeader { hash, .. }) if hash == head_hash);

        drop(handle);
    }

    #[tokio::test]
    pub async fn test_reorg_with_jump_ahead() {
        // This test simulates a chain reorg and at the same time missing some blocks.
        //
        // 99  100  101  102
        //  o---o                   Chain 0
        //    \-x----x----x         Chain 1
        //
        // head order: o99 o100 x102
        // so the service needs to fill in x101 and x100.
        let mut provider = MockChainProvider::new();

        // current head is at 100.
        provider
            .expect_get_head_block()
            .return_once(|| Ok(make_block_header(100, 0, 0)));

        provider.expect_get_block_by_hash().returning(|hash| {
            let number = hash.as_bytes()[1];
            let stream = if number < 100 { 0 } else { hash.as_bytes()[0] };
            let parent_stream = if number <= 100 { 0 } else { stream };
            Ok(Some(make_block_header(number, stream, parent_stream)))
        });

        let (tx, rx) = mpsc::channel(100);

        provider
            .expect_subscribe_blocks()
            .return_once(|| Ok(Box::pin(ReceiverStream::new(rx))));

        let (handle, mut stream) = start_head_tracker(Arc::new(provider)).await.unwrap();

        let message = stream.next().await.unwrap();
        let head_hash = BlockHash::from_bytes(&[0, 100]);
        assert_matches!(message, Message::NewBlock(BlockHeader { hash, .. }) if hash == head_hash);

        let new_head = make_block_header(102, 1, 1);
        tx.send(new_head).await.unwrap();

        // new head on alternate chain
        let message = stream.next().await.unwrap();
        let head_hash = BlockHash::from_bytes(&[1, 100]);
        assert_matches!(message, Message::NewBlock(BlockHeader { hash, .. }) if hash == head_hash);

        // catchup back to current head
        let message = stream.next().await.unwrap();
        let head_hash = BlockHash::from_bytes(&[1, 101]);
        assert_matches!(message, Message::NewBlock(BlockHeader { hash, .. }) if hash == head_hash);
        let message = stream.next().await.unwrap();
        let head_hash = BlockHash::from_bytes(&[1, 102]);
        assert_matches!(message, Message::NewBlock(BlockHeader { hash, .. }) if hash == head_hash);

        drop(handle);
    }

    #[tokio::test]
    pub async fn test_reorg_going_backward() {
        // This test simulates a chain reorg that removes some blocks on the head.
        //
        // 99  100  101  102
        //  o---o----o----o         Chain 0
        //    \-x                   Chain 1
        //
        // head order: o99 o110 o101 o012 x100
        let mut provider = MockChainProvider::new();

        // current head is at 102.
        provider
            .expect_get_head_block()
            .return_once(|| Ok(make_block_header(102, 0, 0)));

        provider.expect_get_block_by_hash().returning(|hash| {
            let number = hash.as_bytes()[1];
            Ok(Some(make_block_header(number, 0, 0)))
        });

        let (tx, rx) = mpsc::channel(100);

        provider
            .expect_subscribe_blocks()
            .return_once(|| Ok(Box::pin(ReceiverStream::new(rx))));

        let (handle, mut stream) = start_head_tracker(Arc::new(provider)).await.unwrap();

        let message = stream.next().await.unwrap();
        let head_hash = BlockHash::from_bytes(&[0, 102]);
        assert_matches!(message, Message::NewBlock(BlockHeader { hash, .. }) if hash == head_hash);

        let new_head = make_block_header(100, 1, 0);
        tx.send(new_head).await.unwrap();

        let message = stream.next().await.unwrap();
        let head_hash = BlockHash::from_bytes(&[1, 100]);
        assert_matches!(message, Message::NewBlock(BlockHeader { hash, .. }) if hash == head_hash);

        drop(handle);
    }

    #[tokio::test]
    pub async fn test_complex_reorg() {
        // This test simulates a realistic chain reorg.
        //
        // 99  100  101  102
        //  o---o----o----o         Chain 0
        //    \-x----x              Chain 1
        //
        // head order: o99 o100 o101 x100 x101 o102
        let mut provider = MockChainProvider::new();

        // current head is at 100.
        provider
            .expect_get_head_block()
            .return_once(|| Ok(make_block_header(100, 0, 0)));

        provider.expect_get_block_by_hash().returning(|hash| {
            // method called only for "missing" blocks o100 o101 after head is x101.
            // no stream is always 0.
            let number = hash.as_bytes()[1];
            let stream = hash.as_bytes()[0];
            assert!(stream == 0);
            Ok(Some(make_block_header(number, 0, 0)))
        });

        let (tx, rx) = mpsc::channel(100);

        provider
            .expect_subscribe_blocks()
            .return_once(|| Ok(Box::pin(ReceiverStream::new(rx))));

        let (handle, mut stream) = start_head_tracker(Arc::new(provider)).await.unwrap();

        let message = stream.next().await.unwrap();
        let head_hash = BlockHash::from_bytes(&[0, 100]);
        assert_matches!(message, Message::NewBlock(BlockHeader { hash, .. }) if hash == head_hash);

        // o101
        let new_head = make_block_header(101, 0, 0);
        tx.send(new_head).await.unwrap();
        // x100
        let new_head = make_block_header(100, 1, 0);
        tx.send(new_head).await.unwrap();
        // x101
        let new_head = make_block_header(101, 1, 1);
        tx.send(new_head).await.unwrap();
        // o102
        let new_head = make_block_header(102, 0, 0);
        tx.send(new_head).await.unwrap();

        // receive o101
        let message = stream.next().await.unwrap();
        let head_hash = BlockHash::from_bytes(&[0, 101]);
        assert_matches!(message, Message::NewBlock(BlockHeader { hash, .. }) if hash == head_hash);

        // receive x100
        let message = stream.next().await.unwrap();
        let head_hash = BlockHash::from_bytes(&[1, 100]);
        assert_matches!(message, Message::NewBlock(BlockHeader { hash, .. }) if hash == head_hash);

        // receive x101
        let message = stream.next().await.unwrap();
        let head_hash = BlockHash::from_bytes(&[1, 101]);
        assert_matches!(message, Message::NewBlock(BlockHeader { hash, .. }) if hash == head_hash);

        // receive o100, o101, o102
        let message = stream.next().await.unwrap();
        let head_hash = BlockHash::from_bytes(&[0, 100]);
        assert_matches!(message, Message::NewBlock(BlockHeader { hash, .. }) if hash == head_hash);

        let message = stream.next().await.unwrap();
        let head_hash = BlockHash::from_bytes(&[0, 101]);
        assert_matches!(message, Message::NewBlock(BlockHeader { hash, .. }) if hash == head_hash);

        let message = stream.next().await.unwrap();
        let head_hash = BlockHash::from_bytes(&[0, 102]);
        assert_matches!(message, Message::NewBlock(BlockHeader { hash, .. }) if hash == head_hash);

        drop(handle);
    }
}
