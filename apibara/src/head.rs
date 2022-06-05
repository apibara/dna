use anyhow::Result;
use futures::{stream::BoxStream, StreamExt};
use std::sync::{Arc, Mutex};

use crate::chain::{BlockHeader, ChainProvider};

#[derive(Debug)]
pub enum BlockStreamMessage {
    NewBlock(BlockHeader),
    Rollback(BlockHeader),
}

/// Track the current blockchain head, creating
/// messages to signal either a new block, or
/// a chain rollback.
pub struct HeadTracker<P: ChainProvider> {
    provider: Arc<P>,
}

impl<P: ChainProvider> HeadTracker<P> {
    /// Create a new head tracker.
    pub fn new(provider: Arc<P>) -> HeadTracker<P> {
        HeadTracker { provider }
    }

    /// Initialize the head tracker.
    ///
    /// 1. Fetch current head
    /// 2. Fetch `n` most recent blocks
    /// 3. Start streaming blocks
    pub async fn start(&mut self) -> Result<BoxStream<'_, BlockStreamMessage>> {
        let stream = self.provider.subscribe_blocks().await?;
        let current = self.provider.get_head_block().await?;

        let state = Arc::new(Mutex::new(HeadTrackerState { current }));
        let transformed_stream = Box::pin(stream.scan(state, |state, block| {
            let state = state.clone();
            async move {
                // TODO: what to do if cannot get lock?
                let mut state = state.lock().unwrap();

                if state.is_clean_apply(&block) {
                    state.update_current(block.clone());
                    Some(BlockStreamMessage::NewBlock(block))
                } else {
                    // TODO: handle reorg better
                    // this code assumes that node gives us a (new) valid head and
                    // will continue from there.
                    // But that's not always the case, for example we can get
                    // the following stream:
                    // (a, 1) - (b, 2, p: a) - (c, 2, p: a) - (d, 3, p: b)
                    //
                    // notice how node `d` has `b` as parent. The current code
                    // emits the following sequence:
                    //
                    // NB(a) - NB(b) - R(c) - R(d)
                    //
                    // The correct sequence is:
                    //
                    // NB(a) - NB(b) - R(c) - R(b) - NB(d)
                    //
                    // Somehow we need to generate two events in one step.
                    state.update_current(block.clone());
                    Some(BlockStreamMessage::Rollback(block))
                }
            }
        }));
        Ok(transformed_stream)
    }
}

#[derive(Debug)]
struct HeadTrackerState {
    current: BlockHeader,
}

impl HeadTrackerState {
    pub fn is_clean_apply(&self, block: &BlockHeader) -> bool {
        // current head and block have the same height
        if block.number == self.current.number {
            return block.hash == self.current.hash;
        }
        if block.number == self.current.number + 1 {
            return match &block.parent_hash {
                None => false,
                Some(hash) => hash == &self.current.hash,
            };
        }
        false
    }

    pub fn update_current(&mut self, new_head: BlockHeader) {
        self.current = new_head;
    }
}
