use apibara_dna_common::error::{DnaError, Result};
use error_stack::ResultExt;
use futures_util::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::{core::Cursor, segment::conversion::U64Ext};

use super::{models, RpcProvider};

pub struct ChainTracker {
    provider: RpcProvider,
}

#[derive(Debug, Clone)]
pub enum ChainChange {
    /// First message in the stream, with the starting state.
    Initialize { head: Cursor, finalized: Cursor },
    /// A new head has been detected.
    NewHead(Cursor),
    /// A new finalized block has been detected.
    NewFinalized(Cursor),
    /// The chain reorganized.
    Invalidate,
}

impl ChainTracker {
    pub fn new(provider: RpcProvider) -> Self {
        Self { provider }
    }

    pub fn start(self, ct: CancellationToken) -> impl Stream<Item = Result<ChainChange>> {
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(track_chain(self.provider, tx, ct));

        ReceiverStream::new(rx)
    }
}

async fn track_chain(
    provider: RpcProvider,
    tx: mpsc::Sender<Result<ChainChange>>,
    ct: CancellationToken,
) -> Result<()> {
    let mut head = provider.get_latest_block().await?.cursor();
    let mut finalized = provider.get_finalized_block().await?.cursor();

    let change = ChainChange::Initialize {
        head: head.clone(),
        finalized: finalized.clone(),
    };

    tx.send(Ok(change))
        .await
        .map_err(|_| DnaError::Fatal)
        .attach_printable("failed to send initial chain state")?;

    let mut head_timeout = tokio::time::interval(tokio::time::Duration::from_secs(2));
    let mut finalized_timeout = tokio::time::interval(tokio::time::Duration::from_secs(20));
    loop {
        tokio::select! {
            _ = ct.cancelled() => break,
            _ = head_timeout.tick() => {
                let new_head = provider.get_latest_block().await?.cursor();
                if new_head != head {
                    head = new_head;
                    let change = ChainChange::NewHead(head.clone());
                    tx.send(Ok(change)).await.map_err(|_| DnaError::Fatal).attach_printable("failed to send chain change")?;
                }
            }
            _ = finalized_timeout.tick() => {
                let new_finalized = provider.get_finalized_block().await?.cursor();
                if new_finalized != finalized {
                    finalized = new_finalized;
                    let change = ChainChange::NewFinalized(finalized.clone());
                    tx.send(Ok(change)).await.map_err(|_| DnaError::Fatal).attach_printable("failed to send chain change")?;
                }
            }
        };
    }

    Ok(())
}

pub trait BlockExt {
    fn cursor(&self) -> Cursor;
}

impl BlockExt for models::Block {
    fn cursor(&self) -> Cursor {
        Cursor::new(
            self.header.number.unwrap_or_default().as_u64(),
            self.header.hash.unwrap_or_default(),
        )
    }
}
