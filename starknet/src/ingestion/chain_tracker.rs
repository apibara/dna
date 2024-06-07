use apibara_dna_common::{
    error::{DnaError, Result},
    ingestion::ChainChange,
};
use error_stack::ResultExt;
use futures_util::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use super::RpcProvider;

pub struct ChainTracker {
    provider: RpcProvider,
}

impl ChainTracker {
    pub fn new(provider: RpcProvider) -> Self {
        Self { provider }
    }

    pub fn start(self, ct: CancellationToken) -> impl Stream<Item = ChainChange> {
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(track_chain(self.provider, tx, ct));

        ReceiverStream::new(rx)
    }
}

async fn track_chain(
    provider: RpcProvider,
    tx: mpsc::Sender<ChainChange>,
    ct: CancellationToken,
) -> Result<()> {
    let mut head = provider.get_latest_block().await?;

    let mut finalized = provider
        .get_finalized_block(None, Some(head.number))
        .await?;

    let change = ChainChange::Initialize {
        head: head.clone(),
        finalized: finalized.clone(),
    };

    tx.send(change)
        .await
        .map_err(|_| DnaError::Fatal)
        .attach_printable("failed to send initial chain state")?;

    let mut head_timeout = tokio::time::interval(tokio::time::Duration::from_secs(2));
    let mut finalized_timeout = tokio::time::interval(tokio::time::Duration::from_secs(20));
    loop {
        tokio::select! {
            _ = ct.cancelled() => break,
            _ = head_timeout.tick() => {
                let new_head = provider.get_latest_block().await?;
                if new_head != head {
                    head = new_head;
                    let change = ChainChange::NewHead(head.clone());
                    tx.send(change).await.map_err(|_| DnaError::Fatal).attach_printable("failed to send chain change")?;
                }
            }
            _ = finalized_timeout.tick() => {
                let new_finalized = provider.get_finalized_block(Some(finalized.number), Some(head.number)).await?;
                if new_finalized != finalized {
                    finalized = new_finalized;
                    let change = ChainChange::NewFinalized(finalized.clone());
                    tx.send(change).await.map_err(|_| DnaError::Fatal).attach_printable("failed to send chain change")?;
                }
            }
        };
    }

    Ok(())
}