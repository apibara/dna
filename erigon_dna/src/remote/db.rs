//! Erigon remote db.

use croaring::Bitmap;
use reth_primitives::{Header, H160};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::trace;

use crate::erigon::{
    tables,
    types::{LogId, TransactionLog},
    BlockHash, Forkchoice, GlobalBlockId, LogAddressIndex,
};

use super::{
    kv::{RemoteCursor, RemoteTx},
    KvClient,
};

pub struct RemoteDB {
    client: KvClient<Channel>,
}

#[derive(Debug, thiserror::Error)]
pub enum RemoteDBError {
    #[error("remote db rpc error: {0}")]
    Rpc(#[from] tonic::Status),
}

pub struct LogStream<'a> {
    txn: RemoteTx,
    cursor: Mutex<Option<RemoteCursor<'a, tables::LogTable>>>,
    block_number: u64,
}

impl RemoteDB {
    pub fn new(client: KvClient<Channel>) -> Self {
        Self { client }
    }

    /// Access the header by the canonical block number.
    pub async fn header_by_number(
        &mut self,
        block_number: u64,
    ) -> Result<Option<Header>, RemoteDBError> {
        trace!(number = block_number, "header by number");
        let txn = self.client.begin_txn().await?;
        let block_id =
            if let Some(block_id) = self.canonical_block_id_with_txn(&txn, block_number).await? {
                block_id
            } else {
                return Ok(None);
            };
        trace!(block_id = ?block_id, "seek block header");
        let curr = txn.open_cursor::<tables::HeaderTable>().await?;
        match curr.seek_exact(&block_id).await? {
            None => Ok(None),
            Some((_, header)) => Ok(Some(header.0)),
        }
    }

    /// Returns a stream over the logs in the given block.
    pub async fn logs_by_block_number(
        &mut self,
        block_number: u64,
    ) -> Result<LogStream<'_>, RemoteDBError> {
        trace!(number = block_number, "logs by number");
        let txn = self.client.begin_txn().await?;
        Ok(LogStream {
            txn,
            cursor: Mutex::new(None),
            block_number,
        })
    }

    /// Returns the request fork choice.
    pub async fn forkchoice(
        &mut self,
        choice: Forkchoice,
    ) -> Result<Option<GlobalBlockId>, RemoteDBError> {
        trace!(forkchoice = ?choice, "forkchoice");
        let txn = self.client.begin_txn().await?;
        let curr = txn.open_cursor::<tables::LastForkchoiceTable>().await?;
        match curr.seek_exact(&choice).await?.map(|(_, hash)| hash) {
            None => Ok(None),
            Some(hash) => {
                if let Some(number) = self.block_number_by_hash_with_txn(&txn, &hash).await? {
                    let id = GlobalBlockId::new(number, hash.0);
                    Ok(Some(id))
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub async fn log_bitmap_for_addresses(
        &mut self,
        addresses: impl Iterator<Item = H160>,
        from_block: u64,
        to_block: u64,
    ) -> Result<Bitmap, RemoteDBError> {
        trace!(
            from_block = from_block,
            to_block = to_block,
            "logs bitmap for addresses"
        );
        let txn = self.client.begin_txn().await?;
        let curr = txn.open_cursor::<tables::LogAddressIndexTable>().await?;
        let mut chunks = Vec::new();
        for log_address in addresses {
            let log_addr_index = LogAddressIndex {
                log_address,
                block: from_block,
            };

            let mut value = curr.seek(&log_addr_index).await?;

            while let Some((k, v)) = value {
                if k.log_address != log_address {
                    break;
                }
                let bm = Bitmap::deserialize(&v);
                chunks.push(bm);
                if k.block >= to_block {
                    break;
                }

                value = curr.next().await?;
            }
        }
        let chunks: Vec<&Bitmap> = chunks.iter().collect();
        Ok(Bitmap::fast_or(&chunks))
    }

    /// Returns the block number by the block hash.
    pub async fn block_number_by_hash(
        &mut self,
        hash: BlockHash,
    ) -> Result<Option<u64>, RemoteDBError> {
        trace!(hash = ?hash, "header number");
        let txn = self.client.begin_txn().await?;
        let curr = txn.open_cursor::<tables::HeaderNumberTable>().await?;
        let number = curr.seek_exact(&hash).await?.map(|(_, number)| number);
        Ok(number)
    }

    /// Returns the canonical block hash by the block number.
    pub async fn canonical_block_hash(
        &mut self,
        block_number: u64,
    ) -> Result<Option<BlockHash>, RemoteDBError> {
        trace!(number = block_number, "canonical block hash");
        let txn = self.client.begin_txn().await?;
        let block_id = self.canonical_block_id_with_txn(&txn, block_number).await?;
        Ok(block_id.map(|id| BlockHash(id.1)))
    }

    /// Returns the canonical block id (number + Hash) by the block number.
    pub async fn canonical_block_id(
        &mut self,
        block_number: u64,
    ) -> Result<Option<GlobalBlockId>, RemoteDBError> {
        trace!(number = block_number, "canonical block id");
        let txn = self.client.begin_txn().await?;
        self.canonical_block_id_with_txn(&txn, block_number).await
    }

    async fn canonical_block_id_with_txn(
        &mut self,
        txn: &RemoteTx,
        block_number: u64,
    ) -> Result<Option<GlobalBlockId>, RemoteDBError> {
        let curr = txn.open_cursor::<tables::CanonicalHeaderTable>().await?;
        let global_id = match curr.seek_exact(&block_number).await? {
            None => return Ok(None),
            Some((number, hash)) => GlobalBlockId::new(number, hash.0),
        };
        Ok(Some(global_id))
    }

    async fn block_number_by_hash_with_txn(
        &mut self,
        txn: &RemoteTx,
        hash: &BlockHash,
    ) -> Result<Option<u64>, RemoteDBError> {
        let curr = txn.open_cursor::<tables::HeaderNumberTable>().await?;
        let number = curr.seek_exact(&hash).await?.map(|(_, number)| number);
        Ok(number)
    }
}

impl<'a> LogStream<'a> {
    pub async fn next_log(&'a self) -> Result<Option<(LogId, TransactionLog)>, RemoteDBError> {
        let mut maybe_cursor = self.cursor.lock().await;
        match &mut *maybe_cursor {
            None => {
                let cursor = self.txn.open_cursor::<tables::LogTable>().await?;
                let value = cursor.prefix(&self.block_number).await?;
                *maybe_cursor = Some(cursor);
                Ok(self.check_prefix(value))
            }
            Some(cursor) => {
                let value = cursor.next().await?;
                Ok(self.check_prefix(value))
            }
        }
    }

    // Returns the log only if the block prefix matches.
    fn check_prefix(
        &self,
        value: Option<(LogId, TransactionLog)>,
    ) -> Option<(LogId, TransactionLog)> {
        let (log_id, log) = value?;
        if log_id.0 == self.block_number {
            Some((log_id, log))
        } else {
            None
        }
    }
}
