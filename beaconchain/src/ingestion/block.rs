//! Block ingestion helpers.

use alloy_eips::eip2718::Decodable2718;
use apibara_dna_common::{
    chain::BlockInfo,
    ingestion::{BlockIngestion, IngestionError},
    Cursor, Hash,
};
use error_stack::{FutureExt, Result, ResultExt};

use crate::{
    provider::{
        http::{BeaconApiErrorExt, BeaconApiProvider, BlockId},
        models::{self, BeaconCursorExt},
    },
    store::{self, fragment},
};

pub struct BeaconChainBlockIngestion {
    provider: BeaconApiProvider,
}

impl BeaconChainBlockIngestion {
    pub fn new(provider: BeaconApiProvider) -> Self {
        Self { provider }
    }

    async fn ingest_block_by_id(
        &self,
        block_id: BlockId,
    ) -> Result<Option<(BlockInfo, store::block::Block)>, IngestionError> {
        // Fetch all data using the block root to avoid issues with reorgs.
        let block_root = match self.provider.get_block_root(block_id).await {
            Ok(header) => header,
            Err(err) if err.is_not_found() => return Ok(None),
            Err(err) => {
                return Err(err).change_context(IngestionError::RpcRequest);
            }
        };

        let block_hash = fragment::B256::from(block_root.data.root);
        let block_id = BlockId::BlockRoot(block_root.data.root);

        let block = async {
            self.provider
                .get_block(block_id.clone())
                .await
                .change_context(IngestionError::RpcRequest)
                .attach_printable("failed to get block")
                .attach_printable_lazy(|| format!("block id: {block_id:?}"))
        };

        let blob_sidecar = async {
            self.provider
                .get_blob_sidecar(block_id.clone())
                .await
                .change_context(IngestionError::RpcRequest)
                .attach_printable("failed to get blob sidecar")
                .attach_printable_lazy(|| format!("block id: {block_id:?}"))
        };

        let validators = async {
            match self.provider.get_validators(block_id.clone()).await {
                Ok(response) => Ok(response.data),
                Err(err) if err.is_not_found() => Ok(Vec::new()),
                Err(err) => Err(err)
                    .change_context(IngestionError::RpcRequest)
                    .attach_printable("failed to get validators")
                    .attach_printable_lazy(|| format!("block id: {block_id:?}")),
            }
        };

        let block = block.await?;
        let blob_sidecar = blob_sidecar.await?;
        let validators = validators.await?;

        let mut block = block.data.message;

        let transactions = if let Some(ref mut execution_payload) = block.body.execution_payload {
            std::mem::take(&mut execution_payload.transactions)
        } else {
            Vec::new()
        };

        let header = fragment::BlockHeader::from(block);

        let block_info = BlockInfo {
            number: header.slot,
            hash: block_hash.into(),
            parent: header.parent_root.into(),
        };

        let transactions = transactions
            .into_iter()
            .enumerate()
            .map(|(tx_index, bytes)| decode_transaction(tx_index, &bytes))
            .collect::<Result<Vec<_>, _>>()
            .change_context(IngestionError::Model)
            .attach_printable("failed to decode transactions")?;

        let mut blobs = blob_sidecar
            .data
            .into_iter()
            .map(fragment::Blob::from)
            .collect::<Vec<_>>();

        add_transaction_to_blobs(&mut blobs, &transactions)
            .change_context(IngestionError::Model)
            .attach_printable("failed to add transactions to blobs")?;

        let validators = validators
            .into_iter()
            .map(fragment::Validator::from)
            .collect::<Vec<_>>();

        let block = {
            let mut block_builder = store::block::BlockBuilder::new(header);
            block_builder.add_transactions(transactions);
            block_builder.add_validators(validators);
            block_builder.add_blobs(blobs);
            block_builder
                .build()
                .change_context(IngestionError::Model)
                .attach_printable("failed to build block")?
        };

        Ok((block_info, block).into())
    }
}

impl BlockIngestion for BeaconChainBlockIngestion {
    type Block = fragment::Slot<store::block::Block>;

    async fn get_head_cursor(&self) -> Result<Cursor, IngestionError> {
        let cursor = self
            .provider
            .get_header(BlockId::Head)
            .await
            .change_context(IngestionError::RpcRequest)
            .attach_printable("failed to get head header")?
            .cursor();
        Ok(cursor)
    }

    async fn get_finalized_cursor(&self) -> Result<Cursor, IngestionError> {
        let cursor = self
            .provider
            .get_header(BlockId::Finalized)
            .await
            .change_context(IngestionError::RpcRequest)
            .attach_printable("failed to get finalized header")?
            .cursor();
        Ok(cursor)
    }

    async fn ingest_block_by_number(
        &self,
        block_number: u64,
    ) -> Result<(BlockInfo, Self::Block), IngestionError> {
        let block = self
            .ingest_block_by_id(BlockId::Slot(block_number))
            .change_context(IngestionError::RpcRequest)
            .attach_printable("failed to get block by number")
            .attach_printable_lazy(|| format!("block number: {}", block_number))
            .await?;

        if let Some((block_info, block)) = block {
            return Ok((block_info, fragment::Slot::Proposed(block)));
        }

        if block_number == 0 {
            return Err(IngestionError::BlockNotFound).attach_printable("genesis block not found");
        }

        let parent_hash = match self
            .provider
            .get_block_root(BlockId::Slot(block_number - 1))
            .await
        {
            Ok(response) => response.data.root,
            Err(err) if err.is_not_found() => models::B256::default(),
            Err(err) => {
                return Err(err).change_context(IngestionError::RpcRequest);
            }
        };

        let hash = fragment::B256::default();
        let parent_hash = fragment::B256::from(parent_hash);

        let block_info = BlockInfo {
            number: block_number,
            hash: hash.into(),
            parent: parent_hash.into(),
        };

        Ok((block_info, fragment::Slot::Missed { slot: block_number }))
    }

    async fn ingest_block_by_hash(
        &self,
        block_hash: impl Into<Hash>,
    ) -> Result<(BlockInfo, Self::Block), IngestionError> {
        let hash = block_hash.into();
        let hash = models::B256::try_from(hash.as_slice())
            .change_context(IngestionError::BadHash)
            .attach_printable("failed to convert hash to B256")
            .attach_printable_lazy(|| format!("hash: {}", hash))?;

        let (block_info, block) = self
            .ingest_block_by_id(BlockId::BlockRoot(hash))
            .change_context(IngestionError::RpcRequest)
            .attach_printable("failed to get block by hash")
            .attach_printable_lazy(|| format!("block hash: {}", hash))
            .await?
            .ok_or(IngestionError::BlockNotFound)
            .attach_printable("block with the given hash not found")
            .attach_printable_lazy(|| format!("block hash: {}", hash))?;

        Ok((block_info, fragment::Slot::Proposed(block)))
    }
}

pub fn decode_transaction(
    transaction_index: usize,
    mut bytes: &[u8],
) -> Result<fragment::Transaction, IngestionError> {
    let tx = models::TxEnvelope::network_decode(&mut bytes)
        .change_context(IngestionError::Model)
        .attach_printable("failed to decode EIP 2718 transaction")?;
    let mut tx = fragment::Transaction::try_from(tx).change_context(IngestionError::Model)?;

    tx.transaction_index = transaction_index as u32;

    Ok(tx)
}

pub fn add_transaction_to_blobs(
    blobs: &mut [fragment::Blob],
    transactions: &[fragment::Transaction],
) -> Result<(), IngestionError> {
    let mut blobs_updated = 0;
    for transaction in transactions {
        for blob_hash in transaction.blob_versioned_hashes.iter() {
            let blob = blobs
                .iter_mut()
                .find(|blob| &blob.blob_hash == blob_hash)
                .ok_or(IngestionError::Model)
                .attach_printable("expected blob to exist")
                .attach_printable_lazy(|| {
                    format!(
                        "blob_hash: {:?}, transaction_hash: {:?}",
                        blob_hash, transaction.transaction_hash
                    )
                })?;
            blob.transaction_index = transaction.transaction_index;
            blob.transaction_hash = transaction.transaction_hash;

            blobs_updated += 1;
        }
    }

    assert!(blobs_updated == blobs.len());
    Ok(())
}
