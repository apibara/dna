//! Block ingestion helpers.

use alloy_eips::eip2718::Decodable2718;
use apibara_dna_common::{
    chain::BlockInfo,
    fragment::{
        Block, BodyFragment, HeaderFragment, Index, IndexFragment, IndexGroupFragment, Join,
        JoinFragment, JoinGroupFragment,
    },
    index::{BitmapIndexBuilder, ScalarValue},
    ingestion::{BlockIngestion, IngestionError},
    join::{JoinToManyIndex, JoinToManyIndexBuilder, JoinToOneIndex, JoinToOneIndexBuilder},
    Cursor, Hash,
};
use error_stack::{FutureExt, Result, ResultExt};
use prost::Message;
use tracing::Instrument;

use crate::{
    fragment::{
        BLOB_FRAGMENT_ID, BLOB_FRAGMENT_NAME, INDEX_TRANSACTION_BY_CREATE,
        INDEX_TRANSACTION_BY_FROM_ADDRESS, INDEX_TRANSACTION_BY_TO_ADDRESS,
        INDEX_VALIDATOR_BY_INDEX, INDEX_VALIDATOR_BY_STATUS, TRANSACTION_FRAGMENT_ID,
        TRANSACTION_FRAGMENT_NAME, VALIDATOR_FRAGMENT_ID, VALIDATOR_FRAGMENT_NAME,
    },
    proto::{FallibleModelExt, ModelExt},
    provider::{
        http::{BeaconApiErrorExt, BeaconApiProvider, BlockId},
        models::{self, BeaconCursorExt},
    },
};

#[derive(Clone)]
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
    ) -> Result<Option<(BlockInfo, Block)>, IngestionError> {
        // Fetch all data using the block root to avoid issues with reorgs.
        let block_root = match self.provider.get_block_root(block_id).await {
            Ok(header) => header,
            Err(err) if err.is_not_found() => return Ok(None),
            Err(err) => {
                return Err(err).change_context(IngestionError::RpcRequest);
            }
        };

        let block_id = BlockId::BlockRoot(block_root.data.root);

        let block = tokio::spawn({
            let provider = self.provider.clone();
            let block_id = block_id.clone();
            async move {
                provider
                    .get_block(block_id.clone())
                    .await
                    .change_context(IngestionError::RpcRequest)
                    .attach_printable("failed to get block")
                    .attach_printable_lazy(|| format!("block id: {block_id:?}"))
            }
        })
        .instrument(tracing::info_span!("beaconchain_get_block"));

        let blob_sidecar = tokio::spawn({
            let provider = self.provider.clone();
            let block_id = block_id.clone();
            async move {
                provider
                    .get_blob_sidecar(block_id.clone())
                    .await
                    .change_context(IngestionError::RpcRequest)
                    .attach_printable("failed to get blob sidecar")
                    .attach_printable_lazy(|| format!("block id: {block_id:?}"))
            }
        })
        .instrument(tracing::info_span!("beaconchain_get_blob_sidecar"));

        let validators = tokio::spawn({
            let provider = self.provider.clone();
            let block_id = block_id.clone();
            async move {
                match provider.get_validators(block_id.clone()).await {
                    Ok(response) => Ok(response.data),
                    Err(err) if err.is_not_found() => Ok(Vec::new()),
                    Err(err) => Err(err)
                        .change_context(IngestionError::RpcRequest)
                        .attach_printable("failed to get validators")
                        .attach_printable_lazy(|| format!("block id: {block_id:?}")),
                }
            }
        })
        .instrument(tracing::info_span!("beaconchain_get_validators"));

        let block = block.await.change_context(IngestionError::RpcRequest)??;

        let mut blobs = blob_sidecar
            .await
            .change_context(IngestionError::RpcRequest)??
            .data;
        let mut validators = validators
            .await
            .change_context(IngestionError::RpcRequest)??;

        let mut block = block.data.message;

        let block_info = BlockInfo {
            number: block.slot,
            hash: Hash(block_root.data.root.to_vec()),
            parent: Hash(block.parent_root.to_vec()),
        };

        let transactions = if let Some(ref mut execution_payload) = block.body.execution_payload {
            std::mem::take(&mut execution_payload.transactions)
        } else {
            Vec::new()
        };

        validators.sort_by_key(|v| v.index);
        blobs.sort_by_key(|b| b.index);

        let header_fragment = {
            let header = block.to_proto();
            HeaderFragment {
                data: header.encode_to_vec(),
            }
        };

        let (body, index, join) = collect_block_body_and_index(&transactions, &validators, &blobs)?;

        let block = Block {
            header: header_fragment,
            index,
            join,
            body,
        };

        Ok((block_info, block).into())
    }

    async fn get_block_info_for_missed_slot(
        &self,
        block_number: u64,
    ) -> Result<BlockInfo, IngestionError> {
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

        let hash = Hash([0; 32].to_vec());
        let parent = Hash(parent_hash.to_vec());

        let block_info = BlockInfo {
            number: block_number,
            hash,
            parent,
        };

        Ok(block_info)
    }
}

impl BlockIngestion for BeaconChainBlockIngestion {
    #[tracing::instrument("beaconchain_get_head_cursor", skip(self), err(Debug))]
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

    #[tracing::instrument("beaconchain_get_finalized_cursor", skip(self), err(Debug))]
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

    #[tracing::instrument("beaconchain_get_block_info_by_number", skip(self), err(Debug))]
    async fn get_block_info_by_number(
        &self,
        block_number: u64,
    ) -> Result<BlockInfo, IngestionError> {
        let header = match self.provider.get_header(BlockId::Slot(block_number)).await {
            Ok(header) => header,
            Err(err) if err.is_not_found() => {
                return self.get_block_info_for_missed_slot(block_number).await
            }
            Err(err) => {
                return Err(err)
                    .change_context(IngestionError::RpcRequest)
                    .attach_printable("failed to get header by number")
                    .attach_printable_lazy(|| format!("block number: {}", block_number))
            }
        };

        let hash = header.data.root;
        let parent = header.data.header.message.parent_root;
        let number = header.data.header.message.slot;

        Ok(BlockInfo {
            number,
            hash: Hash(hash.0.to_vec()),
            parent: Hash(parent.0.to_vec()),
        })
    }

    #[tracing::instrument("beaconchain_ingest_block_by_number", skip(self), err(Debug))]
    async fn ingest_block_by_number(
        &self,
        block_number: u64,
    ) -> Result<(BlockInfo, Block), IngestionError> {
        let block = self
            .ingest_block_by_id(BlockId::Slot(block_number))
            .change_context(IngestionError::RpcRequest)
            .attach_printable("failed to get block by number")
            .attach_printable_lazy(|| format!("block number: {}", block_number))
            .await?;

        if let Some((block_info, block)) = block {
            return Ok((block_info, block));
        }

        if block_number == 0 {
            return Err(IngestionError::BlockNotFound).attach_printable("genesis block not found");
        }

        let block_info = self.get_block_info_for_missed_slot(block_number).await?;

        // Missed slots have no data and no indices.
        let header_fragment = HeaderFragment { data: Vec::new() };

        let index_fragment = IndexGroupFragment {
            indexes: vec![
                IndexFragment {
                    fragment_id: TRANSACTION_FRAGMENT_ID,
                    range_start: 0,
                    range_len: 0,
                    indexes: Vec::default(),
                },
                IndexFragment {
                    fragment_id: VALIDATOR_FRAGMENT_ID,
                    range_start: 0,
                    range_len: 0,
                    indexes: Vec::default(),
                },
                IndexFragment {
                    fragment_id: BLOB_FRAGMENT_ID,
                    range_start: 0,
                    range_len: 0,
                    indexes: Vec::default(),
                },
            ],
        };

        let join_fragment = JoinGroupFragment {
            joins: vec![
                JoinFragment {
                    fragment_id: TRANSACTION_FRAGMENT_ID,
                    joins: vec![Join {
                        to_fragment_id: BLOB_FRAGMENT_ID,
                        index: JoinToManyIndex::default().into(),
                    }],
                },
                JoinFragment {
                    fragment_id: VALIDATOR_FRAGMENT_ID,
                    joins: Vec::default(),
                },
                JoinFragment {
                    fragment_id: BLOB_FRAGMENT_ID,
                    joins: vec![Join {
                        to_fragment_id: TRANSACTION_FRAGMENT_ID,
                        index: JoinToOneIndex::default().into(),
                    }],
                },
            ],
        };

        let body_fragments = vec![
            BodyFragment {
                fragment_id: TRANSACTION_FRAGMENT_ID,
                name: TRANSACTION_FRAGMENT_NAME.to_string(),
                data: Vec::default(),
            },
            BodyFragment {
                fragment_id: VALIDATOR_FRAGMENT_ID,
                name: VALIDATOR_FRAGMENT_NAME.to_string(),
                data: Vec::default(),
            },
            BodyFragment {
                fragment_id: BLOB_FRAGMENT_ID,
                name: BLOB_FRAGMENT_NAME.to_string(),
                data: Vec::default(),
            },
        ];

        let block = Block {
            header: header_fragment,
            index: index_fragment,
            body: body_fragments,
            join: join_fragment,
        };

        Ok((block_info, block))
    }
}

pub fn collect_block_body_and_index(
    transactions: &[models::Bytes],
    validators: &[models::Validator],
    blobs: &[models::BlobSidecar],
) -> Result<(Vec<BodyFragment>, IndexGroupFragment, JoinGroupFragment), IngestionError> {
    let transactions = transactions
        .iter()
        .map(|bytes| decode_transaction(bytes))
        .collect::<Result<Vec<_>, _>>()?;

    let mut block_transactions = Vec::new();
    let mut block_validators = Vec::new();
    let mut block_blobs = Vec::new();

    let mut index_transaction_by_from_address = BitmapIndexBuilder::default();
    let mut index_transaction_by_to_address = BitmapIndexBuilder::default();
    let mut index_transaction_by_create = BitmapIndexBuilder::default();
    let mut join_transaction_to_blobs = JoinToManyIndexBuilder::default();

    let mut index_validator_by_index = BitmapIndexBuilder::default();
    let mut index_validator_by_status = BitmapIndexBuilder::default();

    let mut join_blob_to_transaction = JoinToOneIndexBuilder::default();

    for (transaction_index, transaction) in transactions.into_iter().enumerate() {
        let transaction_index = transaction_index as u32;

        let mut transaction = transaction.to_proto()?;
        transaction.transaction_index = transaction_index;

        if let Some(from) = transaction.from {
            index_transaction_by_from_address
                .insert(ScalarValue::B160(from.to_bytes()), transaction_index);
        }

        match transaction.to {
            Some(to) => {
                index_transaction_by_to_address
                    .insert(ScalarValue::B160(to.to_bytes()), transaction_index);
                index_transaction_by_create.insert(ScalarValue::Bool(false), transaction_index);
            }
            None => {
                index_transaction_by_create.insert(ScalarValue::Bool(true), transaction_index);
            }
        }

        block_transactions.push(transaction);
    }

    for (validator_offset, validator) in validators.iter().enumerate() {
        let validator_offset = validator_offset as u32;
        let validator = validator.to_proto();

        index_validator_by_index.insert(
            ScalarValue::Uint32(validator.validator_index),
            validator_offset,
        );

        index_validator_by_status.insert(ScalarValue::Int32(validator.status), validator_offset);

        block_validators.push(validator);
    }

    for blob in blobs.iter() {
        let mut blob = blob.to_proto();

        let Some(tx) = block_transactions.iter().find(|tx| {
            tx.blob_versioned_hashes
                .contains(&blob.blob_hash.unwrap_or_default())
        }) else {
            return Err(IngestionError::Model)
                .attach_printable("no transaction found for blob")
                .attach_printable_lazy(|| {
                    format!("blob hash: {}", blob.blob_hash.unwrap_or_default())
                });
        };

        blob.transaction_index = tx.transaction_index;
        blob.transaction_hash = tx.transaction_hash;

        join_blob_to_transaction.insert(blob.blob_index, tx.transaction_index);
        join_transaction_to_blobs.insert(tx.transaction_index, blob.blob_index);

        block_blobs.push(blob);
    }

    let transaction_index = {
        let index_transaction_by_from_address = Index {
            index_id: INDEX_TRANSACTION_BY_FROM_ADDRESS,
            index: index_transaction_by_from_address
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let index_transaction_by_to_address = Index {
            index_id: INDEX_TRANSACTION_BY_TO_ADDRESS,
            index: index_transaction_by_to_address
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let index_transaction_by_create = Index {
            index_id: INDEX_TRANSACTION_BY_CREATE,
            index: index_transaction_by_create
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        IndexFragment {
            fragment_id: TRANSACTION_FRAGMENT_ID,
            range_start: 0,
            range_len: block_transactions.len() as u32,
            indexes: vec![
                index_transaction_by_from_address,
                index_transaction_by_to_address,
                index_transaction_by_create,
            ],
        }
    };

    let transaction_join = {
        let join_transaction_to_blobs = Join {
            to_fragment_id: BLOB_FRAGMENT_ID,
            index: join_transaction_to_blobs
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        JoinFragment {
            fragment_id: TRANSACTION_FRAGMENT_ID,
            joins: vec![join_transaction_to_blobs],
        }
    };

    let transaction_fragment = BodyFragment {
        fragment_id: TRANSACTION_FRAGMENT_ID,
        name: TRANSACTION_FRAGMENT_NAME.to_string(),
        data: block_transactions
            .iter()
            .map(Message::encode_to_vec)
            .collect(),
    };

    let validator_index = {
        let index_validator_by_index = Index {
            index_id: INDEX_VALIDATOR_BY_INDEX,
            index: index_validator_by_index
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        let index_validator_by_status = Index {
            index_id: INDEX_VALIDATOR_BY_STATUS,
            index: index_validator_by_status
                .build()
                .change_context(IngestionError::Indexing)?
                .into(),
        };

        IndexFragment {
            fragment_id: VALIDATOR_FRAGMENT_ID,
            range_start: 0,
            range_len: block_validators.len() as u32,
            indexes: vec![index_validator_by_index, index_validator_by_status],
        }
    };

    let validator_join = JoinFragment {
        fragment_id: VALIDATOR_FRAGMENT_ID,
        joins: Vec::default(),
    };

    let validator_fragment = BodyFragment {
        fragment_id: VALIDATOR_FRAGMENT_ID,
        name: VALIDATOR_FRAGMENT_NAME.to_string(),
        data: block_validators
            .iter()
            .map(Message::encode_to_vec)
            .collect(),
    };

    let blob_index = IndexFragment {
        fragment_id: BLOB_FRAGMENT_ID,
        range_start: 0,
        range_len: block_blobs.len() as u32,
        indexes: Vec::new(),
    };

    let blob_join = {
        let join_blob_to_transaction = Join {
            to_fragment_id: TRANSACTION_FRAGMENT_ID,
            index: join_blob_to_transaction.build().into(),
        };

        JoinFragment {
            fragment_id: BLOB_FRAGMENT_ID,
            joins: vec![join_blob_to_transaction],
        }
    };

    let blob_fragment = BodyFragment {
        fragment_id: BLOB_FRAGMENT_ID,
        name: BLOB_FRAGMENT_NAME.to_string(),
        data: block_blobs.iter().map(Message::encode_to_vec).collect(),
    };

    let index_group = IndexGroupFragment {
        indexes: vec![transaction_index, validator_index, blob_index],
    };

    let join_group = JoinGroupFragment {
        joins: vec![transaction_join, validator_join, blob_join],
    };

    Ok((
        vec![transaction_fragment, validator_fragment, blob_fragment],
        index_group,
        join_group,
    ))
}

pub fn decode_transaction(mut bytes: &[u8]) -> Result<models::TxEnvelope, IngestionError> {
    models::TxEnvelope::network_decode(&mut bytes)
        .change_context(IngestionError::Model)
        .attach_printable("failed to decode EIP 2718 transaction")
}
