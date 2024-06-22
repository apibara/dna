use error_stack::{Report, ResultExt};

use crate::{ingestion::models, segment::store};

#[derive(Debug)]
pub struct ConversionError;

impl From<models::Validator> for store::Validator {
    fn from(x: models::Validator) -> Self {
        store::Validator {
            validator_index: x.index,
            balance: x.balance,
            status: x.status,
            pubkey: x.validator.pubkey.into(),
            withdrawal_credentials: x.validator.withdrawal_credentials.into(),
            effective_balance: x.validator.effective_balance,
            slashed: x.validator.slashed,
            activation_eligibility_epoch: x.validator.activation_eligibility_epoch,
            activation_epoch: x.validator.activation_epoch,
            exit_epoch: x.validator.exit_epoch,
            withdrawable_epoch: x.validator.withdrawable_epoch,
        }
    }
}

impl From<models::BlobSidecar> for store::Blob {
    fn from(x: models::BlobSidecar) -> Self {
        store::Blob {
            index: x.index,
            blob: x.blob.into(),
            kzg_commitment: x.kzg_commitment.into(),
            kzg_proof: x.kzg_proof.into(),
            kzg_commitment_inclusion_proof: x
                .kzg_commitment_inclusion_proof
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

impl From<models::BeaconBlock> for store::BlockHeader {
    fn from(x: models::BeaconBlock) -> Self {
        store::BlockHeader {
            slot: x.slot,
            proposer_index: x.proposer_index,
            parent_root: x.parent_root.into(),
            state_root: x.state_root.into(),
            randao_reveal: x.body.randao_reveal.into(),
            deposit_count: x.body.eth1_data.deposit_count,
            deposit_root: x.body.eth1_data.deposit_root.into(),
            block_hash: x.body.eth1_data.block_hash.into(),
            graffiti: x.body.graffiti.into(),
            execution_payload: x.body.execution_payload.into(),
            blob_kzg_commitments: x
                .body
                .blob_kzg_commitments
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}
impl From<models::ExecutionPayload> for store::ExecutionPayload {
    fn from(x: models::ExecutionPayload) -> Self {
        store::ExecutionPayload {
            parent_hash: x.parent_hash.into(),
            fee_recipient: x.fee_recipient.into(),
            state_root: x.state_root.into(),
            receipts_root: x.receipts_root.into(),
            logs_bloom: x.logs_bloom.into(),
            prev_randao: x.prev_randao.into(),
            block_number: x.block_number,
            timestamp: x.timestamp,
        }
    }
}

impl TryFrom<models::TxEnvelope> for store::Transaction {
    type Error = Report<ConversionError>;

    fn try_from(x: models::TxEnvelope) -> Result<Self, Self::Error> {
        use models::TxEnvelope::*;
        match x {
            Legacy(tx) => tx.try_into(),
            Eip2930(tx) => tx.try_into(),
            Eip1559(tx) => tx.try_into(),
            Eip4844(tx) => tx.try_into(),
            _ => Err::<_, Report<ConversionError>>(ConversionError.into())
                .attach_printable("invalid transaction type"),
        }
    }
}

impl TryFrom<models::Signed<models::TxLegacy>> for store::Transaction {
    type Error = Report<ConversionError>;

    fn try_from(x: models::Signed<models::TxLegacy>) -> Result<Self, Self::Error> {
        let from = x
            .recover_signer()
            .change_context(ConversionError)
            .attach_printable("failed to recover sender of legacy transaction")?;

        Ok(store::Transaction {
            transaction_type: 0,
            transaction_index: u32::MAX,
            transaction_hash: x.hash().into(),
            nonce: x.tx().nonce,
            from: from.into(),
            to: x.tx().to.to_option(),
            value: x.tx().value.into(),
            gas_price: Some(x.tx().gas_price),
            gas_limit: x.tx().gas_limit,
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            max_fee_per_blob_gas: None,
            input: x.tx().input.clone().into(),
            signature: x.signature().into(),
            chain_id: x.tx().chain_id,
            access_list: Vec::default(),
            blob_versioned_hashes: Vec::default(),
        })
    }
}

impl TryFrom<models::Signed<models::TxEip2930>> for store::Transaction {
    type Error = Report<ConversionError>;

    fn try_from(x: models::Signed<models::TxEip2930>) -> Result<Self, Self::Error> {
        let from = x
            .recover_signer()
            .change_context(ConversionError)
            .attach_printable("failed to recover sender of legacy transaction")?;

        let access_list = x.tx().access_list.iter().map(Into::into).collect();

        Ok(store::Transaction {
            transaction_type: 0,
            transaction_index: u32::MAX,
            transaction_hash: x.hash().into(),
            nonce: x.tx().nonce,
            from: from.into(),
            to: x.tx().to.to_option(),
            value: x.tx().value.into(),
            gas_price: Some(x.tx().gas_price),
            gas_limit: x.tx().gas_limit,
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            max_fee_per_blob_gas: None,
            input: x.tx().input.clone().into(),
            signature: x.signature().into(),
            chain_id: Some(x.tx().chain_id),
            access_list,
            blob_versioned_hashes: Vec::default(),
        })
    }
}

impl TryFrom<models::Signed<models::TxEip1559>> for store::Transaction {
    type Error = Report<ConversionError>;

    fn try_from(x: models::Signed<models::TxEip1559>) -> Result<Self, Self::Error> {
        let from = x
            .recover_signer()
            .change_context(ConversionError)
            .attach_printable("failed to recover sender of legacy transaction")?;

        let access_list = x.tx().access_list.iter().map(Into::into).collect();

        Ok(store::Transaction {
            transaction_type: 0,
            transaction_index: u32::MAX,
            transaction_hash: x.hash().into(),
            nonce: x.tx().nonce,
            from: from.into(),
            to: x.tx().to.to_option(),
            value: x.tx().value.into(),
            gas_price: None,
            gas_limit: x.tx().gas_limit,
            max_fee_per_gas: Some(x.tx().max_fee_per_gas),
            max_priority_fee_per_gas: Some(x.tx().max_priority_fee_per_gas),
            max_fee_per_blob_gas: None,
            input: x.tx().input.clone().into(),
            signature: x.signature().into(),
            chain_id: Some(x.tx().chain_id),
            access_list,
            blob_versioned_hashes: Vec::default(),
        })
    }
}

impl TryFrom<models::Signed<models::TxEip4844Variant>> for store::Transaction {
    type Error = Report<ConversionError>;

    fn try_from(x: models::Signed<models::TxEip4844Variant>) -> Result<Self, Self::Error> {
        let tx = x.tx().tx();

        let from = x
            .recover_signer()
            .change_context(ConversionError)
            .attach_printable("failed to recover sender of legacy transaction")?;

        let access_list = tx.access_list.iter().map(Into::into).collect();
        let blob_versioned_hashes = tx.blob_versioned_hashes.iter().map(Into::into).collect();

        Ok(store::Transaction {
            transaction_type: 0,
            transaction_index: u32::MAX,
            transaction_hash: x.hash().into(),
            nonce: tx.nonce,
            from: from.into(),
            to: Some(tx.to.into()),
            value: tx.value.into(),
            gas_price: None,
            gas_limit: tx.gas_limit,
            max_fee_per_gas: Some(tx.max_fee_per_gas),
            max_priority_fee_per_gas: Some(tx.max_priority_fee_per_gas),
            max_fee_per_blob_gas: Some(tx.max_fee_per_blob_gas),
            input: tx.input.clone().into(),
            signature: x.signature().into(),
            chain_id: Some(tx.chain_id),
            access_list,
            blob_versioned_hashes,
        })
    }
}

impl From<&models::Signature> for store::Signature {
    fn from(x: &models::Signature) -> Self {
        store::Signature {
            r: x.r().into(),
            s: x.s().into(),
        }
    }
}

impl From<&models::AccessListItem> for store::AccessListItem {
    fn from(x: &models::AccessListItem) -> Self {
        let storage_keys = x.storage_keys.iter().map(Into::into).collect();
        store::AccessListItem {
            address: x.address.into(),
            storage_keys,
        }
    }
}

impl From<models::B256> for store::B256 {
    fn from(x: models::B256) -> Self {
        store::B256(x.0)
    }
}

impl From<models::U256> for store::U256 {
    fn from(x: models::U256) -> Self {
        store::U256(x.to_be_bytes())
    }
}

impl From<&models::B256> for store::B256 {
    fn from(x: &models::B256) -> Self {
        store::B256(x.0)
    }
}

impl From<models::B384> for store::B384 {
    fn from(x: models::B384) -> Self {
        store::B384(x.to_be_bytes())
    }
}

impl From<models::Address> for store::Address {
    fn from(x: models::Address) -> Self {
        store::Address(x.0.into())
    }
}

impl From<models::Bytes> for store::Bytes {
    fn from(x: models::Bytes) -> Self {
        store::Bytes(x.0.to_vec())
    }
}

impl error_stack::Context for ConversionError {}

impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Failed to convert data from RPC to storage type")
    }
}

trait TxKindExt {
    fn to_option(self) -> Option<store::Address>;
}

impl TxKindExt for models::TxKind {
    fn to_option(self) -> Option<store::Address> {
        match self {
            models::TxKind::Create => None,
            models::TxKind::Call(to) => Some(to.into()),
        }
    }
}
