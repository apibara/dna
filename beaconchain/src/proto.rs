use apibara_dna_common::ingestion::IngestionError;
use apibara_dna_protocol::beaconchain;
use error_stack::{Result, ResultExt};

use crate::provider::models;

pub trait ModelExt {
    type Proto;

    fn to_proto(&self) -> Self::Proto;
}

pub trait FallibleModelExt {
    type Proto;

    fn to_proto(&self) -> Result<Self::Proto, IngestionError>;
}

trait TxKindExt {
    fn to_option(self) -> Option<beaconchain::Address>;
}

impl ModelExt for models::BeaconBlock {
    type Proto = beaconchain::BlockHeader;

    fn to_proto(&self) -> Self::Proto {
        beaconchain::BlockHeader {
            slot: self.slot,
            proposer_index: self.proposer_index,
            parent_root: self.parent_root.to_proto().into(),
            state_root: self.state_root.to_proto().into(),
            randao_reveal: self.body.randao_reveal.to_vec(),
            deposit_count: self.body.eth1_data.deposit_count,
            deposit_root: self.body.eth1_data.deposit_root.to_proto().into(),
            block_hash: self.body.eth1_data.block_hash.to_proto().into(),
            graffiti: self.body.graffiti.to_proto().into(),
            execution_payload: self.body.execution_payload.as_ref().map(ModelExt::to_proto),
            blob_kzg_commitments: self
                .body
                .blob_kzg_commitments
                .iter()
                .map(ModelExt::to_proto)
                .collect(),
        }
    }
}

impl ModelExt for models::ExecutionPayload {
    type Proto = beaconchain::ExecutionPayload;

    fn to_proto(&self) -> Self::Proto {
        let timestamp = prost_types::Timestamp {
            seconds: self.timestamp as i64,
            nanos: 0,
        };

        beaconchain::ExecutionPayload {
            parent_hash: self.parent_hash.to_proto().into(),
            fee_recipient: self.fee_recipient.to_proto().into(),
            state_root: self.state_root.to_proto().into(),
            receipts_root: self.receipts_root.to_proto().into(),
            logs_bloom: self.logs_bloom.to_vec(),
            prev_randao: self.prev_randao.to_proto().into(),
            block_number: self.block_number,
            timestamp: timestamp.into(),
        }
    }
}

impl FallibleModelExt for models::TxEnvelope {
    type Proto = beaconchain::Transaction;

    fn to_proto(&self) -> Result<Self::Proto, IngestionError> {
        match self {
            models::TxEnvelope::Legacy(tx) => tx.to_proto(),
            models::TxEnvelope::Eip2930(tx) => tx.to_proto(),
            models::TxEnvelope::Eip1559(tx) => tx.to_proto(),
            models::TxEnvelope::Eip4844(tx) => tx.to_proto(),
            _ => Err(IngestionError::Model).attach_printable("unknown transaction type"),
        }
    }
}

impl FallibleModelExt for models::Signed<models::TxLegacy> {
    type Proto = beaconchain::Transaction;

    fn to_proto(&self) -> Result<Self::Proto, IngestionError> {
        let from = self
            .recover_signer()
            .change_context(IngestionError::Model)
            .attach_printable("failed to recover sender of legacy transaction")?;
        let tx = self.tx();

        Ok(beaconchain::Transaction {
            filter_ids: Vec::default(),
            transaction_type: models::TxType::Legacy as u64,
            transaction_index: u32::MAX,
            transaction_hash: self.hash().to_proto().into(),
            nonce: tx.nonce,
            from: from.to_proto().into(),
            to: tx.to.to_option(),
            value: tx.value.to_proto().into(),
            gas_price: tx.gas_price.to_proto().into(),
            gas_limit: tx.gas_limit.to_proto().into(),
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            max_fee_per_blob_gas: None,
            input: tx.input.to_vec(),
            signature: self.signature().to_proto().into(),
            chain_id: tx.chain_id,
            access_list: Vec::default(),
            blob_versioned_hashes: Vec::default(),
        })
    }
}

impl FallibleModelExt for models::Signed<models::TxEip2930> {
    type Proto = beaconchain::Transaction;

    fn to_proto(&self) -> Result<Self::Proto, IngestionError> {
        let from = self
            .recover_signer()
            .change_context(IngestionError::Model)
            .attach_printable("failed to recover sender of EIP-2930 transaction")?;
        let tx = self.tx();

        Ok(beaconchain::Transaction {
            filter_ids: Vec::default(),
            transaction_type: models::TxType::Eip2930 as u64,
            transaction_index: u32::MAX,
            transaction_hash: self.hash().to_proto().into(),
            nonce: tx.nonce,
            from: from.to_proto().into(),
            to: tx.to.to_option(),
            value: tx.value.to_proto().into(),
            gas_price: tx.gas_price.to_proto().into(),
            gas_limit: tx.gas_limit.to_proto().into(),
            max_fee_per_gas: None,
            max_priority_fee_per_gas: None,
            max_fee_per_blob_gas: None,
            input: tx.input.to_vec(),
            signature: self.signature().to_proto().into(),
            chain_id: tx.chain_id.into(),
            access_list: tx.access_list.iter().map(ModelExt::to_proto).collect(),
            blob_versioned_hashes: Vec::default(),
        })
    }
}

impl FallibleModelExt for models::Signed<models::TxEip1559> {
    type Proto = beaconchain::Transaction;

    fn to_proto(&self) -> Result<Self::Proto, IngestionError> {
        let from = self
            .recover_signer()
            .change_context(IngestionError::Model)
            .attach_printable("failed to recover sender of EIP-1559 transaction")?;
        let tx = self.tx();

        Ok(beaconchain::Transaction {
            filter_ids: Vec::default(),
            transaction_type: models::TxType::Eip1559 as u64,
            transaction_index: u32::MAX,
            transaction_hash: self.hash().to_proto().into(),
            nonce: tx.nonce,
            from: from.to_proto().into(),
            to: tx.to.to_option(),
            value: tx.value.to_proto().into(),
            gas_price: None,
            gas_limit: tx.gas_limit.to_proto().into(),
            max_fee_per_gas: tx.max_fee_per_gas.to_proto().into(),
            max_priority_fee_per_gas: tx.max_priority_fee_per_gas.to_proto().into(),
            max_fee_per_blob_gas: None,
            input: tx.input.to_vec(),
            signature: self.signature().to_proto().into(),
            chain_id: tx.chain_id.into(),
            access_list: tx.access_list.iter().map(ModelExt::to_proto).collect(),
            blob_versioned_hashes: Vec::default(),
        })
    }
}

impl FallibleModelExt for models::Signed<models::TxEip4844Variant> {
    type Proto = beaconchain::Transaction;

    fn to_proto(&self) -> Result<Self::Proto, IngestionError> {
        let from = self
            .recover_signer()
            .change_context(IngestionError::Model)
            .attach_printable("failed to recover sender of EIP-4844 transaction")?;
        let tx = self.tx().tx();

        Ok(beaconchain::Transaction {
            filter_ids: Vec::default(),
            transaction_type: models::TxType::Eip4844 as u64,
            transaction_index: u32::MAX,
            transaction_hash: self.hash().to_proto().into(),
            nonce: tx.nonce,
            from: from.to_proto().into(),
            to: tx.to.to_proto().into(),
            value: tx.value.to_proto().into(),
            gas_price: None,
            gas_limit: tx.gas_limit.to_proto().into(),
            max_fee_per_gas: tx.max_fee_per_gas.to_proto().into(),
            max_priority_fee_per_gas: tx.max_priority_fee_per_gas.to_proto().into(),
            max_fee_per_blob_gas: tx.max_fee_per_blob_gas.to_proto().into(),
            input: tx.input.to_vec(),
            signature: self.signature().to_proto().into(),
            chain_id: tx.chain_id.into(),
            access_list: tx.access_list.iter().map(ModelExt::to_proto).collect(),
            blob_versioned_hashes: tx
                .blob_versioned_hashes
                .iter()
                .map(ModelExt::to_proto)
                .collect(),
        })
    }
}

impl ModelExt for models::Signature {
    type Proto = beaconchain::Signature;

    fn to_proto(&self) -> Self::Proto {
        beaconchain::Signature {
            r: self.r().to_proto().into(),
            s: self.s().to_proto().into(),
        }
    }
}

impl ModelExt for models::AccessListItem {
    type Proto = beaconchain::AccessListItem;

    fn to_proto(&self) -> Self::Proto {
        beaconchain::AccessListItem {
            address: self.address.to_proto().into(),
            storage_keys: self.storage_keys.iter().map(ModelExt::to_proto).collect(),
        }
    }
}

impl ModelExt for models::Validator {
    type Proto = beaconchain::Validator;

    fn to_proto(&self) -> Self::Proto {
        beaconchain::Validator {
            filter_ids: Vec::default(),
            validator_index: self.index,
            balance: self.balance,
            status: self.status.to_proto() as i32,
            pubkey: self.validator.pubkey.to_proto().into(),
            withdrawal_credentials: self.validator.withdrawal_credentials.to_proto().into(),
            effective_balance: self.validator.effective_balance,
            slashed: self.validator.slashed,
            activation_eligibility_epoch: self.validator.activation_eligibility_epoch,
            activation_epoch: self.validator.activation_epoch,
            exit_epoch: self.validator.exit_epoch,
            withdrawable_epoch: self.validator.withdrawable_epoch,
        }
    }
}

impl ModelExt for models::BlobSidecar {
    type Proto = beaconchain::Blob;

    fn to_proto(&self) -> Self::Proto {
        beaconchain::Blob {
            filter_ids: Vec::default(),
            blob_index: self.index,
            blob: self.blob.to_vec(),
            kzg_commitment: self.kzg_commitment.to_proto().into(),
            kzg_proof: self.kzg_proof.to_proto().into(),
            kzg_commitment_inclusion_proof: self
                .kzg_commitment_inclusion_proof
                .iter()
                .map(ModelExt::to_proto)
                .collect(),
            blob_hash: self.hash().to_proto().into(),
            transaction_index: u32::MAX,
            transaction_hash: None,
        }
    }
}

impl ModelExt for models::ValidatorStatus {
    type Proto = beaconchain::ValidatorStatus;

    fn to_proto(&self) -> Self::Proto {
        match self {
            models::ValidatorStatus::PendingInitialized => {
                beaconchain::ValidatorStatus::PendingInitialized
            }
            models::ValidatorStatus::PendingQueued => beaconchain::ValidatorStatus::PendingQueued,
            models::ValidatorStatus::ActiveOngoing => beaconchain::ValidatorStatus::ActiveOngoing,
            &models::ValidatorStatus::ActiveExiting => beaconchain::ValidatorStatus::ActiveExiting,
            models::ValidatorStatus::ActiveSlashed => beaconchain::ValidatorStatus::ActiveSlashed,
            models::ValidatorStatus::ExitedUnslashed => {
                beaconchain::ValidatorStatus::ExitedUnslashed
            }
            models::ValidatorStatus::ExitedSlashed => beaconchain::ValidatorStatus::ExitedSlashed,
            models::ValidatorStatus::WithdrawalPossible => {
                beaconchain::ValidatorStatus::WithdrawalPossible
            }
            models::ValidatorStatus::WithdrawalDone => beaconchain::ValidatorStatus::WithdrawalDone,
        }
    }
}

impl ModelExt for models::B256 {
    type Proto = beaconchain::B256;

    fn to_proto(&self) -> Self::Proto {
        beaconchain::B256::from_bytes(&self.0)
    }
}

impl ModelExt for models::U256 {
    type Proto = beaconchain::U256;

    fn to_proto(&self) -> Self::Proto {
        beaconchain::U256::from_bytes(&self.to_be_bytes())
    }
}

impl ModelExt for u128 {
    type Proto = beaconchain::U128;

    fn to_proto(&self) -> Self::Proto {
        beaconchain::U128::from_bytes(&self.to_be_bytes())
    }
}

impl ModelExt for models::B384 {
    type Proto = beaconchain::B384;

    fn to_proto(&self) -> Self::Proto {
        beaconchain::B384::from_bytes(&self.to_be_bytes())
    }
}

impl ModelExt for models::Address {
    type Proto = beaconchain::Address;

    fn to_proto(&self) -> Self::Proto {
        beaconchain::Address::from_bytes(&self.0)
    }
}

impl TxKindExt for models::TxKind {
    fn to_option(self) -> Option<beaconchain::Address> {
        match self {
            models::TxKind::Create => None,
            models::TxKind::Call(to) => to.to_proto().into(),
        }
    }
}
