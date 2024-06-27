use apibara_dna_protocol::beaconchain;

use crate::{ingestion::models, segment::store};

impl From<store::BlockHeader> for beaconchain::BlockHeader {
    fn from(x: store::BlockHeader) -> Self {
        let parent_root = x.parent_root.into();
        let state_root = x.state_root.into();
        let deposit_root = x.deposit_root.into();
        let block_hash = x.block_hash.into();
        let graffiti = x.graffiti.into();
        let execution_payload = x.execution_payload.map(Into::into);
        let blob_kzg_commitments = x.blob_kzg_commitments.into_iter().map(Into::into).collect();

        Self {
            slot: x.slot,
            proposer_index: x.proposer_index,
            parent_root: Some(parent_root),
            state_root: Some(state_root),
            randao_reveal: x.randao_reveal.0,
            deposit_count: x.deposit_count,
            deposit_root: Some(deposit_root),
            block_hash: Some(block_hash),
            graffiti: Some(graffiti),
            execution_payload,
            blob_kzg_commitments,
        }
    }
}

impl From<store::ExecutionPayload> for beaconchain::ExecutionPayload {
    fn from(x: store::ExecutionPayload) -> Self {
        let parent_hash = x.parent_hash.into();
        let fee_recipient = x.fee_recipient.into();
        let state_root = x.state_root.into();
        let receipts_root = x.receipts_root.into();
        let prev_randao = x.prev_randao.into();
        let timestamp = prost_types::Timestamp {
            seconds: x.timestamp as i64,
            nanos: 0,
        };

        Self {
            parent_hash: Some(parent_hash),
            fee_recipient: Some(fee_recipient),
            state_root: Some(state_root),
            receipts_root: Some(receipts_root),
            logs_bloom: x.logs_bloom.0,
            prev_randao: Some(prev_randao),
            block_number: x.block_number,
            timestamp: Some(timestamp),
        }
    }
}

impl From<store::Transaction> for beaconchain::Transaction {
    fn from(x: store::Transaction) -> Self {
        let transaction_hash = x.transaction_hash.into();
        let from = x.from.into();
        let to = x.to.map(Into::into);
        let value = x.value.into();
        let gas_price = x.gas_price.map(Into::into);
        let gas_limit = x.gas_limit.into();
        let max_fee_per_gas = x.max_fee_per_gas.map(Into::into);
        let max_priority_fee_per_gas = x.max_priority_fee_per_gas.map(Into::into);
        let signature = x.signature.into();
        let access_list = x.access_list.into_iter().map(Into::into).collect();
        let max_fee_per_blob_gas = x.max_fee_per_blob_gas.map(Into::into);
        let blob_versioned_hashes = x
            .blob_versioned_hashes
            .into_iter()
            .map(Into::into)
            .collect();

        Self {
            transaction_hash: Some(transaction_hash),
            nonce: x.nonce,
            transaction_index: x.transaction_index,
            from: Some(from),
            to,
            value: Some(value),
            gas_price,
            gas_limit: Some(gas_limit),
            max_fee_per_gas,
            max_priority_fee_per_gas,
            input: x.input.0,
            signature: Some(signature),
            chain_id: x.chain_id,
            access_list,
            transaction_type: x.transaction_type,
            max_fee_per_blob_gas,
            blob_versioned_hashes,
        }
    }
}

impl From<store::Validator> for beaconchain::Validator {
    fn from(x: store::Validator) -> Self {
        let pubkey = x.pubkey.into();
        let withdrawal_credentials = x.withdrawal_credentials.into();
        let status = beaconchain::ValidatorStatus::from(x.status);

        Self {
            validator_index: x.validator_index,
            balance: x.balance,
            status: status as i32,
            pubkey: Some(pubkey),
            withdrawal_credentials: Some(withdrawal_credentials),
            effective_balance: x.effective_balance,
            slashed: x.slashed,
            activation_eligibility_epoch: x.activation_eligibility_epoch,
            activation_epoch: x.activation_epoch,
            exit_epoch: x.exit_epoch,
            withdrawable_epoch: x.withdrawable_epoch,
        }
    }
}

impl From<store::Blob> for beaconchain::Blob {
    fn from(x: store::Blob) -> Self {
        let kzg_commitment = x.kzg_commitment.into();
        let kzg_proof = x.kzg_proof.into();
        let kzg_commitment_inclusion_proof = x
            .kzg_commitment_inclusion_proof
            .into_iter()
            .map(Into::into)
            .collect();
        let blob_hash = x.blob_hash.into();
        let transaction_hash = x.transaction_hash.into();

        Self {
            blob_index: x.blob_index,
            blob: x.blob.0,
            kzg_commitment: Some(kzg_commitment),
            kzg_proof: Some(kzg_proof),
            kzg_commitment_inclusion_proof,
            blob_hash: Some(blob_hash),
            transaction_index: x.transaction_index,
            transaction_hash: Some(transaction_hash),
        }
    }
}

impl From<store::Signature> for beaconchain::Signature {
    fn from(x: store::Signature) -> Self {
        let r = x.r.into();
        let s = x.s.into();
        Self {
            r: Some(r),
            s: Some(s),
        }
    }
}

impl From<store::AccessListItem> for beaconchain::AccessListItem {
    fn from(x: store::AccessListItem) -> Self {
        let address = x.address.into();
        let storage_keys = x.storage_keys.into_iter().map(Into::into).collect();

        Self {
            address: Some(address),
            storage_keys,
        }
    }
}

impl From<&store::B256> for beaconchain::B256 {
    fn from(x: &store::B256) -> Self {
        beaconchain::B256::from_bytes(&x.0)
    }
}

impl From<store::B256> for beaconchain::B256 {
    fn from(x: store::B256) -> Self {
        beaconchain::B256::from_bytes(&x.0)
    }
}

impl From<store::U256> for beaconchain::U256 {
    fn from(x: store::U256) -> Self {
        beaconchain::U256::from_bytes(&x.0)
    }
}

impl From<store::B384> for beaconchain::B384 {
    fn from(x: store::B384) -> Self {
        beaconchain::B384::from_bytes(&x.0)
    }
}

impl From<store::Address> for beaconchain::Address {
    fn from(x: store::Address) -> Self {
        beaconchain::Address::from_bytes(&x.0)
    }
}

impl From<beaconchain::Address> for store::Address {
    fn from(x: beaconchain::Address) -> Self {
        store::Address(x.to_bytes())
    }
}

impl From<models::ValidatorStatus> for beaconchain::ValidatorStatus {
    fn from(x: models::ValidatorStatus) -> Self {
        use models::ValidatorStatus::*;
        match x {
            PendingInitialized => beaconchain::ValidatorStatus::PendingInitialized,
            PendingQueued => beaconchain::ValidatorStatus::PendingQueued,
            ActiveOngoing => beaconchain::ValidatorStatus::ActiveOngoing,
            ActiveExiting => beaconchain::ValidatorStatus::ActiveExiting,
            ActiveSlashed => beaconchain::ValidatorStatus::ActiveSlashed,
            ExitedSlashed => beaconchain::ValidatorStatus::ExitedSlashed,
            ExitedUnslashed => beaconchain::ValidatorStatus::ExitedUnslashed,
            WithdrawalPossible => beaconchain::ValidatorStatus::WithdrawalPossible,
            WithdrawalDone => beaconchain::ValidatorStatus::WithdrawalDone,
        }
    }
}
