use apibara_dna_protocol::beaconchain;
use rkyv::option::ArchivedOption;

use crate::{
    provider::models::{ArchivedValidatorStatus, ValidatorStatus},
    store::fragment,
};

impl From<beaconchain::Address> for fragment::Address {
    fn from(v: beaconchain::Address) -> Self {
        fragment::Address(v.to_bytes())
    }
}

impl TryFrom<i32> for ValidatorStatus {
    type Error = prost::UnknownEnumValue;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match beaconchain::ValidatorStatus::try_from(value)? {
            beaconchain::ValidatorStatus::Unknown => Err(prost::UnknownEnumValue(value)),
            beaconchain::ValidatorStatus::PendingInitialized => {
                Ok(ValidatorStatus::PendingInitialized)
            }
            beaconchain::ValidatorStatus::PendingQueued => Ok(ValidatorStatus::PendingQueued),
            beaconchain::ValidatorStatus::ActiveOngoing => Ok(ValidatorStatus::ActiveOngoing),
            beaconchain::ValidatorStatus::ActiveExiting => Ok(ValidatorStatus::ActiveExiting),
            beaconchain::ValidatorStatus::ActiveSlashed => Ok(ValidatorStatus::ActiveSlashed),
            beaconchain::ValidatorStatus::ExitedUnslashed => Ok(ValidatorStatus::ExitedUnslashed),
            beaconchain::ValidatorStatus::ExitedSlashed => Ok(ValidatorStatus::ExitedSlashed),
            beaconchain::ValidatorStatus::WithdrawalPossible => {
                Ok(ValidatorStatus::WithdrawalPossible)
            }
            beaconchain::ValidatorStatus::WithdrawalDone => Ok(ValidatorStatus::WithdrawalDone),
        }
    }
}

impl From<&fragment::ArchivedBlockHeader> for beaconchain::BlockHeader {
    fn from(v: &fragment::ArchivedBlockHeader) -> Self {
        let blob_kzg_commitments = v.blob_kzg_commitments.iter().map(Into::into).collect();

        beaconchain::BlockHeader {
            slot: v.slot.to_native(),
            proposer_index: v.proposer_index.to_native(),
            parent_root: (&v.parent_root).into(),
            state_root: (&v.state_root).into(),
            randao_reveal: v.randao_reveal.0.to_vec(),
            deposit_count: v.deposit_count.to_native(),
            deposit_root: (&v.deposit_root).into(),
            block_hash: (&v.block_hash).into(),
            graffiti: (&v.graffiti).into(),
            execution_payload: v.execution_payload.as_ref().map(Into::into),
            blob_kzg_commitments,
        }
    }
}

impl From<&fragment::ArchivedTransaction> for beaconchain::Transaction {
    fn from(v: &fragment::ArchivedTransaction) -> Self {
        let chain_id = match v.chain_id {
            ArchivedOption::None => None,
            ArchivedOption::Some(chain_id) => Some(chain_id.to_native()),
        };

        let access_list = v.access_list.iter().map(Into::into).collect();
        let blob_versioned_hashes = v.blob_versioned_hashes.iter().map(Into::into).collect();

        beaconchain::Transaction {
            filter_ids: Vec::new(),
            nonce: v.nonce.to_native(),
            transaction_hash: (&v.transaction_hash).into(),
            transaction_index: v.transaction_index.to_native(),
            from: (&v.from).into(),
            to: v.to.as_ref().map(Into::into),
            value: (&v.value).into(),
            gas_price: v.gas_price.as_ref().map(|g| g.to_native().into()),
            gas_limit: Some(v.gas_limit.to_native().into()),
            max_fee_per_gas: v.max_fee_per_gas.as_ref().map(|g| g.to_native().into()),
            max_priority_fee_per_gas: v
                .max_priority_fee_per_gas
                .as_ref()
                .map(|g| g.to_native().into()),
            input: v.input.0.to_vec(),
            signature: (&v.signature).into(),
            chain_id,
            access_list,
            transaction_type: v.transaction_type.to_native(),
            max_fee_per_blob_gas: v.max_fee_per_gas.as_ref().map(|g| g.to_native().into()),
            blob_versioned_hashes,
        }
    }
}

impl From<&fragment::ArchivedAccessListItem> for beaconchain::AccessListItem {
    fn from(v: &fragment::ArchivedAccessListItem) -> Self {
        let storage_keys = v.storage_keys.iter().map(Into::into).collect();
        beaconchain::AccessListItem {
            address: (&v.address).into(),
            storage_keys,
        }
    }
}

impl From<&fragment::ArchivedSignature> for beaconchain::Signature {
    fn from(v: &fragment::ArchivedSignature) -> Self {
        beaconchain::Signature {
            r: (&v.r).into(),
            s: (&v.s).into(),
        }
    }
}

impl From<&fragment::ArchivedSignature> for Option<beaconchain::Signature> {
    fn from(v: &fragment::ArchivedSignature) -> Self {
        Some(v.into())
    }
}

impl From<&fragment::ArchivedValidator> for beaconchain::Validator {
    fn from(v: &fragment::ArchivedValidator) -> Self {
        beaconchain::Validator {
            filter_ids: Vec::new(),
            validator_index: v.validator_index.to_native(),
            balance: v.balance.to_native(),
            status: (&v.status).into(),
            pubkey: (&v.pubkey).into(),
            withdrawal_credentials: (&v.withdrawal_credentials).into(),
            effective_balance: v.effective_balance.to_native(),
            slashed: v.slashed,
            activation_eligibility_epoch: v.activation_eligibility_epoch.to_native(),
            activation_epoch: v.activation_epoch.to_native(),
            exit_epoch: v.exit_epoch.to_native(),
            withdrawable_epoch: v.withdrawable_epoch.to_native(),
        }
    }
}

impl From<&fragment::ArchivedBlob> for beaconchain::Blob {
    fn from(v: &fragment::ArchivedBlob) -> Self {
        beaconchain::Blob {
            filter_ids: Vec::new(),
            blob_index: v.blob_index.to_native(),
            blob: v.blob.0.to_vec(),
            kzg_commitment: (&v.kzg_commitment).into(),
            kzg_proof: (&v.kzg_proof).into(),
            kzg_commitment_inclusion_proof: v
                .kzg_commitment_inclusion_proof
                .iter()
                .map(Into::into)
                .collect(),
            blob_hash: (&v.blob_hash).into(),
            transaction_index: v.transaction_index.to_native(),
            transaction_hash: (&v.transaction_hash).into(),
        }
    }
}

impl From<&fragment::ArchivedExecutionPayload> for beaconchain::ExecutionPayload {
    fn from(v: &fragment::ArchivedExecutionPayload) -> Self {
        let timestamp = prost_types::Timestamp {
            seconds: v.timestamp.to_native() as i64,
            nanos: 0,
        };

        beaconchain::ExecutionPayload {
            parent_hash: (&v.parent_hash).into(),
            fee_recipient: (&v.fee_recipient).into(),
            state_root: (&v.state_root).into(),
            receipts_root: (&v.receipts_root).into(),
            logs_bloom: v.logs_bloom.0.to_vec(),
            prev_randao: (&v.prev_randao).into(),
            block_number: v.block_number.to_native(),
            timestamp: timestamp.into(),
        }
    }
}

impl From<&fragment::ArchivedB256> for beaconchain::B256 {
    fn from(v: &fragment::ArchivedB256) -> Self {
        beaconchain::B256::from_bytes(&v.0)
    }
}

impl From<&fragment::ArchivedB256> for Option<beaconchain::B256> {
    fn from(v: &fragment::ArchivedB256) -> Self {
        Some(v.into())
    }
}

impl From<&fragment::ArchivedU256> for beaconchain::U256 {
    fn from(v: &fragment::ArchivedU256) -> Self {
        beaconchain::U256::from_bytes(&v.0)
    }
}

impl From<&fragment::ArchivedU256> for Option<beaconchain::U256> {
    fn from(v: &fragment::ArchivedU256) -> Self {
        Some(v.into())
    }
}

impl From<&fragment::ArchivedB384> for beaconchain::B384 {
    fn from(v: &fragment::ArchivedB384) -> Self {
        beaconchain::B384::from_bytes(&v.0)
    }
}

impl From<&fragment::ArchivedB384> for Option<beaconchain::B384> {
    fn from(v: &fragment::ArchivedB384) -> Self {
        Some(v.into())
    }
}

impl From<&fragment::ArchivedAddress> for beaconchain::Address {
    fn from(v: &fragment::ArchivedAddress) -> Self {
        beaconchain::Address::from_bytes(&v.0)
    }
}

impl From<&fragment::ArchivedAddress> for Option<beaconchain::Address> {
    fn from(v: &fragment::ArchivedAddress) -> Self {
        Some(v.into())
    }
}

impl From<&ArchivedValidatorStatus> for beaconchain::ValidatorStatus {
    fn from(v: &ArchivedValidatorStatus) -> Self {
        use ArchivedValidatorStatus::*;
        match v {
            PendingInitialized => beaconchain::ValidatorStatus::PendingInitialized,
            PendingQueued => beaconchain::ValidatorStatus::PendingQueued,
            ActiveOngoing => beaconchain::ValidatorStatus::ActiveOngoing,
            ActiveExiting => beaconchain::ValidatorStatus::ActiveExiting,
            ActiveSlashed => beaconchain::ValidatorStatus::ActiveSlashed,
            ExitedUnslashed => beaconchain::ValidatorStatus::ExitedUnslashed,
            ExitedSlashed => beaconchain::ValidatorStatus::ExitedSlashed,
            WithdrawalPossible => beaconchain::ValidatorStatus::WithdrawalPossible,
            WithdrawalDone => beaconchain::ValidatorStatus::WithdrawalDone,
        }
    }
}

impl From<&ArchivedValidatorStatus> for i32 {
    fn from(v: &ArchivedValidatorStatus) -> Self {
        let status: beaconchain::ValidatorStatus = v.into();
        status as i32
    }
}
