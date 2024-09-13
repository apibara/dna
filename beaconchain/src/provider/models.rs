use apibara_dna_common::{Cursor, GetCursor, Hash};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DefaultOnNull, DisplayFromStr};

pub use alloy_consensus::{
    Signed, TxEip1559, TxEip2930, TxEip4844, TxEip4844Variant, TxEip4844WithSidecar, TxEnvelope,
    TxLegacy,
};
pub use alloy_eips::eip2930::AccessListItem;
pub use alloy_primitives::{ruint::aliases::B384, Address, Bytes, Signature, TxKind, B256, U256};
pub use alloy_rpc_types_beacon::header::HeaderResponse;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRootResponse {
    pub data: BlockRoot,
    pub finalized: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockRoot {
    pub root: B256,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeaconBlockResponse {
    pub finalized: bool,
    pub data: BeaconBlockData,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeaconBlockData {
    pub message: BeaconBlock,
    pub signature: Bytes,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeaconBlock {
    #[serde_as(as = "DisplayFromStr")]
    pub slot: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub proposer_index: u32,
    pub parent_root: B256,
    pub state_root: B256,
    pub body: BeaconBlockBody,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeaconBlockBody {
    pub randao_reveal: Bytes,
    pub eth1_data: Eth1Data,
    pub graffiti: B256,
    pub execution_payload: Option<ExecutionPayload>,
    #[serde(default)]
    pub blob_kzg_commitments: Vec<B384>,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Eth1Data {
    #[serde_as(as = "DisplayFromStr")]
    pub deposit_count: u64,
    pub deposit_root: B256,
    pub block_hash: B256,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionPayload {
    pub parent_hash: B256,
    pub fee_recipient: Address,
    pub state_root: B256,
    pub receipts_root: B256,
    pub logs_bloom: Bytes,
    pub prev_randao: B256,
    #[serde_as(as = "DisplayFromStr")]
    pub block_number: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub timestamp: u64,
    #[serde_as(deserialize_as = "DefaultOnNull")]
    #[serde(default)]
    pub transactions: Vec<Bytes>,
    #[serde_as(deserialize_as = "DefaultOnNull")]
    #[serde(default)]
    pub withdrawals: Vec<Withdrawal>,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Withdrawal {
    #[serde_as(as = "DisplayFromStr")]
    pub index: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub validator_index: u32,
    pub address: Address,
    #[serde_as(as = "DisplayFromStr")]
    pub amount: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobSidecarResponse {
    pub data: Vec<BlobSidecar>,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobSidecar {
    #[serde_as(as = "DisplayFromStr")]
    pub index: u32,
    pub blob: Bytes,
    pub kzg_commitment: B384,
    pub kzg_proof: B384,
    pub kzg_commitment_inclusion_proof: Vec<B256>,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatorInfo {
    pub pubkey: B384,
    pub withdrawal_credentials: B256,
    #[serde_as(as = "DisplayFromStr")]
    pub effective_balance: u64,
    pub slashed: bool,
    #[serde_as(as = "DisplayFromStr")]
    pub activation_eligibility_epoch: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub activation_epoch: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub exit_epoch: u64,
    #[serde_as(as = "DisplayFromStr")]
    pub withdrawable_epoch: u64,
}

#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Validator {
    #[serde_as(as = "DisplayFromStr")]
    pub index: u32,
    #[serde_as(as = "DisplayFromStr")]
    pub balance: u64,
    pub validator: ValidatorInfo,
    pub status: ValidatorStatus,
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    Hash,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
#[serde(rename_all = "snake_case")]
pub enum ValidatorStatus {
    PendingInitialized,
    PendingQueued,
    ActiveOngoing,
    ActiveExiting,
    ActiveSlashed,
    ExitedUnslashed,
    ExitedSlashed,
    WithdrawalPossible,
    WithdrawalDone,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidatorsResponse {
    pub data: Vec<Validator>,
}

impl BlobSidecar {
    pub fn hash(&self) -> B256 {
        super::utils::kzg_commitment_to_versioned_hash(&self.kzg_commitment)
    }
}

impl GetCursor for BeaconBlock {
    fn cursor(&self) -> Option<Cursor> {
        let hash = Hash(self.state_root.0.to_vec());
        Some(Cursor::new(self.slot, hash))
    }
}

pub trait BeaconCursorExt {
    fn cursor(&self) -> Cursor;
}

impl BeaconCursorExt for HeaderResponse {
    fn cursor(&self) -> Cursor {
        let hash = Hash(self.data.root.0.to_vec());
        Cursor::new(self.data.header.message.slot, hash)
    }
}
