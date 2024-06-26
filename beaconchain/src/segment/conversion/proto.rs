use apibara_dna_protocol::beaconchain;

use crate::segment::store;

impl From<store::BlockHeader> for beaconchain::BlockHeader {
    fn from(x: store::BlockHeader) -> Self {
        let parent_root = x.parent_root.into();
        let state_root = x.state_root.into();
        let deposit_root = x.deposit_root.into();
        let block_hash = x.block_hash.into();
        let graffiti = x.graffiti.into();
        let execution_payload = x.execution_payload.into();
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
            execution_payload: Some(execution_payload),
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
