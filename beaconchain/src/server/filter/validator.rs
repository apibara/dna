use apibara_dna_protocol::beaconchain;
use rkyv::with::With;
use roaring::RoaringBitmap;

use crate::{ingestion::models, segment::store};

pub struct ValidatorFilter {
    pub validator_index: Option<u32>,
    pub status: Option<models::ValidatorStatus>,
}

impl ValidatorFilter {
    pub fn fill_validator_bitmap(
        &self,
        validators_count: usize,
        index: &store::ValidatorsIndex,
        bitmap: &mut RoaringBitmap,
    ) -> Result<(), std::io::Error> {
        if self.validator_index.is_none() && self.status.is_none() {
            bitmap.insert_range(0..validators_count as u32);
            return Ok(());
        }

        if let Some(validator_index) = self.validator_index {
            if validators_count > validator_index as usize {
                bitmap.insert(validator_index);
            }
        }

        if let Some(status) = self.status {
            let status_bitmap = index
                .validator_index_by_status
                .get(&status)
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid status")
                })?;

            let status_bitmap = RoaringBitmap::try_from(status_bitmap)?;
            *bitmap |= status_bitmap;
        }

        Ok(())
    }
}

impl From<beaconchain::ValidatorFilter> for ValidatorFilter {
    fn from(x: beaconchain::ValidatorFilter) -> Self {
        Self {
            validator_index: x.validator_index,
            status: x.status.and_then(convert_validator_status),
        }
    }
}

fn convert_validator_status(status: i32) -> Option<models::ValidatorStatus> {
    use beaconchain::ValidatorStatus::*;
    let Some(status) = beaconchain::ValidatorStatus::from_i32(status) else {
        return None;
    };
    match status {
        Unknown => None,
        PendingInitialized => models::ValidatorStatus::PendingInitialized.into(),
        PendingQueued => models::ValidatorStatus::PendingQueued.into(),
        ActiveOngoing => models::ValidatorStatus::ActiveOngoing.into(),
        ActiveExiting => models::ValidatorStatus::ActiveExiting.into(),
        ActiveSlashed => models::ValidatorStatus::ActiveSlashed.into(),
        ExitedSlashed => models::ValidatorStatus::ExitedSlashed.into(),
        ExitedUnslashed => models::ValidatorStatus::ExitedUnslashed.into(),
        WithdrawalPossible => models::ValidatorStatus::WithdrawalPossible.into(),
        WithdrawalDone => models::ValidatorStatus::WithdrawalDone.into(),
    }
}
