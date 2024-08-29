use apibara_dna_common::store::index::{ArchivedIndexGroup, TaggedIndex};
use error_stack::{Result, ResultExt};
use rkyv::validation::validators::DefaultValidator;
use tracing::info;

use crate::error::BeaconChainError;

pub fn print_index<'a, TI: TaggedIndex>(
    data: &'a ArchivedIndexGroup,
) -> Result<(), BeaconChainError>
where
    TI::Key: 'a + std::fmt::Debug,
    <TI::Key as rkyv::Archive>::Archived:
        rkyv::CheckBytes<DefaultValidator<'a>> + rkyv::Deserialize<TI::Key, rkyv::Infallible>,
{
    info!("  {} ({})", TI::name(), TI::tag());
    let Some(index) = data.get_index::<TI>().change_context(BeaconChainError)? else {
        info!("    missing");
        return Ok(());
    };

    for (key, bitmap) in index.iter() {
        let bitmap = bitmap.deserialize().change_context(BeaconChainError)?;
        info!("    {:?} -> {:?}", key, bitmap);
    }

    Ok(())
}
