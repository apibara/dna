use apibara_dna_common::{
    rkyv::Checked,
    store::{
        bitmap::BitmapMap,
        index::{ArchivedIndexGroup, TaggedIndex},
    },
};
use error_stack::{Result, ResultExt};
use rkyv::{de::Pool, rancor::Strategy, Archive, Deserialize};
use tracing::info;

use crate::error::BeaconChainError;

pub fn print_index<'a, TI: TaggedIndex>(
    data: &'a ArchivedIndexGroup,
) -> Result<(), BeaconChainError>
where
    TI::Key: 'a + std::fmt::Debug,
    <TI::Key as rkyv::Archive>::Archived: for<'b> Checked<'b>,
    <BitmapMap<TI::Key> as Archive>::Archived:
        Deserialize<BitmapMap<TI::Key>, Strategy<Pool, rkyv::rancor::Error>>,
{
    let mut deserializer = Pool::default();
    info!("  {} ({})", TI::name(), TI::tag());
    let Some(index) = data
        .deserialize_index::<TI, _>(&mut deserializer)
        .change_context(BeaconChainError)?
    else {
        info!("    missing");
        return Ok(());
    };

    for (key, bitmap) in index.iter() {
        let bitmap = bitmap.deserialize().change_context(BeaconChainError)?;
        info!("    {:?} -> {:?}", key, bitmap);
    }

    Ok(())
}
