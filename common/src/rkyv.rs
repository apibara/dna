//! rkyv helpers.

use rkyv::{
    api::high::{HighSerializer, HighValidator},
    ser::allocator::ArenaHandle,
    util::AlignedVec,
};

pub trait Serializable<'a>:
    rkyv::Serialize<HighSerializer<'a, AlignedVec, ArenaHandle<'a>, rkyv::rancor::Error>>
{
}

pub trait Checked<'a>: rkyv::bytecheck::CheckBytes<HighValidator<'a, rkyv::rancor::Error>> {}

impl<'a, T> Serializable<'a> for T where
    T: rkyv::Serialize<HighSerializer<'a, AlignedVec, ArenaHandle<'a>, rkyv::rancor::Error>>
{
}

impl<'a, T> Checked<'a> for T where
    T: rkyv::bytecheck::CheckBytes<HighValidator<'a, rkyv::rancor::Error>>
{
}
