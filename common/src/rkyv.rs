//! rkyv helpers.

use rkyv::{
    api::high::{HighSerializer, HighValidator},
    rancor::Fallible,
    ser::{allocator::ArenaHandle, Writer, WriterExt},
    util::AlignedVec,
    vec::{ArchivedVec, VecResolver},
    with::{ArchiveWith, DeserializeWith, SerializeWith},
    Archive, Deserialize, Place, Serialize,
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

/// Wrapper type to align `Vec<u8>` fields.
pub struct Aligned<const N: usize>;

impl<const N: usize> ArchiveWith<Vec<u8>> for Aligned<N> {
    type Archived = ArchivedVec<u8>;
    type Resolver = VecResolver;

    fn resolve_with(field: &Vec<u8>, resolver: Self::Resolver, out: Place<Self::Archived>) {
        field.resolve(resolver, out);
    }
}

impl<S, const N: usize> SerializeWith<Vec<u8>, S> for Aligned<N>
where
    S: Fallible + Writer + ?Sized,
    Vec<u8>: Serialize<S>,
{
    fn serialize_with(field: &Vec<u8>, serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        serializer.align(N)?;
        field.serialize(serializer)
    }
}

impl<D, const N: usize> DeserializeWith<ArchivedVec<u8>, Vec<u8>, D> for Aligned<N>
where
    D: Fallible + ?Sized,
    ArchivedVec<u8>: Deserialize<Vec<u8>, D>,
{
    fn deserialize_with(
        field: &ArchivedVec<u8>,
        deserializer: &mut D,
    ) -> Result<Vec<u8>, D::Error> {
        field.deserialize(deserializer)
    }
}
