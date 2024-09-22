use error_stack::{Result, ResultExt};
use rkyv::{Archive, Deserialize, Portable, Serialize};

use crate::rkyv::{Aligned, Checked, Serializable};

#[derive(Debug)]
pub struct FragmentError;

/// A fragment is a piece of block data.
pub trait Fragment: Archive {
    /// The unique tag of the fragment.
    fn tag() -> u8;
    /// The unique name of the fragment.
    fn name() -> &'static str;
}

#[derive(Archive, Serialize, Deserialize, Debug)]
pub struct SerializedFragment {
    pub tag: u8,
    #[rkyv(with = Aligned<16>)]
    pub data: Vec<u8>,
}

/// A block is a collection of fragments.
#[derive(Archive, Serialize, Deserialize, Debug, Default)]
pub struct Block {
    pub fragments: Vec<SerializedFragment>,
}

impl Block {
    /// Add a fragment to the block.
    pub fn add_fragment<F>(&mut self, fragment: F) -> Result<(), FragmentError>
    where
        F: Fragment + for<'a> Serializable<'a>,
    {
        let tag = F::tag();
        let bytes = rkyv::to_bytes(&fragment)
            .change_context(FragmentError)
            .attach_printable("failed to serialize fragment")
            .attach_printable_lazy(|| format!("fragment: {}", F::name()))?;

        self.fragments.push(SerializedFragment {
            tag,
            data: bytes.to_vec(),
        });

        Ok(())
    }
}

impl ArchivedBlock {
    /// Access a fragment from the block.
    pub fn access_fragment<F>(&self) -> Result<&<F as Archive>::Archived, FragmentError>
    where
        F: Fragment,
        F::Archived: Portable + for<'a> Checked<'a>,
    {
        let Some(serialized) = self.fragments.iter().find(|f| f.tag == F::tag()) else {
            return Err(FragmentError)
                .attach_printable("missing fragment")
                .attach_printable_lazy(|| format!("fragment: {}", F::name()));
        };

        rkyv::access::<rkyv::Archived<F>, rkyv::rancor::Error>(&serialized.data)
            .change_context(FragmentError)
            .attach_printable("failed to access fragment")
            .attach_printable_lazy(|| format!("fragment: {}", F::name()))
    }
}

impl error_stack::Context for FragmentError {}

impl std::fmt::Display for FragmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fragment error")
    }
}

#[cfg(test)]
mod tests {
    use rkyv::{Archive, Deserialize, Serialize};

    use super::{Block, Fragment};

    #[derive(Archive, Serialize, Deserialize, Debug)]
    pub struct Foo {
        pub name: String,
        pub value: u64,
    }

    #[derive(Archive, Serialize, Deserialize, Debug)]
    pub struct Bar(pub [u64; 3]);

    impl Fragment for Foo {
        fn name() -> &'static str {
            "foo"
        }

        fn tag() -> u8 {
            1
        }
    }

    impl Fragment for Bar {
        fn name() -> &'static str {
            "bar"
        }

        fn tag() -> u8 {
            2
        }
    }

    #[test]
    fn test_fragment() {
        let bytes = {
            let mut block = Block::default();

            block
                .add_fragment(Foo {
                    name: "foo".to_string(),
                    value: 42,
                })
                .unwrap();

            block.add_fragment(Bar([1, 2, 3])).unwrap();
            rkyv::to_bytes::<rkyv::rancor::Error>(&block).unwrap()
        };

        let block = rkyv::access::<rkyv::Archived<Block>, rkyv::rancor::Error>(&bytes).unwrap();
        let bar = block.access_fragment::<Bar>().unwrap();
        assert_eq!(bar.0, [1, 2, 3]);

        let foo = block.access_fragment::<Foo>().unwrap();
        assert_eq!(foo.name, "foo");
        assert_eq!(foo.value, 42);
    }
}
