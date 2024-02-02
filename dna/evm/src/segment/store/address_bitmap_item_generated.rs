// automatically generated by the FlatBuffers compiler, do not modify
// @generated
extern crate alloc;
extern crate flatbuffers;
use self::flatbuffers::{EndianScalar, Follow};
use super::*;
use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use core::cmp::Ordering;
use core::mem;
pub enum AddressBitmapItemOffset {}
#[derive(Copy, Clone, PartialEq)]

pub struct AddressBitmapItem<'a> {
    pub _tab: flatbuffers::Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for AddressBitmapItem<'a> {
    type Inner = AddressBitmapItem<'a>;
    #[inline]
    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: flatbuffers::Table::new(buf, loc),
        }
    }
}

impl<'a> AddressBitmapItem<'a> {
    pub const VT_KEY: flatbuffers::VOffsetT = 4;
    pub const VT_BITMAP: flatbuffers::VOffsetT = 6;

    pub const fn get_fully_qualified_name() -> &'static str {
        "AddressBitmapItem"
    }

    #[inline]
    pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
        AddressBitmapItem { _tab: table }
    }
    #[allow(unused_mut)]
    pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
        _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
        args: &'args AddressBitmapItemArgs<'args>,
    ) -> flatbuffers::WIPOffset<AddressBitmapItem<'bldr>> {
        let mut builder = AddressBitmapItemBuilder::new(_fbb);
        if let Some(x) = args.bitmap {
            builder.add_bitmap(x);
        }
        if let Some(x) = args.key {
            builder.add_key(x);
        }
        builder.finish()
    }

    #[inline]
    pub fn key(&self) -> Option<&'a Address> {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe { self._tab.get::<Address>(AddressBitmapItem::VT_KEY, None) }
    }
    #[inline]
    pub fn bitmap(&self) -> Option<flatbuffers::Vector<'a, u8>> {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe {
            self._tab
                .get::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'a, u8>>>(
                    AddressBitmapItem::VT_BITMAP,
                    None,
                )
        }
    }
}

impl flatbuffers::Verifiable for AddressBitmapItem<'_> {
    #[inline]
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        use self::flatbuffers::Verifiable;
        v.visit_table(pos)?
            .visit_field::<Address>("key", Self::VT_KEY, false)?
            .visit_field::<flatbuffers::ForwardsUOffset<flatbuffers::Vector<'_, u8>>>(
                "bitmap",
                Self::VT_BITMAP,
                false,
            )?
            .finish();
        Ok(())
    }
}
pub struct AddressBitmapItemArgs<'a> {
    pub key: Option<&'a Address>,
    pub bitmap: Option<flatbuffers::WIPOffset<flatbuffers::Vector<'a, u8>>>,
}
impl<'a> Default for AddressBitmapItemArgs<'a> {
    #[inline]
    fn default() -> Self {
        AddressBitmapItemArgs {
            key: None,
            bitmap: None,
        }
    }
}

pub struct AddressBitmapItemBuilder<'a: 'b, 'b> {
    fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
    start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}
impl<'a: 'b, 'b> AddressBitmapItemBuilder<'a, 'b> {
    #[inline]
    pub fn add_key(&mut self, key: &Address) {
        self.fbb_
            .push_slot_always::<&Address>(AddressBitmapItem::VT_KEY, key);
    }
    #[inline]
    pub fn add_bitmap(&mut self, bitmap: flatbuffers::WIPOffset<flatbuffers::Vector<'b, u8>>) {
        self.fbb_
            .push_slot_always::<flatbuffers::WIPOffset<_>>(AddressBitmapItem::VT_BITMAP, bitmap);
    }
    #[inline]
    pub fn new(
        _fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>,
    ) -> AddressBitmapItemBuilder<'a, 'b> {
        let start = _fbb.start_table();
        AddressBitmapItemBuilder {
            fbb_: _fbb,
            start_: start,
        }
    }
    #[inline]
    pub fn finish(self) -> flatbuffers::WIPOffset<AddressBitmapItem<'a>> {
        let o = self.fbb_.end_table(self.start_);
        flatbuffers::WIPOffset::new(o.value())
    }
}

impl core::fmt::Debug for AddressBitmapItem<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let mut ds = f.debug_struct("AddressBitmapItem");
        ds.field("key", &self.key());
        ds.field("bitmap", &self.bitmap());
        ds.finish()
    }
}