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
pub enum LogSegmentOffset {}
#[derive(Copy, Clone, PartialEq)]

pub struct LogSegment<'a> {
    pub _tab: flatbuffers::Table<'a>,
}

impl<'a> flatbuffers::Follow<'a> for LogSegment<'a> {
    type Inner = LogSegment<'a>;
    #[inline]
    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        Self {
            _tab: flatbuffers::Table::new(buf, loc),
        }
    }
}

impl<'a> LogSegment<'a> {
    pub const VT_FIRST_BLOCK_NUMBER: flatbuffers::VOffsetT = 4;
    pub const VT_BLOCKS: flatbuffers::VOffsetT = 6;

    pub const fn get_fully_qualified_name() -> &'static str {
        "LogSegment"
    }

    #[inline]
    pub unsafe fn init_from_table(table: flatbuffers::Table<'a>) -> Self {
        LogSegment { _tab: table }
    }
    #[allow(unused_mut)]
    pub fn create<'bldr: 'args, 'args: 'mut_bldr, 'mut_bldr>(
        _fbb: &'mut_bldr mut flatbuffers::FlatBufferBuilder<'bldr>,
        args: &'args LogSegmentArgs<'args>,
    ) -> flatbuffers::WIPOffset<LogSegment<'bldr>> {
        let mut builder = LogSegmentBuilder::new(_fbb);
        builder.add_first_block_number(args.first_block_number);
        if let Some(x) = args.blocks {
            builder.add_blocks(x);
        }
        builder.finish()
    }

    #[inline]
    pub fn first_block_number(&self) -> u64 {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe {
            self._tab
                .get::<u64>(LogSegment::VT_FIRST_BLOCK_NUMBER, Some(0))
                .unwrap()
        }
    }
    #[inline]
    pub fn blocks(
        &self,
    ) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<BlockLogs<'a>>>> {
        // Safety:
        // Created from valid Table for this object
        // which contains a valid value in this slot
        unsafe {
            self._tab.get::<flatbuffers::ForwardsUOffset<
                flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<BlockLogs>>,
            >>(LogSegment::VT_BLOCKS, None)
        }
    }
}

impl flatbuffers::Verifiable for LogSegment<'_> {
    #[inline]
    fn run_verifier(
        v: &mut flatbuffers::Verifier,
        pos: usize,
    ) -> Result<(), flatbuffers::InvalidFlatbuffer> {
        use self::flatbuffers::Verifiable;
        v.visit_table(pos)?
            .visit_field::<u64>("first_block_number", Self::VT_FIRST_BLOCK_NUMBER, false)?
            .visit_field::<flatbuffers::ForwardsUOffset<
                flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<BlockLogs>>,
            >>("blocks", Self::VT_BLOCKS, false)?
            .finish();
        Ok(())
    }
}
pub struct LogSegmentArgs<'a> {
    pub first_block_number: u64,
    pub blocks: Option<
        flatbuffers::WIPOffset<
            flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<BlockLogs<'a>>>,
        >,
    >,
}
impl<'a> Default for LogSegmentArgs<'a> {
    #[inline]
    fn default() -> Self {
        LogSegmentArgs {
            first_block_number: 0,
            blocks: None,
        }
    }
}

pub struct LogSegmentBuilder<'a: 'b, 'b> {
    fbb_: &'b mut flatbuffers::FlatBufferBuilder<'a>,
    start_: flatbuffers::WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}
impl<'a: 'b, 'b> LogSegmentBuilder<'a, 'b> {
    #[inline]
    pub fn add_first_block_number(&mut self, first_block_number: u64) {
        self.fbb_
            .push_slot::<u64>(LogSegment::VT_FIRST_BLOCK_NUMBER, first_block_number, 0);
    }
    #[inline]
    pub fn add_blocks(
        &mut self,
        blocks: flatbuffers::WIPOffset<
            flatbuffers::Vector<'b, flatbuffers::ForwardsUOffset<BlockLogs<'b>>>,
        >,
    ) {
        self.fbb_
            .push_slot_always::<flatbuffers::WIPOffset<_>>(LogSegment::VT_BLOCKS, blocks);
    }
    #[inline]
    pub fn new(_fbb: &'b mut flatbuffers::FlatBufferBuilder<'a>) -> LogSegmentBuilder<'a, 'b> {
        let start = _fbb.start_table();
        LogSegmentBuilder {
            fbb_: _fbb,
            start_: start,
        }
    }
    #[inline]
    pub fn finish(self) -> flatbuffers::WIPOffset<LogSegment<'a>> {
        let o = self.fbb_.end_table(self.start_);
        flatbuffers::WIPOffset::new(o.value())
    }
}

impl core::fmt::Debug for LogSegment<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        let mut ds = f.debug_struct("LogSegment");
        ds.field("first_block_number", &self.first_block_number());
        ds.field("blocks", &self.blocks());
        ds.finish()
    }
}