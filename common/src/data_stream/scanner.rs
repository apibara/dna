use std::ops::RangeInclusive;

use error_stack::Result;
use futures::Future;
use roaring::RoaringBitmap;

use crate::Cursor;

#[derive(Debug)]
pub struct ScannerError;

/// Action to take.
#[derive(Debug, PartialEq)]
pub enum ScannerAction {
    /// Continue scanning.
    Continue,
    /// Stop scanning.
    Stop,
}

/// Send the specified data to the client.
pub struct SendData {
    pub cursor: Option<Cursor>,
    pub end_cursor: Cursor,
    pub data: Vec<Vec<u8>>,
}

/// Information about a block in a segment.
#[derive(Debug, Clone)]
pub struct SegmentBlock {
    /// The block's cursor.
    pub cursor: Option<Cursor>,
    /// The block's end cursor.
    pub end_cursor: Cursor,
    /// Offset of the block in the segment.
    pub offset: usize,
}

pub trait ScannerFactory {
    type Scanner: Scanner;

    fn create_scanner(&self, filters: &[Vec<u8>]) -> tonic::Result<Self::Scanner, tonic::Status>;
}

pub trait Scanner: Send {
    fn fill_block_bitmap(
        &mut self,
        group_cursor: Cursor,
        blocks_in_group: usize,
        bitmap: &mut RoaringBitmap,
        block_range: RangeInclusive<u32>,
    ) -> impl Future<Output = Result<(), ScannerError>> + Send;

    fn scan_segment<S, SR>(
        &mut self,
        segment_cursor: Cursor,
        blocks: Vec<SegmentBlock>,
        cb: S,
    ) -> impl Future<Output = Result<(), ScannerError>> + Send
    where
        S: Fn(SendData) -> SR + Send,
        SR: Future<Output = ScannerAction> + Send;

    fn scan_single<S>(
        &mut self,
        cursor: &Cursor,
        cb: S,
    ) -> impl Future<Output = Result<(), ScannerError>> + Send
    where
        S: FnOnce(Vec<Vec<u8>>) + Send;
}

impl error_stack::Context for ScannerError {}

impl std::fmt::Display for ScannerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "scanner error")
    }
}
