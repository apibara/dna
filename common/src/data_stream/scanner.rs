use std::ops::RangeInclusive;

use error_stack::Result;
use futures::Future;
use roaring::RoaringBitmap;

use crate::Cursor;

#[derive(Debug)]
pub struct ScannerError;

pub trait ScannerFactory {
    type Scanner: Scanner;

    fn create_scanner(&self, filters: &[Vec<u8>]) -> tonic::Result<Self::Scanner, tonic::Status>;
}

pub trait Scanner: Send {
    fn fill_block_bitmap(
        &mut self,
        bitmap: &mut RoaringBitmap,
        block_range: RangeInclusive<u32>,
    ) -> impl Future<Output = Result<(), ScannerError>> + Send;

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
