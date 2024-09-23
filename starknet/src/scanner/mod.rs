use apibara_dna_common::{
    data_stream::{FragmentAccess, Scanner, ScannerError, ScannerFactory},
    Cursor,
};
use error_stack::Result;

#[derive(Debug, Default, Clone)]
pub struct StarknetScannerFactory;

#[derive(Debug)]
pub struct StarknetScanner {}

impl ScannerFactory for StarknetScannerFactory {
    type Scanner = StarknetScanner;

    fn create_scanner(&self, _filters: &[Vec<u8>]) -> tonic::Result<Self::Scanner, tonic::Status> {
        Ok(StarknetScanner {})
    }
}

impl Scanner for StarknetScanner {
    async fn fill_block_bitmap(
        &mut self,
        group: &apibara_dna_common::store::group::ArchivedSegmentGroup,
        bitmap: &mut roaring::RoaringBitmap,
        block_range: std::ops::RangeInclusive<u32>,
    ) -> Result<(), ScannerError> {
        todo!();
    }

    async fn scan_single<S>(
        &mut self,
        cursor: &Cursor,
        fragment_access: &FragmentAccess,
        cb: S,
    ) -> Result<(), ScannerError>
    where
        S: FnOnce(Vec<Vec<u8>>) + Send,
    {
        todo!();
    }
}
