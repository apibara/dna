use std::ops::RangeInclusive;

use apibara_dna_common::{
    data_stream::{Scanner, ScannerError, ScannerFactory},
    Cursor,
};
use error_stack::Result;
use prost::Message;
use roaring::RoaringBitmap;
use tracing::info;

pub struct BeaconChainScannerFactory;

pub struct BeaconChainScanner;

impl BeaconChainScannerFactory {
    pub fn new() -> Self {
        Self
    }
}

impl ScannerFactory for BeaconChainScannerFactory {
    type Scanner = BeaconChainScanner;

    fn create_scanner(&self, _filters: &[Vec<u8>]) -> tonic::Result<Self::Scanner, tonic::Status> {
        Ok(BeaconChainScanner)
    }
}

impl Scanner for BeaconChainScanner {
    async fn fill_block_bitmap(
        &mut self,
        _bitmap: &mut RoaringBitmap,
        _block_range: RangeInclusive<u32>,
    ) -> Result<(), ScannerError> {
        Ok(())
    }

    #[tracing::instrument(name = "stream_send_block", skip_all, err(Debug), fields(cursor = %cursor, data_size))]
    async fn scan_single<S>(&mut self, cursor: &Cursor, cb: S) -> Result<(), ScannerError>
    where
        S: FnOnce(Vec<Vec<u8>>) + Send,
    {
        info!(cursor = %cursor, "scanning single block");
        let block = apibara_dna_protocol::beaconchain::Block::default();

        let encoded = vec![block.encode_to_vec()];
        let data_size = encoded.iter().map(|b| b.len()).sum::<usize>();

        let current_span = tracing::Span::current();
        current_span.record("data_size", data_size);

        cb(encoded);

        Ok(())
    }
}
