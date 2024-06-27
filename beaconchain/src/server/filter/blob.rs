use apibara_dna_protocol::beaconchain;

#[derive(Debug)]
pub struct BlobFilter {
    pub include_transaction: bool,
}

impl From<beaconchain::BlobFilter> for BlobFilter {
    fn from(x: beaconchain::BlobFilter) -> Self {
        Self {
            include_transaction: x.include_transaction.unwrap_or(false),
        }
    }
}
