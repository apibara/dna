use apibara_dna_common::{data_stream::BlockFilterFactory, query::BlockFilter};

pub struct BeaconChainFilterFactory;

impl BlockFilterFactory for BeaconChainFilterFactory {
    fn create_block_filter(
        &self,
        filters: &[Vec<u8>],
    ) -> tonic::Result<Vec<BlockFilter>, tonic::Status> {
        todo!();
    }
}
