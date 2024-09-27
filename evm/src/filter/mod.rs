use apibara_dna_common::{data_stream::BlockFilterFactory, query::BlockFilter};

pub struct EvmFilterFactory;

impl BlockFilterFactory for EvmFilterFactory {
    fn create_block_filter(
        &self,
        _filters: &[Vec<u8>],
    ) -> tonic::Result<Vec<BlockFilter>, tonic::Status> {
        todo!();
    }
}
