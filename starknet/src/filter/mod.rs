mod event;
mod helpers;
mod message;
mod transaction;

use apibara_dna_common::{data_stream::BlockFilterFactory, query::BlockFilter};
use apibara_dna_protocol::starknet;
use prost::Message;

use self::helpers::{BlockFilterExt, FragmentFilterExt};

#[derive(Debug, Clone)]
pub struct StarknetFilterFactory;

impl BlockFilterFactory for StarknetFilterFactory {
    fn create_block_filter(
        &self,
        filters: &[Vec<u8>],
    ) -> tonic::Result<Vec<BlockFilter>, tonic::Status> {
        let proto_filters = filters
            .iter()
            .map(|bytes| starknet::Filter::decode(bytes.as_slice()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| tonic::Status::invalid_argument("failed to decode filter"))?;

        if proto_filters.is_empty() {
            return Err(tonic::Status::invalid_argument("no filters provided"));
        }

        if proto_filters.len() > 5 {
            return Err(tonic::Status::invalid_argument(format!(
                "too many filters ({} > 5)",
                proto_filters.len(),
            )));
        }

        proto_filters
            .iter()
            .map(BlockFilterExt::compile_to_block_filter)
            .collect()
    }
}

impl BlockFilterExt for starknet::Filter {
    fn compile_to_block_filter(&self) -> tonic::Result<BlockFilter, tonic::Status> {
        let mut block_filter = BlockFilter::default();

        if self.header.map(|h| h.always()).unwrap_or(false) {
            block_filter.set_always_include_header(true);
        }

        for filter in self.transactions.iter() {
            let filter = filter.compile_to_filter()?;
            block_filter.add_filter(filter);
        }

        for filter in self.events.iter() {
            let filter = filter.compile_to_filter()?;
            block_filter.add_filter(filter);
        }

        for filter in self.messages.iter() {
            let filter = filter.compile_to_filter()?;
            block_filter.add_filter(filter);
        }

        if !block_filter.always_include_header && block_filter.is_empty() {
            return Err(tonic::Status::invalid_argument("no filters provided"));
        }

        Ok(block_filter)
    }
}
