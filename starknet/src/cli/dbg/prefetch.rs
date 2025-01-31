use error_stack::{Result, ResultExt};
use prost::Message;

use apibara_dna_common::{
    cli::{EtcdArgs, ObjectStoreArgs},
    dbg::run_debug_prefetch_stream,
    file_cache::FileCacheArgs,
};
use apibara_dna_protocol::starknet;
use clap::Args;
use tokio_util::sync::CancellationToken;

use crate::{error::StarknetError, filter::BlockFilterExt};

#[derive(Args, Debug)]
pub struct DebugPrefetchCommand {
    /// Hex-encoded filter.
    #[arg(long)]
    filter: String,
    /// Queue size.
    #[arg(long, default_value = "100")]
    queue_size: usize,
    #[clap(flatten)]
    object_store: ObjectStoreArgs,
    #[clap(flatten)]
    etcd: EtcdArgs,
    #[clap(flatten)]
    cache: FileCacheArgs,
}

impl DebugPrefetchCommand {
    pub async fn run(self, ct: CancellationToken) -> Result<(), StarknetError> {
        let filter_bytes = hex::decode(&self.filter)
            .change_context(StarknetError)
            .attach_printable("failed to decode hex filter")?;

        let filter = starknet::Filter::decode(filter_bytes.as_slice())
            .change_context(StarknetError)
            .attach_printable("failed to decode filter")?;

        let filter = filter
            .compile_to_block_filter()
            .change_context(StarknetError)?;

        run_debug_prefetch_stream(
            filter,
            self.queue_size,
            self.object_store,
            self.etcd,
            self.cache,
            ct,
        )
        .await
        .change_context(StarknetError)
    }
}
