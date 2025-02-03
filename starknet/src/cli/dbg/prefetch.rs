use std::collections::HashMap;

use error_stack::{Result, ResultExt};
use prost::Message;

use apibara_dna_common::{
    cli::{EtcdArgs, ObjectStoreArgs},
    dbg::run_debug_prefetch_stream,
    file_cache::FileCacheArgs,
    fragment::{
        HEADER_FRAGMENT_ID, HEADER_FRAGMENT_NAME, INDEX_FRAGMENT_ID, INDEX_FRAGMENT_NAME,
        JOIN_FRAGMENT_ID, JOIN_FRAGMENT_NAME,
    },
};
use apibara_dna_protocol::starknet;
use clap::Args;
use tokio_util::sync::CancellationToken;

use crate::{
    error::StarknetError,
    filter::BlockFilterExt,
    fragment::{
        CONTRACT_CHANGE_FRAGMENT_ID, CONTRACT_CHANGE_FRAGMENT_NAME, EVENT_FRAGMENT_ID,
        EVENT_FRAGMENT_NAME, MESSAGE_FRAGMENT_ID, MESSAGE_FRAGMENT_NAME, NONCE_UPDATE_FRAGMENT_ID,
        NONCE_UPDATE_FRAGMENT_NAME, RECEIPT_FRAGMENT_ID, RECEIPT_FRAGMENT_NAME,
        STORAGE_DIFF_FRAGMENT_ID, STORAGE_DIFF_FRAGMENT_NAME, TRANSACTION_FRAGMENT_ID,
        TRANSACTION_FRAGMENT_NAME,
    },
};

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

        let fragment_id_to_name = HashMap::from([
            (INDEX_FRAGMENT_ID, INDEX_FRAGMENT_NAME.to_string()),
            (JOIN_FRAGMENT_ID, JOIN_FRAGMENT_NAME.to_string()),
            (HEADER_FRAGMENT_ID, HEADER_FRAGMENT_NAME.to_string()),
            (
                TRANSACTION_FRAGMENT_ID,
                TRANSACTION_FRAGMENT_NAME.to_string(),
            ),
            (RECEIPT_FRAGMENT_ID, RECEIPT_FRAGMENT_NAME.to_string()),
            (EVENT_FRAGMENT_ID, EVENT_FRAGMENT_NAME.to_string()),
            (MESSAGE_FRAGMENT_ID, MESSAGE_FRAGMENT_NAME.to_string()),
            (
                STORAGE_DIFF_FRAGMENT_ID,
                STORAGE_DIFF_FRAGMENT_NAME.to_string(),
            ),
            (
                NONCE_UPDATE_FRAGMENT_ID,
                NONCE_UPDATE_FRAGMENT_NAME.to_string(),
            ),
            (
                CONTRACT_CHANGE_FRAGMENT_ID,
                CONTRACT_CHANGE_FRAGMENT_NAME.to_string(),
            ),
            (
                NONCE_UPDATE_FRAGMENT_ID,
                NONCE_UPDATE_FRAGMENT_NAME.to_string(),
            ),
        ]);

        run_debug_prefetch_stream(
            filter,
            fragment_id_to_name,
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
