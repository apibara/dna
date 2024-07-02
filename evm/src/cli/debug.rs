use std::time::Duration;

use apibara_dna_common::{
    ingestion::{BlockIngestionDriver, IngestionState, Snapshot, SnapshotError, SnapshotReader},
    segment::{store::Segment, SegmentOptions},
};
use clap::Args;
use error_stack::{Result, ResultExt};
use futures_util::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::{
    error::DnaEvmError,
    ingestion::{models, EvmCursorProvider, EvmCursorProviderOptions},
};

use super::common::RpcArgs;

#[derive(Args, Debug)]
pub struct DebugChainTrackerArgs {
    #[clap(flatten)]
    rpc: RpcArgs,
}

struct MockSnapshotReader;

pub async fn run_debug_chain_tracker(args: DebugChainTrackerArgs) -> Result<(), DnaEvmError> {
    let ct = CancellationToken::new();

    let provider_factory = args.rpc.to_json_rpc_provider_factory()?;
    let provider = provider_factory.new_provider();

    let options = EvmCursorProviderOptions {
        poll_interval: Duration::from_secs(1),
        ..Default::default()
    };

    let cursor_provider = EvmCursorProvider::new(provider, options);
    let (mut cursor_stream, handle) =
        BlockIngestionDriver::new(cursor_provider, MockSnapshotReader, Default::default())
            .start(ct);

    while let Some(message) = cursor_stream.next().await {
        println!("{:?}", message);
    }

    handle
        .await
        .change_context(DnaEvmError::Fatal)?
        .change_context(DnaEvmError::Fatal)?;

    Ok(())
}

#[async_trait::async_trait]
impl SnapshotReader for MockSnapshotReader {
    async fn read(&self) -> Result<Snapshot, SnapshotError> {
        Ok(Snapshot {
            revision: 0,
            segment_options: SegmentOptions {
                segment_size: 100,
                group_size: 10,
            },
            ingestion: IngestionState {
                group_count: 0,
                extra_segment_count: 0,
                first_block_number: 0,
            },
        })
    }
}
