use alloy::{
    consensus::EthereumTxEnvelope,
    providers::{Provider, ProviderBuilder},
};
use arrow::util::pretty::pretty_format_batches;
use clap::Args;
use datafusion::{
    config::{ParquetOptions, TableParquetOptions},
    dataframe::DataFrameWriteOptions,
    prelude::{DataFrame, SessionContext},
};
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;

use crate::{error::EvmError, schema::transaction::TransactionBatchBuilder};

#[derive(Args, Debug)]
pub struct IngestArgs {
    /// The Ethereum RPC URL to connect to.
    #[arg(long = "rpc.url", env = "EVM_RPC_URL")]
    pub rpc_url: String,
    /// The output file to write the data to.
    #[arg(long = "output.file", env = "EVM_OUTPUT_FILE")]
    pub output_file: String,
    /// The starting block number for ingestion.
    #[arg(
        long = "ingest.starting-block",
        env = "EVM_INGEST_STARTING_BLOCK",
        default_value = "0"
    )]
    pub ingest_starting_block: u32,
    /// The number of blocks to ingest.
    #[arg(long = "ingest.block-count", env = "EVM_INGEST_BLOCK_COUNT")]
    pub ingest_block_count: u32,
}

impl IngestArgs {
    pub async fn run(self, ct: CancellationToken) -> Result<(), EvmError> {
        // Implementation goes here
        let rpc_url = self
            .rpc_url
            .parse::<reqwest::Url>()
            .change_context(EvmError)
            .attach_printable("failed to parse RPC URL")?;
        let provider = ProviderBuilder::new().connect_http(rpc_url);

        let starting_block = self.ingest_starting_block;
        let ending_block = starting_block + self.ingest_block_count;

        let mut tx_batch_builder = TransactionBatchBuilder::default();

        for block_number in starting_block..ending_block {
            println!("Processing block {}", block_number);
            let block = provider
                .get_block_by_number((block_number as u64).into())
                .full()
                .await
                .change_context(EvmError)
                .attach_printable("failed to get block")
                .attach_printable_lazy(|| format!("block number: {block_number}"))?;
            let Some(block) = block else {
                println!("Block not found");
                continue;
            };

            println!("Block: {}", block.hash());

            let header = block.header;

            for (tx_index, tx) in block.transactions.into_transactions().enumerate() {
                let signer = tx.as_recovered().signer();
                tx_batch_builder.append_transaction(&header, tx_index, tx.inner.as_ref(), &signer);
            }

            println!();
        }

        let data = tx_batch_builder.finish();

        let ctx = SessionContext::new();
        let df = ctx.read_batch(data).change_context(EvmError)?;
        df.write_parquet(
            &self.output_file,
            DataFrameWriteOptions::default(),
            TableParquetOptions {
                global: ParquetOptions {
                    bloom_filter_on_write: true,
                    ..Default::default()
                },
                ..Default::default()
            }
            .into(),
        )
        .await
        .change_context(EvmError)?;

        Ok(())
    }
}
