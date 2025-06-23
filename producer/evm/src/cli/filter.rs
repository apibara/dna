use std::time::Instant;

use alloy::{
    consensus::EthereumTxEnvelope,
    providers::{Provider, ProviderBuilder},
};
use arrow::{ipc::FixedSizeBinary, util::pretty::pretty_format_batches};
use clap::Args;
use datafusion::{
    config::{ParquetOptions, TableParquetOptions},
    dataframe::DataFrameWriteOptions,
    prelude::{DataFrame, ParquetReadOptions, SessionContext, col, lit},
};
use error_stack::{FutureExt, Result, ResultExt};
use tokio_util::sync::CancellationToken;

use crate::error::EvmError;

#[derive(Args, Debug)]
pub struct FilterArgs {
    /// The input file to read the data from.
    #[arg(long = "input.file", env = "EVM_INPUT_FILE")]
    pub input_file: String,
    /// The transaction sender address.
    pub from_address: String,
}

impl FilterArgs {
    pub async fn run(self, ct: CancellationToken) -> Result<(), EvmError> {
        let ctx = SessionContext::new();

        let df = ctx
            .read_parquet(self.input_file, ParquetReadOptions::default())
            .await
            .change_context(EvmError)?;

        let start = Instant::now();

        let from_address = hex::decode(self.from_address).change_context(EvmError)?;
        let values = df
            .filter(col("from_address").eq(lit(from_address)))
            .change_context(EvmError)?
            .collect()
            .change_context(EvmError)
            .await?;
        let count = values.iter().fold(0, |c, b| c + b.num_rows());

        let elapsed = start.elapsed();
        println!("{:?}", count);
        println!("Elapsed time: {:?}", elapsed);
        Ok(())
    }
}
