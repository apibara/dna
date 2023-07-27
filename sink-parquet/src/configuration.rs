use std::path::PathBuf;

use apibara_sink_common::SinkOptions;
use clap::Args;

use crate::sink::SinkParquetError;

#[derive(Debug)]
pub struct SinkParquetConfiguration {
    pub output_dir: PathBuf,
    pub batch_size: usize,
}

#[derive(Debug, Args, Default, SinkOptions)]
#[sink_options(tag = "parquet")]
pub struct SinkParquetOptions {
    /// The output directory to write the parquet files to.
    #[arg(long, env = "PARQUET_OUTPUT_DIR")]
    pub output_dir: Option<String>,
    /// The batch size to use when writing parquet files.
    #[arg(long, env = "PARQUET_BATCH_SIZE")]
    pub batch_size: Option<usize>,
}

impl SinkOptions for SinkParquetOptions {
    fn merge(self, other: Self) -> Self {
        Self {
            output_dir: self.output_dir.or(other.output_dir),
            batch_size: self.batch_size.or(other.batch_size),
        }
    }
}

impl SinkParquetOptions {
    pub fn to_parquet_configuration(self) -> Result<SinkParquetConfiguration, SinkParquetError> {
        let output_dir = self
            .output_dir
            .ok_or_else(|| SinkParquetError::MissingOutputDir)?
            .into();

        let batch_size = self.batch_size.unwrap_or(1000);
        let batch_size = batch_size.clamp(100, 5_000);

        Ok(SinkParquetConfiguration {
            output_dir,
            batch_size,
        })
    }
}
