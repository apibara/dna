use std::path::PathBuf;

use apibara_sink_common::SinkOptions;
use clap::Args;
use error_stack::{Result, ResultExt};

use crate::sink::SinkParquetError;

#[derive(Debug)]
pub struct SinkParquetConfiguration {
    pub output_dir: PathBuf,
    pub batch_size: usize,
    pub datasets: Option<Vec<String>>,
}

#[derive(Debug, Args, Default, SinkOptions)]
#[sink_options(tag = "parquet")]
pub struct SinkParquetOptions {
    /// The output directory to write the parquet files to.
    ///
    /// If it starts with `s3://`, the files will be written to S3. The S3 credentials are loaded using the default AWS credentials provider chain.
    ///
    /// Otherwise, they will be written to the local filesystem. If the directory does not exist, it will be created.
    #[arg(long, env = "PARQUET_OUTPUT_DIR")]
    pub output_dir: Option<String>,
    /// The batch size to use when writing parquet files.
    #[arg(long, env = "PARQUET_BATCH_SIZE")]
    pub batch_size: Option<usize>,
    /// Output multiple datasets.
    ///
    /// Datasets are organized in subdirectories of the output directory.
    #[arg(long, env = "PARQUET_DATASETS", value_delimiter = ',')]
    pub datasets: Option<Vec<String>>,
}

impl SinkOptions for SinkParquetOptions {
    fn merge(self, other: Self) -> Self {
        Self {
            output_dir: self.output_dir.or(other.output_dir),
            batch_size: self.batch_size.or(other.batch_size),
            datasets: self.datasets.or(other.datasets),
        }
    }
}

impl SinkParquetOptions {
    pub fn to_parquet_configuration(self) -> Result<SinkParquetConfiguration, SinkParquetError> {
        let output_dir = self
            .output_dir
            .ok_or(SinkParquetError)
            .attach_printable("missing output directory")?
            .into();

        let batch_size = self.batch_size.unwrap_or(1000);
        let batch_size = batch_size.clamp(100, 5_000);

        Ok(SinkParquetConfiguration {
            output_dir,
            batch_size,
            datasets: self.datasets,
        })
    }
}
