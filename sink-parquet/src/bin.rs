use apibara_core::starknet::v1alpha2::{Block, Filter};
use apibara_observability::init_opentelemetry;
use apibara_sink_common::{ConfigurationArgsWithoutFinality, SinkConnector, SinkConnectorExt};
use apibara_sink_parquet::ParquetSink;
use clap::Parser;
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// The directory where to write Parquet files.
    #[arg(long, env)]
    output_dir: String,
    /// Each Parquet file contains data for the specified number of blocks.
    #[arg(long, env, default_value = "1000")]
    parquet_batch_size: Option<usize>,
    #[command(flatten)]
    configuration: ConfigurationArgsWithoutFinality,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_opentelemetry()?;
    let args = Cli::parse();

    let sink = ParquetSink::new(args.output_dir, args.parquet_batch_size.unwrap_or(1000))?;
    let ct = CancellationToken::new();
    let connector =
        SinkConnector::<Filter, Block>::from_configuration_args(args.configuration.into())?;

    connector.consume_stream(sink, ct).await?;

    Ok(())
}
