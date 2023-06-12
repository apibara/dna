use apibara_core::starknet::v1alpha2::{Block, Filter};
use apibara_observability::init_opentelemetry;
use apibara_sink_common::{ConfigurationArgs, SinkConnector, SinkConnectorExt};
use apibara_sink_postgres::PostgresSink;
use clap::Parser;
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(long, env)]
    connection_string: String,
    #[arg(long, env)]
    table_name: String,
    #[command(flatten)]
    configuration: ConfigurationArgs,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_opentelemetry()?;
    let args = Cli::parse();

    let sink = PostgresSink::new(args.connection_string, args.table_name).await?;
    let ct = CancellationToken::new();
    let connector = SinkConnector::<Filter, Block>::from_configuration_args(args.configuration)?;

    connector.consume_stream(sink, ct).await?;

    Ok(())
}
