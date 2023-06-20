use apibara_core::starknet::v1alpha2::{Block, Filter};
use apibara_observability::init_opentelemetry;
use apibara_sink_common::{ConfigurationArgs, SinkConnector, SinkConnectorExt};
use apibara_sink_mongo::MongoSink;
use clap::Parser;
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Connection string for the MongoDB instance.
    #[arg(long, env)]
    mongo_url: String,
    /// Target database name.
    #[arg(long, env)]
    db_name: String,
    /// Target collection name.
    #[arg(long, env)]
    collection_name: String,
    #[command(flatten)]
    configuration: ConfigurationArgs,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_opentelemetry()?;
    let args = Cli::parse();

    let sink = MongoSink::new(args.mongo_url, args.db_name, args.collection_name).await?;
    let ct = CancellationToken::new();
    let connector = SinkConnector::<Filter, Block>::from_configuration_args(args.configuration)?;

    connector.consume_stream(sink, ct).await?;

    Ok(())
}
