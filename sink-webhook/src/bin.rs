use apibara_core::starknet::v1alpha2::{Block, Filter};
use apibara_observability::init_opentelemetry;
use apibara_sink_common::{ConfigurationArgs, SinkConnector, SinkConnectorExt};
use apibara_sink_webhook::WebhookSink;
use clap::Parser;
use tokio_util::sync::CancellationToken;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// The target url to send the request to.
    #[arg(long, env)]
    target_url: String,
    /// Additional headers to send with the request.
    #[arg(long, short = 'H', env)]
    header: Vec<String>,
    #[command(flatten)]
    configuration: ConfigurationArgs,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_opentelemetry()?;
    let args = Cli::parse();

    let sink = WebhookSink::new(args.target_url)?.with_headers(&args.header)?;
    let ct = CancellationToken::new();
    let connector = SinkConnector::<Filter, Block>::from_configuration_args(args.configuration)?;

    connector.consume_stream(sink, ct).await?;

    Ok(())
}
