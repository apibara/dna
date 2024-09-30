use std::time::{Duration, Instant};

use apibara_dna_protocol::{
    dna::stream::{dna_stream_client::DnaStreamClient, Cursor, StreamDataRequest},
    evm,
};
use clap::{Args, Parser, Subcommand};
use error_stack::{Result, ResultExt};
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[derive(Debug)]
pub struct BenchmarkError;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Benchmark the EVM DNA stream.
    Evm(CommonArgs),
}

#[derive(Args, Debug)]
pub struct CommonArgs {
    /// Hex-encoded filter.
    #[clap(long, default_value = "0x")]
    pub filter: String,
    /// Stream URL.
    #[clap(long, default_value = "http://localhost:7007")]
    pub stream_url: String,
    /// Start streaming from this block.
    #[clap(long)]
    pub starting_block: Option<u64>,
    /// Stop streaming at this block.
    #[clap(long)]
    pub ending_block: Option<u64>,
}

impl Cli {
    pub async fn run(self, ct: CancellationToken) -> Result<(), BenchmarkError> {
        match self.command {
            Command::Evm(args) => run_benchmark::<evm::Filter, EvmStats>(args, ct).await,
        }
    }
}

async fn run_benchmark<F, S>(args: CommonArgs, ct: CancellationToken) -> Result<(), BenchmarkError>
where
    F: Message + Default,
    S: Stats,
{
    let bytes = hex::decode(&args.filter)
        .change_context(BenchmarkError)
        .attach_printable("failed to filter hex string")?;

    let filter = <F as Message>::decode(bytes.as_slice())
        .change_context(BenchmarkError)
        .attach_printable("failed to decode filter")?;

    let mut client = DnaStreamClient::connect(args.stream_url.clone())
        .await
        .change_context(BenchmarkError)?;

    let starting_cursor = args.starting_block.map(|block| Cursor {
        order_key: block,
        unique_key: Vec::new(),
    });

    let stream = client
        .stream_data(StreamDataRequest {
            filter: vec![filter.encode_to_vec()],
            starting_cursor,
            ..Default::default()
        })
        .await
        .change_context(BenchmarkError)?
        .into_inner()
        .take_until(async move { ct.cancelled().await });

    tokio::pin!(stream);

    let mut stats = S::new();

    let mut last_print = Instant::now();
    let print_interval = Duration::from_secs(10);

    while let Some(message) = stream.try_next().await.change_context(BenchmarkError)? {
        use apibara_dna_protocol::dna::stream::stream_data_response::Message as ProtoMessage;
        if let Some(ProtoMessage::Data(data_message)) = message.message {
            let block_number = data_message
                .cursor
                .as_ref()
                .map(|c| c.order_key)
                .unwrap_or_default();

            if let Some(block_data) = data_message.data.first() {
                let block = S::Block::decode(block_data.as_slice())
                    .change_context(BenchmarkError)
                    .attach_printable("failed to decode block")?;
                stats.record(block);

                if last_print.elapsed() > print_interval {
                    last_print = Instant::now();
                    stats.print_summary();
                }
            }

            if let Some(end_block) = args.ending_block {
                if block_number >= end_block {
                    info!(block_number, "reached ending block");
                    break;
                }
            }
        }
    }

    stats.print_summary();

    Ok(())
}

trait Stats {
    type Block: Message + Default;
    fn new() -> Self;
    fn record(&mut self, item: Self::Block);
    fn print_summary(&self);
}

struct EvmStats {
    pub start: Instant,
    pub blocks: u64,
    pub transactions: u64,
    pub receipts: u64,
    pub logs: u64,
    pub withdrawals: u64,
}

impl Stats for EvmStats {
    type Block = evm::Block;

    fn new() -> Self {
        Self {
            start: Instant::now(),
            blocks: 0,
            transactions: 0,
            receipts: 0,
            logs: 0,
            withdrawals: 0,
        }
    }

    fn record(&mut self, block: evm::Block) {
        self.blocks += 1;

        self.transactions += block.transactions.len() as u64;
        self.receipts += block.receipts.len() as u64;
        self.logs += block.logs.len() as u64;
        self.withdrawals += block.withdrawals.len() as u64;
    }

    fn print_summary(&self) {
        let elapsed = self.start.elapsed();

        let elapsed_sec = elapsed.as_secs_f64();

        info!(
            blocks = %self.blocks,
            transactions = %self.transactions,
            receipts = %self.receipts,
            logs = %self.logs,
            withdrawals = %self.withdrawals,
            elapsed = ?elapsed,
            "evm stats (count)"
        );

        let block_rate = self.blocks as f64 / elapsed_sec;
        let transaction_rate = self.transactions as f64 / elapsed_sec;
        let receipt_rate = self.receipts as f64 / elapsed_sec;
        let log_rate = self.logs as f64 / elapsed_sec;
        let withdrawal_rate = self.withdrawals as f64 / elapsed_sec;

        info!(
            blocks = %block_rate,
            transactions = %transaction_rate,
            receipts = %receipt_rate,
            logs = %log_rate,
            withdrawals = %withdrawal_rate,
            elapsed = ?elapsed,
            "evm stats (rate)"
        );
    }
}

impl error_stack::Context for BenchmarkError {}

impl std::fmt::Display for BenchmarkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "benchmark error")
    }
}
