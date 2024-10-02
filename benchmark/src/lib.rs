use std::time::{Duration, Instant};

use apibara_dna_protocol::{
    dna::stream::{dna_stream_client::DnaStreamClient, Cursor, StreamDataRequest},
    evm, starknet,
};
use byte_unit::Byte;
use clap::{Args, Parser, Subcommand};
use error_stack::{Result, ResultExt};
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use tokio::task::JoinSet;
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
    /// Benchmark the Starknet DNA stream.
    Starknet(CommonArgs),
}

#[derive(Args, Debug, Clone)]
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
    #[clap(long, default_value = "1")]
    pub concurrency: usize,
}

impl Cli {
    pub async fn run(self, ct: CancellationToken) -> Result<(), BenchmarkError> {
        match self.command {
            Command::Evm(args) => run_benchmark::<evm::Filter, EvmStats>(args, ct).await,
            Command::Starknet(args) => {
                run_benchmark::<starknet::Filter, StarknetStats>(args, ct).await
            }
        }
    }
}

async fn run_benchmark<F, S>(args: CommonArgs, ct: CancellationToken) -> Result<(), BenchmarkError>
where
    F: Message + Clone + Default + Send + 'static,
    S: Stats + Send + 'static,
{
    let bytes = hex::decode(&args.filter)
        .change_context(BenchmarkError)
        .attach_printable("failed to filter hex string")?;

    let filter = <F as Message>::decode(bytes.as_slice())
        .change_context(BenchmarkError)
        .attach_printable("failed to decode filter")?;

    let mut tasks = JoinSet::new();
    for i in 0..args.concurrency {
        tasks.spawn(run_benchmark_single::<F, S>(
            i,
            args.clone(),
            filter.clone(),
            ct.clone(),
        ));
    }

    while let Some(result) = tasks.join_next().await {
        result.change_context(BenchmarkError)??;
    }

    Ok(())
}

async fn run_benchmark_single<F, S>(
    index: usize,
    args: CommonArgs,
    filter: F,
    ct: CancellationToken,
) -> Result<(), BenchmarkError>
where
    F: Message + Default + Send,
    S: Stats + Send,
{
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

    let mut stats = S::new(index);

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
                let block = S::Block::decode(block_data.as_ref())
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
    fn new(index: usize) -> Self;
    fn record(&mut self, item: Self::Block);
    fn print_summary(&self);
}

struct EvmStats {
    pub index: usize,
    pub start: Instant,
    pub bytes: u64,
    pub blocks: u64,
    pub transactions: u64,
    pub receipts: u64,
    pub logs: u64,
    pub withdrawals: u64,
}

impl Stats for EvmStats {
    type Block = evm::Block;

    fn new(index: usize) -> Self {
        Self {
            index,
            start: Instant::now(),
            blocks: 0,
            bytes: 0,
            transactions: 0,
            receipts: 0,
            logs: 0,
            withdrawals: 0,
        }
    }

    fn record(&mut self, block: evm::Block) {
        self.blocks += 1;
        self.bytes += block.encoded_len() as u64;

        self.transactions += block.transactions.len() as u64;
        self.receipts += block.receipts.len() as u64;
        self.logs += block.logs.len() as u64;
        self.withdrawals += block.withdrawals.len() as u64;
    }

    fn print_summary(&self) {
        let elapsed = self.start.elapsed();

        let elapsed_sec = elapsed.as_secs_f64();
        let bytes = Byte::from_u64(self.bytes);

        info!(
            blocks = %self.blocks,
            bytes = format!("{:#.6}", bytes),
            transactions = %self.transactions,
            receipts = %self.receipts,
            logs = %self.logs,
            withdrawals = %self.withdrawals,
            elapsed = ?elapsed,
            "[{}] evm stats (count)",
            self.index,
        );

        let block_rate = self.blocks as f64 / elapsed_sec;
        let byte_rate = Byte::from_f64(self.bytes as f64 / elapsed_sec).unwrap_or_default();
        let transaction_rate = self.transactions as f64 / elapsed_sec;
        let receipt_rate = self.receipts as f64 / elapsed_sec;
        let log_rate = self.logs as f64 / elapsed_sec;
        let withdrawal_rate = self.withdrawals as f64 / elapsed_sec;

        info!(
            blocks = %block_rate,
            bytes = format!("{:#.6}/s", byte_rate),
            transactions = %transaction_rate,
            receipts = %receipt_rate,
            logs = %log_rate,
            withdrawals = %withdrawal_rate,
            elapsed = ?elapsed,
            "[{}] evm stats (rate)",
            self.index,
        );
    }
}

struct StarknetStats {
    pub index: usize,
    pub start: Instant,
    pub blocks: u64,
    pub bytes: u64,
    pub transactions: u64,
    pub receipts: u64,
    pub events: u64,
    pub messages: u64,
}

impl Stats for StarknetStats {
    type Block = starknet::Block;

    fn new(index: usize) -> Self {
        Self {
            index,
            start: Instant::now(),
            blocks: 0,
            bytes: 0,
            transactions: 0,
            receipts: 0,
            events: 0,
            messages: 0,
        }
    }

    fn record(&mut self, block: starknet::Block) {
        self.blocks += 1;
        self.bytes += block.encoded_len() as u64;

        self.transactions += block.transactions.len() as u64;
        self.receipts += block.receipts.len() as u64;
        self.events += block.events.len() as u64;
        self.messages += block.messages.len() as u64;
    }

    fn print_summary(&self) {
        let elapsed = self.start.elapsed();

        let elapsed_sec = elapsed.as_secs_f64();
        let bytes = Byte::from_u64(self.bytes);

        info!(
            blocks = %self.blocks,
            bytes = format!("{:#.6}", bytes),
            transactions = %self.transactions,
            receipts = %self.receipts,
            logs = %self.events,
            withdrawals = %self.messages,
            elapsed = ?elapsed,
            "[{}] starknet stats (count)",
            self.index
        );

        let block_rate = self.blocks as f64 / elapsed_sec;
        let byte_rate = Byte::from_f64(self.bytes as f64 / elapsed_sec).unwrap_or_default();
        let transaction_rate = self.transactions as f64 / elapsed_sec;
        let receipt_rate = self.receipts as f64 / elapsed_sec;
        let event_rate = self.events as f64 / elapsed_sec;
        let message_rate = self.messages as f64 / elapsed_sec;

        info!(
            index = self.index,
            blocks = %block_rate,
            bytes = format!("{:#.6}/s", byte_rate),
            transactions = %transaction_rate,
            receipts = %receipt_rate,
            events = %event_rate,
            messages = %message_rate,
            elapsed = ?elapsed,
            "[{}] starknet stats (rate)",
            self.index
        );
    }
}

impl error_stack::Context for BenchmarkError {}

impl std::fmt::Display for BenchmarkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "benchmark error")
    }
}
