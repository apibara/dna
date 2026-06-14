use std::{
    collections::HashMap,
    str::FromStr,
    time::{Duration, Instant},
};

use apibara_dna_protocol::{
    dna::stream::{
        dna_stream_client::DnaStreamClient, stream_data_response, Cursor, DataFinality,
        StreamDataRequest,
    },
    evm, starknet,
};
use byte_unit::Byte;
use clap::{Args, Parser, Subcommand};
use error_stack::{Result, ResultExt};
use futures::{SinkExt, StreamExt, TryStreamExt};
use prost::Message;
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_util::sync::CancellationToken;
use tonic::{metadata::AsciiMetadataValue, IntoRequest};
use tracing::{info, warn};

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
    /// Compare direct Starknet subscribeEvents latency with DNA pending stream latency.
    StarknetLiveLatency(StarknetLiveLatencyArgs),
}

#[derive(Args, Debug, Clone)]
pub struct CommonArgs {
    /// Hex-encoded filter.
    #[clap(long, default_value = "00")]
    pub filter: String,
    /// Stream URL.
    #[clap(long, default_value = "http://localhost:7007")]
    pub stream_url: String,
    /// Bearer token used for authentication.
    #[clap(long)]
    pub bearer_token: Option<String>,
    /// Start streaming from this block.
    #[clap(long)]
    pub starting_block: Option<u64>,
    /// Stop streaming at this block.
    #[clap(long)]
    pub ending_block: Option<u64>,
    #[clap(long, default_value = "1")]
    pub concurrency: usize,
}

#[derive(Args, Debug, Clone)]
pub struct StarknetLiveLatencyArgs {
    /// Direct Starknet WebSocket RPC URL.
    #[clap(long, default_value = "ws://64.34.87.87:9545/ws/rpc/v0_10")]
    pub direct_ws_url: String,
    /// DNA stream URL.
    #[clap(long, default_value = "http://localhost:7007")]
    pub stream_url: String,
    /// Bearer token used for DNA stream authentication.
    #[clap(long)]
    pub bearer_token: Option<String>,
    /// Death Mountain GameCore contract address.
    #[clap(long)]
    pub game_core_address: String,
    /// GameEvent selector key, for example sn_keccak(\"GameEvent\") padded as a felt hex string.
    #[clap(long)]
    pub game_event_key: String,
    /// Optional keyed-layout adventurer id. If omitted, all GameEvent events are matched.
    #[clap(long)]
    pub adventurer_id: Option<String>,
    /// Benchmark duration in seconds.
    #[clap(long, default_value = "120")]
    pub duration_secs: u64,
    /// Stop once this many matching events have been observed.
    #[clap(long, default_value = "20")]
    pub min_samples: usize,
}

impl Cli {
    pub async fn run(self, ct: CancellationToken) -> Result<(), BenchmarkError> {
        match self.command {
            Command::Evm(args) => run_benchmark::<evm::Filter, EvmStats>(args, ct).await,
            Command::Starknet(args) => {
                run_benchmark::<starknet::Filter, StarknetStats>(args, ct).await
            }
            Command::StarknetLiveLatency(args) => run_starknet_live_latency(args, ct).await,
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

    let mut request = StreamDataRequest {
        filter: vec![filter.encode_to_vec()],
        starting_cursor,
        ..Default::default()
    }
    .into_request();

    if let Some(bearer_token) = args.bearer_token {
        let authorization_value = format!("Bearer {bearer_token}");
        let authorization_value = AsciiMetadataValue::from_str(&authorization_value)
            .change_context(BenchmarkError)
            .attach_printable("failed to parse authorization value")?;
        request
            .metadata_mut()
            .insert("authorization", authorization_value);
    }

    let stream = client
        .stream_data(request)
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
        match message.message {
            Some(ProtoMessage::Data(data_message)) => {
                let block_number = data_message
                    .end_cursor
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
            Some(ProtoMessage::SystemMessage(system_message)) => {
                use apibara_dna_protocol::dna::stream::system_message::Output;

                match system_message.output {
                    Some(Output::Stdout(stdout)) => info!("{}", stdout),
                    Some(Output::Stderr(stderr)) => warn!("{}", stderr),
                    _ => {}
                }
            }
            _ => {}
        }
    }

    stats.print_summary();

    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct EventIdentity {
    transaction_hash: String,
    event_index_in_transaction: u32,
    from_address: String,
    keys: Vec<String>,
    data: Vec<String>,
}

#[derive(Debug, Clone)]
struct EventObservation {
    identity: EventIdentity,
    seen_at: Instant,
    block_number: Option<u64>,
    finality: String,
    event_index: Option<u32>,
}

#[derive(Debug, Default)]
struct EventMatchState {
    direct: Option<EventObservation>,
    dna: Option<EventObservation>,
    printed: bool,
}

async fn run_starknet_live_latency(
    args: StarknetLiveLatencyArgs,
    ct: CancellationToken,
) -> Result<(), BenchmarkError> {
    let (direct_tx, mut direct_rx) = mpsc::channel(1024);
    let (dna_tx, mut dna_rx) = mpsc::channel(1024);

    let direct_task = tokio::spawn(run_direct_subscribe_events(
        args.clone(),
        direct_tx,
        ct.clone(),
    ));
    let dna_task = tokio::spawn(run_dna_starknet_events(args.clone(), dna_tx, ct.clone()));

    println!(
        "txHash,blockNumber,eventIndex,finality,directSubscribeEventsSeenMs,dnaStreamSeenMs,dnaMinusSubscribeEventsMs"
    );

    let start = Instant::now();
    let deadline = tokio::time::sleep(Duration::from_secs(args.duration_secs));
    tokio::pin!(deadline);

    let mut matches = HashMap::<EventIdentity, EventMatchState>::new();
    let mut latencies = Vec::<i128>::new();

    loop {
        tokio::select! {
            _ = ct.cancelled() => break,
            _ = &mut deadline => break,
            Some(observation) = direct_rx.recv() => {
                record_observation(observation, true, start, &mut matches, &mut latencies);
            }
            Some(observation) = dna_rx.recv() => {
                record_observation(observation, false, start, &mut matches, &mut latencies);
            }
            else => break,
        }

        if latencies.len() >= args.min_samples {
            break;
        }
    }

    ct.cancel();
    direct_task.abort();
    dna_task.abort();
    finish_latency_task(direct_task, "direct subscribeEvents").await?;
    finish_latency_task(dna_task, "DNA stream").await?;

    print_latency_summary(&latencies);

    Ok(())
}

async fn finish_latency_task(
    task: tokio::task::JoinHandle<Result<(), BenchmarkError>>,
    label: &str,
) -> Result<(), BenchmarkError> {
    match task.await {
        Ok(result) => result,
        Err(err) if err.is_cancelled() => Ok(()),
        Err(err) => Err(err)
            .change_context(BenchmarkError)
            .attach_printable_lazy(|| format!("{label} task failed")),
    }
}

fn record_observation(
    observation: EventObservation,
    is_direct: bool,
    start: Instant,
    matches: &mut HashMap<EventIdentity, EventMatchState>,
    latencies: &mut Vec<i128>,
) {
    let state = matches.entry(observation.identity.clone()).or_default();

    if is_direct {
        if state.direct.is_none() {
            state.direct = Some(observation);
        }
    } else if state.dna.is_none() {
        state.dna = Some(observation);
    }

    let (Some(direct), Some(dna)) = (&state.direct, &state.dna) else {
        return;
    };

    if state.printed {
        return;
    }

    state.printed = true;

    let latency_ms = signed_duration_ms(dna.seen_at, direct.seen_at);
    latencies.push(latency_ms);

    let block_number = dna.block_number.or(direct.block_number).unwrap_or_default();
    let event_index = dna.event_index.unwrap_or_default();
    let direct_seen_ms = signed_duration_ms(direct.seen_at, start);
    let dna_seen_ms = signed_duration_ms(dna.seen_at, start);

    println!(
        "{},{},{},{},{},{},{}",
        dna.identity.transaction_hash,
        block_number,
        event_index,
        dna.finality,
        direct_seen_ms,
        dna_seen_ms,
        latency_ms
    );
}

fn print_latency_summary(latencies: &[i128]) {
    if latencies.is_empty() {
        println!("summary,count=0,p95DnaMinusSubscribeEventsMs=");
        return;
    }

    let mut sorted = latencies.to_vec();
    sorted.sort_unstable();
    let p95_index = ((sorted.len() * 95).div_ceil(100)).saturating_sub(1);
    let p95 = sorted[p95_index];
    let max = sorted[sorted.len() - 1];

    println!(
        "summary,count={},p95DnaMinusSubscribeEventsMs={},maxDnaMinusSubscribeEventsMs={}",
        sorted.len(),
        p95,
        max
    );
}

fn signed_duration_ms(later: Instant, earlier: Instant) -> i128 {
    if later >= earlier {
        later.duration_since(earlier).as_millis() as i128
    } else {
        -(earlier.duration_since(later).as_millis() as i128)
    }
}

async fn run_direct_subscribe_events(
    args: StarknetLiveLatencyArgs,
    tx: mpsc::Sender<EventObservation>,
    ct: CancellationToken,
) -> Result<(), BenchmarkError> {
    let (ws_stream, _) = tokio_tungstenite::connect_async(&args.direct_ws_url)
        .await
        .change_context(BenchmarkError)
        .attach_printable("failed to connect direct Starknet websocket")?;
    let (mut write, mut read) = ws_stream.split();

    let mut keys = vec![vec![normalize_hex_result(&args.game_event_key)?]];
    if let Some(adventurer_id) = &args.adventurer_id {
        keys.push(vec![normalize_hex_result(adventurer_id)?]);
    }

    let subscribe = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "starknet_subscribeEvents",
        "params": {
            "from_address": normalize_hex_result(&args.game_core_address)?,
            "keys": keys,
            "finality_status": "PRE_CONFIRMED"
        }
    });

    write
        .send(WsMessage::Text(subscribe.to_string().into()))
        .await
        .change_context(BenchmarkError)
        .attach_printable("failed to send starknet_subscribeEvents request")?;

    while !ct.is_cancelled() {
        tokio::select! {
            _ = ct.cancelled() => break,
            message = read.next() => {
                let Some(message) = message else {
                    break;
                };
                let message = message
                    .change_context(BenchmarkError)
                    .attach_printable("direct Starknet websocket read failed")?;
                let WsMessage::Text(text) = message else {
                    continue;
                };

                if let Some(observation) = parse_direct_event(&text) {
                    if tx.send(observation).await.is_err() {
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn run_dna_starknet_events(
    args: StarknetLiveLatencyArgs,
    tx: mpsc::Sender<EventObservation>,
    ct: CancellationToken,
) -> Result<(), BenchmarkError> {
    let mut client = DnaStreamClient::connect(args.stream_url.clone())
        .await
        .change_context(BenchmarkError)
        .attach_printable("failed to connect DNA stream")?;

    let game_core_address = starknet::FieldElement::from_hex(&args.game_core_address)
        .change_context(BenchmarkError)
        .attach_printable("failed to parse game core address")?;
    let game_event_key = starknet::FieldElement::from_hex(&args.game_event_key)
        .change_context(BenchmarkError)
        .attach_printable("failed to parse game event key")?;
    let adventurer_id = args
        .adventurer_id
        .as_deref()
        .map(starknet::FieldElement::from_hex)
        .transpose()
        .change_context(BenchmarkError)
        .attach_printable("failed to parse adventurer id")?;

    let mut event_keys = vec![starknet::Key {
        value: Some(game_event_key),
    }];
    if let Some(adventurer_id) = adventurer_id {
        event_keys.push(starknet::Key {
            value: Some(adventurer_id),
        });
    }

    let filter = starknet::Filter {
        header: starknet::HeaderFilter::OnData as i32,
        events: vec![starknet::EventFilter {
            address: Some(game_core_address),
            keys: event_keys,
            strict: Some(false),
            include_receipt: Some(true),
            include_transaction: Some(false),
            ..Default::default()
        }],
        ..Default::default()
    };

    let mut request = StreamDataRequest {
        finality: Some(DataFinality::Pending as i32),
        filter: vec![filter.encode_to_vec()],
        ..Default::default()
    }
    .into_request();

    if let Some(bearer_token) = args.bearer_token {
        let authorization_value = format!("Bearer {bearer_token}");
        let authorization_value = AsciiMetadataValue::from_str(&authorization_value)
            .change_context(BenchmarkError)
            .attach_printable("failed to parse authorization value")?;
        request
            .metadata_mut()
            .insert("authorization", authorization_value);
    }

    let stream = client
        .stream_data(request)
        .await
        .change_context(BenchmarkError)
        .attach_printable("failed to start DNA stream")?
        .into_inner()
        .take_until(async move { ct.cancelled().await });
    tokio::pin!(stream);

    while let Some(message) = stream.try_next().await.change_context(BenchmarkError)? {
        let Some(stream_data_response::Message::Data(data)) = message.message else {
            continue;
        };

        let finality = DataFinality::try_from(data.finality)
            .map(|f| format!("{f:?}"))
            .unwrap_or_else(|_| format!("UNKNOWN({})", data.finality));

        for block_bytes in data.data {
            let block = starknet::Block::decode(block_bytes.as_ref())
                .change_context(BenchmarkError)
                .attach_printable("failed to decode Starknet DNA block")?;
            let block_number = block.header.as_ref().map(|header| header.block_number);

            for event in block.events {
                if let Some(identity) = dna_event_identity(&event) {
                    let observation = EventObservation {
                        identity,
                        seen_at: Instant::now(),
                        block_number,
                        finality: finality.clone(),
                        event_index: Some(event.event_index),
                    };

                    if tx.send(observation).await.is_err() {
                        return Ok(());
                    }
                }
            }
        }
    }

    Ok(())
}

fn parse_direct_event(text: &str) -> Option<EventObservation> {
    let value: Value = serde_json::from_str(text).ok()?;
    if value.get("method").and_then(Value::as_str)? != "starknet_subscriptionEvents" {
        return None;
    }

    let result = value.get("params")?.get("result")?;

    let transaction_hash = normalize_hex_value(result.get("transaction_hash")?.as_str()?)?;
    let event_index_in_transaction = result.get("event_index")?.as_u64()?.try_into().ok()?;
    let from_address = normalize_hex_value(result.get("from_address")?.as_str()?)?;
    let keys = normalize_hex_array(result.get("keys")?)?;
    let data = normalize_hex_array(result.get("data")?)?;
    let block_number = result.get("block_number").and_then(Value::as_u64);
    let finality = result
        .get("finality_status")
        .and_then(Value::as_str)
        .unwrap_or("UNKNOWN")
        .to_string();

    Some(EventObservation {
        identity: EventIdentity {
            transaction_hash,
            event_index_in_transaction,
            from_address,
            keys,
            data,
        },
        seen_at: Instant::now(),
        block_number,
        finality,
        event_index: None,
    })
}

fn dna_event_identity(event: &starknet::Event) -> Option<EventIdentity> {
    Some(EventIdentity {
        transaction_hash: event.transaction_hash.as_ref()?.to_hex(),
        event_index_in_transaction: event.event_index_in_transaction,
        from_address: event.from_address.as_ref()?.to_hex(),
        keys: event
            .keys
            .iter()
            .map(starknet::FieldElement::to_hex)
            .collect(),
        data: event
            .data
            .iter()
            .map(starknet::FieldElement::to_hex)
            .collect(),
    })
}

fn normalize_hex_array(value: &Value) -> Option<Vec<String>> {
    value
        .as_array()?
        .iter()
        .map(|item| normalize_hex_value(item.as_str()?))
        .collect()
}

fn normalize_hex_result(value: &str) -> Result<String, BenchmarkError> {
    Ok(starknet::FieldElement::from_hex(value)
        .change_context(BenchmarkError)?
        .to_hex())
}

fn normalize_hex_value(value: &str) -> Option<String> {
    starknet::FieldElement::from_hex(value)
        .ok()
        .map(|felt| felt.to_hex())
}

trait Stats {
    type Block: Message + Default;
    fn new(index: usize) -> Self;
    fn record(&mut self, item: Self::Block);
    fn print_summary(&self);
}

struct EvmStats {
    pub index: usize,
    pub block_number: u64,
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
            block_number: 0,
            blocks: 0,
            bytes: 0,
            transactions: 0,
            receipts: 0,
            logs: 0,
            withdrawals: 0,
        }
    }

    fn record(&mut self, block: evm::Block) {
        self.block_number = block
            .header
            .as_ref()
            .map(|h| h.block_number)
            .unwrap_or_default();
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
            latest_block = %self.block_number,
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
    pub block_number: u64,
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
            block_number: 0,
            blocks: 0,
            bytes: 0,
            transactions: 0,
            receipts: 0,
            events: 0,
            messages: 0,
        }
    }

    fn record(&mut self, block: starknet::Block) {
        self.block_number = block
            .header
            .as_ref()
            .map(|h| h.block_number)
            .unwrap_or_default();
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
            latest_block = %self.block_number,
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
