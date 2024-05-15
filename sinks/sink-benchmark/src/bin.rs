use std::{fs, path::PathBuf, process::ExitCode, time::Instant};

use apibara_dna_protocol::{
    client::StreamMessage,
    dna::{
        common::Cursor,
        stream::{stream_data_response, DataFinality, StreamDataRequest},
    },
};
use apibara_sink_common::{
    apibara_cli_style, initialize_sink, log_system_message, NetworkFilterOptions, ReportExt,
    SinkError, StreamClientFactory, StreamOptions,
};
use byte_unit::{Byte, ByteUnit};
use clap::Parser;
use error_stack::{Result, ResultExt};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[cfg(not(windows))]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, styles = apibara_cli_style())]
struct Cli {
    /// The path to the configuration.
    #[arg(long, env)]
    filter_path: PathBuf,
    /// The starting block.
    #[arg(long, env)]
    starting_block: u64,
    #[command(flatten)]
    stream: StreamOptions,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args = Cli::parse();
    run_with_args(args).await.to_exit_code()
}

async fn run_with_args(args: Cli) -> Result<(), SinkError> {
    let ct = CancellationToken::new();
    initialize_sink(ct.clone())?;

    if !args.filter_path.exists() {
        return Err(SinkError::Configuration).attach_printable("filter file does not exist");
    }

    let file = fs::File::open(&args.filter_path)
        .change_context(SinkError::Configuration)
        .attach_printable("failed to open filter file")?;

    let filter: NetworkFilterOptions = serde_json::from_reader(file)
        .change_context(SinkError::Configuration)
        .attach_printable("failed to parse filter")?;

    if args.stream.stream_url.is_none() {
        return Err(SinkError::Configuration).attach_printable("stream url is required");
    }

    let starting_block = args.starting_block;
    let ending_block = args
        .stream
        .ending_block
        .ok_or(SinkError::Configuration)
        .attach_printable("ending block is required")?;

    if starting_block >= ending_block {
        return Err(SinkError::Configuration)
            .attach_printable("starting block must be less than ending block");
    }

    let request = StreamDataRequest {
        starting_cursor: Some(Cursor::new_finalized(starting_block)),
        finality: Some(DataFinality::Finalized as i32),
        filter: vec![filter.to_bytes()],
    };

    let stream_configuration = args
        .stream
        .to_stream_configuration()
        .change_context(SinkError::Configuration)
        .attach_printable("failed to create stream configuration")?;

    let stream_client_factory = StreamClientFactory::new(stream_configuration);
    let mut data_stream = stream_client_factory
        .new_stream_client()
        .await?
        .stream_data(request)
        .await
        .change_context(SinkError::Temporary)
        .attach_printable("failed to start stream")?;

    let mut handler = StreamMessageHandler::new(ending_block);

    loop {
        tokio::select! {
            _ = ct.cancelled() => {
                info!("sink stopped: cancelled");
                break;
            }
            stream_message = data_stream.try_next() => {
                match stream_message {
                    Err(err) => {
                        return Err(err)
                            .change_context(SinkError::Temporary)
                            .attach_printable("failed to receive stream message")
                    }
                    Ok(None) => {
                        info!("stream ended");
                        break;
                    }
                    Ok(Some(message)) => {
                        if !handler.handle_message(message).await? {
                            info!("benchmark completed");
                            break;
                        }

                        handler.interim_report();
                    }
                }
            }
        }
    }

    handler.report();

    Ok(())
}

struct StreamMessageHandler {
    ending_block: u64,
    total_block_count: u64,
    total_byte_count: u64,
    start_time: Instant,
    last_report: Instant,
}

impl StreamMessageHandler {
    fn new(ending_block: u64) -> Self {
        Self {
            ending_block,
            total_block_count: 0,
            total_byte_count: 0,
            start_time: Instant::now(),
            last_report: Instant::now(),
        }
    }

    async fn handle_message(&mut self, message: StreamMessage) -> Result<bool, SinkError> {
        use stream_data_response::Message;
        match message {
            Message::Data(data) => {
                self.total_block_count += 1;
                for block in data.data {
                    self.total_byte_count += block.len() as u64;
                }

                let end_cursor = data.end_cursor.unwrap_or_default();
                if end_cursor.order_key >= self.ending_block {
                    return Ok(false);
                }
            }
            Message::Heartbeat(_) => {}
            Message::Invalidate(_) => {
                return Err(SinkError::Fatal).attach_printable("received invalidate message");
            }
            Message::SystemMessage(system_message) => {
                log_system_message(system_message);
            }
        }

        Ok(true)
    }

    fn interim_report(&mut self) {
        if self.last_report.elapsed().as_secs() < 5 {
            return;
        }

        self.last_report = Instant::now();
        self.report();
    }

    fn report(&self) {
        let elapsed = self.start_time.elapsed();
        let elapsed_seconds = elapsed.as_secs_f64();

        if elapsed_seconds < 1.0 {
            return;
        }

        let total_bytes = Byte::from_unit(self.total_byte_count as f64, ByteUnit::B).unwrap();
        let blocks_sec = self.total_block_count as f64 / elapsed_seconds;
        let bytes_sec =
            Byte::from_unit(self.total_byte_count as f64 / elapsed_seconds, ByteUnit::B).unwrap();

        info!(
            elapsed = ?elapsed,
            block_count = self.total_block_count,
            total_bytes = %total_bytes.get_appropriate_unit(false).to_string(),
            blocks_sec,
            bytes_sec = %bytes_sec.get_appropriate_unit(false).to_string(),
            "benchmark report"
        );
    }
}
