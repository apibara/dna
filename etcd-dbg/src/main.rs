use std::time::Duration;

use apibara_etcd::{EtcdClient, EtcdClientOptions, LockOptions};
use apibara_observability::init_opentelemetry;
use clap::{Parser, Subcommand};
use error_stack::{Result, ResultExt};
use tokio_util::sync::CancellationToken;
use tracing::info;

#[derive(Debug)]
struct CliError;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// ETCD distributed lock.
    Lock {
        #[arg(long)]
        key: String,
        #[arg(long, value_delimiter = ',', default_value = "http://localhost:2379")]
        endpoints: Vec<String>,
        #[arg(long)]
        ttl: Option<i64>,
        #[arg(long)]
        prefix: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<(), CliError> {
    init_opentelemetry(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"))
        .change_context(CliError)
        .attach_printable("failed to initialize opentelemetry")?;

    let args = Cli::parse();

    match args.command {
        Command::Lock {
            key,
            endpoints,
            ttl,
            prefix,
        } => {
            let ct = CancellationToken::new();
            let options = EtcdClientOptions { prefix, auth: None };
            let client = EtcdClient::connect(endpoints, options)
                .await
                .change_context(CliError)
                .attach_printable("failed to connect to etcd")?;

            let lock_options = LockOptions {
                ttl: ttl.unwrap_or(60),
            };

            let mut lock_client = client.lock_client(lock_options);

            while !ct.is_cancelled() {
                info!("acquiring lock");

                let Some(mut lock) = lock_client
                    .lock(key.clone(), ct.clone())
                    .await
                    .change_context(CliError)?
                else {
                    break;
                };

                info!("acquired lock");

                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    if lock_client
                        .is_locked(&lock)
                        .await
                        .change_context(CliError)?
                    {
                        info!("lock is still held");
                        lock.keep_alive().await.change_context(CliError)?;
                    } else {
                        info!("lock is no longer held");
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

impl error_stack::Context for CliError {}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cli error")
    }
}
