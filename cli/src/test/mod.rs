use std::path::{Path, PathBuf};

use apibara_sink_common::{DotenvOptions, StreamOptions};
use clap::Args;
use error_stack::{Result, ResultExt};
use tracing::warn;

use crate::error::CliError;

mod error;
mod run;
mod snapshot;

pub const SNAPSHOTS_DIR: &str = "snapshots";

#[derive(Args, Debug)]
pub struct TestArgs {
    /// An indexer script (.js/.ts), a snapshot file (.json) or a folder of snapshots.
    path: Option<PathBuf>,
    /// The number of blocks to stream.
    #[arg(long, short = 'b')]
    num_batches: Option<usize>,
    /// Regenerate the snapshot even if it already exists.
    #[arg(long, short = 'o', default_value_t = false)]
    r#override: bool,
    /// The name of the snapshot.
    #[arg(long, short = 'n')]
    name: Option<String>,
    /// Override the starting block from the script.
    #[arg(long, short, env)]
    starting_block: Option<u64>,
    #[clap(flatten)]
    stream_options: StreamOptions,
    #[clap(flatten)]
    dotenv_options: DotenvOptions,
}

fn validate_args(args: &TestArgs) -> Result<(), CliError> {
    if let Some(name) = &args.name {
        let is_invalid = name.contains(|c| {
            c == '/'
                || c == '.'
                || c == '\\'
                || c == ':'
                || c == '*'
                || c == '?'
                || c == '"'
                || c == '<'
                || c == '>'
                || c == '|'
        });
        if is_invalid {
            return Err(CliError).attach_printable(
                r#"Invalid name `{name}`, name should not contain  /, ., \, :, *, ?, ", <, >, or | as it'll be used to construct the snapshot path"#
            );
        }
    }

    if let Some(num_batches) = args.num_batches {
        if num_batches < 1 {
            return Err(CliError).attach_printable_lazy(|| {
                format!("Invalid number of blocks `{num_batches}`, it should be > 0")
            });
        }
    }

    if let Some(path) = &args.path {
        // Think about using try_exists instead
        if !path.exists() {
            return Err(CliError).attach_printable_lazy(|| {
                format!(
                    "Invalid path: `{}`, no such file or directory",
                    &path.display()
                )
            });
        }
    }

    Ok(())
}

pub fn warn_ignored_args(args: &TestArgs) {
    if args.starting_block.is_some()
        || args.num_batches.is_some()
        || args.r#override
        || args.stream_options.stream_url.is_some()
        || args.stream_options.max_message_size.is_some()
        || args.stream_options.metadata.is_some()
    {
        warn!(
            "The following arguments are ignored: --starting-block, --num-batches, \
            --override, --stream-url, --max-message-size, --metadata when running tests, \
            if you want to generate a snapshot with different options, \
            use the --override flag or give it a name with --name"
        )
    }
}

pub async fn run(args: TestArgs) -> Result<(), CliError> {
    validate_args(&args)?;

    match &args.path {
        Some(path) => {
            // Think about using try_exists instead
            if !path.exists() {
                return Err(CliError).attach_printable_lazy(|| {
                    format!(
                        "Invalid path: `{}`, no such file or directory",
                        &path.display()
                    )
                });
            }

            if path.is_dir() {
                warn_ignored_args(&args);
                run::run_all_tests(&path, &args.dotenv_options, None).await;
                return Ok(());
            }

            let extension = Path::new(&path)
                .extension()
                .ok_or(CliError)
                .attach_printable_lazy(|| format!("Invalid path: `{}`", path.display()))?;

            match extension.to_str().unwrap() {
                "json" => {
                    warn_ignored_args(&args);
                    run::run_single_test(path, None, None, &args.dotenv_options).await;
                },
                "js" | "ts" => {
                    let snapshot_path = args.name.clone()
                        .map(|name| Path::new(SNAPSHOTS_DIR).join(name).with_extension("json"))
                        .unwrap_or(snapshot::get_snapshot_path(path)?);

                    if args.r#override || !snapshot_path.exists() {
                        run::run_generate_snapshot(
                            path,
                            &snapshot_path,
                            args.starting_block,
                            args.num_batches,
                            &args.stream_options,
                            &args.dotenv_options,
                        ).await?;
                    } else {
                        warn_ignored_args(&args);
                        if args.name.is_some() {
                            run::run_single_test(&snapshot_path, None, Some(path), &args.dotenv_options).await;
                        } else {
                            run::run_all_tests(SNAPSHOTS_DIR, &args.dotenv_options, Some(path)).await;
                        }
                    }
                }
                _ => return Err(CliError).attach_printable_lazy(|| format!(
                    "Invalid file extension: `{}`, must be a .json for snapshots or .js / .ts for scripts",
                    path.display()
                )),
            }
        }
        None => {
            warn_ignored_args(&args);
            run::run_all_tests(SNAPSHOTS_DIR, &args.dotenv_options, None).await;
        }
    }

    Ok(())
}
