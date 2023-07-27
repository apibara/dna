use std::{io::ErrorKind, process};

use apibara_sink_common::{apibara_cli_style, load_script, FullOptionsFromScript, ScriptOptions};
use clap::{Args, Parser, Subcommand};
use serde::Deserialize;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None, styles = apibara_cli_style())]
struct Cli {
    #[command(subcommand)]
    subcommand: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the specified indexer.
    Run(RunArgs),
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct DummyOptions {
    pub sink_type: String,
}

#[derive(Args, Debug)]
#[clap(trailing_var_arg = true, allow_hyphen_values = true)]
struct RunArgs {
    /// The path to the indexer script.
    script: String,
    /// Arguments forwarded to the indexer.
    args: Vec<String>,
}

async fn run(args: RunArgs) -> anyhow::Result<()> {
    let mut script = load_script(&args.script, ScriptOptions::default())?;
    // Load the configuration from the script, but we don't need the full options yet.
    let configuration = script
        .configuration::<FullOptionsFromScript<DummyOptions>>()
        .await?;

    // Delegate running the indexer to the sink command.
    let sink_type = configuration.sink.sink_type;
    let sink_command = format!("apibara-sink-{}", sink_type);

    let command_res = process::Command::new(&sink_command)
        .arg("run")
        .arg(args.script)
        .args(args.args)
        .spawn();
    match command_res {
        Ok(_) => Ok(()),
        Err(err) => {
            if let ErrorKind::NotFound = err.kind() {
                eprintln!("error: executable {} is not in your $PATH.", sink_command);
                std::process::exit(1);
            }
            Err(err.into())
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    match args.subcommand {
        Command::Run(args) => run(args).await,
    }
}
