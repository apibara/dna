use std::{io::ErrorKind, process};

use apibara_observability::{init_error_handler, init_opentelemetry};
use apibara_sink_common::{
    apibara_cli_style, load_environment_variables, load_script, DotenvOptions,
    FullOptionsFromScript, ScriptOptions,
};
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
    #[clap(flatten)]
    dotenv: DotenvOptions,
    /// Arguments forwarded to the indexer.
    args: Vec<String>,
}

async fn run(args: RunArgs) -> color_eyre::eyre::Result<()> {
    // While not recommended, the script may return a different sink based on some env variable. We
    // need to load the environment variables before loading the script.
    let allow_env = load_environment_variables(&args.dotenv)?;
    let script_options = ScriptOptions { allow_env };

    let mut script = load_script(&args.script, script_options)?;

    // Load the configuration from the script, but we don't need the full options yet.
    let configuration = script
        .configuration::<FullOptionsFromScript<DummyOptions>>()
        .await?;

    // Delegate running the indexer to the sink command.
    let sink_type = configuration.sink.sink_type;
    let sink_command = format!("apibara-sink-{}", sink_type);

    // Add back the `--env-from-file` argument if specified.
    let mut extra_args = args.args;
    if let Some(env_from_file) = args.dotenv.env_from_file {
        extra_args.push("--env-from-file".to_string());
        extra_args.push(env_from_file.to_string_lossy().to_string());
    };

    let command_res = process::Command::new(&sink_command)
        .arg("run")
        .arg(args.script)
        .args(extra_args)
        .spawn();

    match command_res {
        Ok(mut child) => {
            child.wait()?;
            Ok(())
        }
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
async fn main() -> color_eyre::eyre::Result<()> {
    init_error_handler()?;
    init_opentelemetry()?;

    let args = Cli::parse();
    match args.subcommand {
        Command::Run(args) => run(args).await,
    }
}
