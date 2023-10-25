use std::{io::ErrorKind, process};

use apibara_sink_common::{
    load_environment_variables, load_script, DotenvOptions, FullOptionsFromScript, ScriptOptions,
};
use clap::Args;
use colored::*;
use error_stack::{Result, ResultExt};
use serde::Deserialize;

use crate::{error::CliError, paths::plugins_dir};

#[derive(Args, Debug)]
#[clap(trailing_var_arg = true, allow_hyphen_values = true)]
pub struct RunArgs {
    /// The path to the indexer script.
    script: String,
    #[clap(flatten)]
    dotenv: DotenvOptions,
    /// Arguments forwarded to the indexer.
    args: Vec<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct DummyOptions {
    pub sink_type: String,
}

pub async fn run(args: RunArgs) -> Result<(), CliError> {
    // While not recommended, the script may return a different sink based on some env variable. We
    // need to load the environment variables before loading the script.
    let allow_env = load_environment_variables(&args.dotenv).change_context(CliError)?;
    let script_options = ScriptOptions { allow_env };

    let mut script = load_script(&args.script, script_options).change_context(CliError)?;

    // Load the configuration from the script, but we don't need the full options yet.
    let configuration = script
        .configuration::<FullOptionsFromScript<DummyOptions>>()
        .await
        .change_context(CliError)?;

    // Delegate running the indexer to the sink command.
    let sink_type = configuration.sink.sink_type;
    let sink_command = get_sink_command(&sink_type);

    // Add back the `--allow-env` argument if specified.
    let mut extra_args = args.args;
    if let Some(allow_env) = args.dotenv.allow_env {
        extra_args.push("--allow-env".to_string());
        extra_args.push(allow_env.to_string_lossy().to_string());
    };

    let command_res = process::Command::new(sink_command)
        .arg("run")
        .arg(args.script)
        .args(extra_args)
        .spawn();

    match command_res {
        Ok(mut child) => {
            child.wait().change_context(CliError)?;
            Ok(())
        }
        Err(err) => {
            if let ErrorKind::NotFound = err.kind() {
                eprintln!(
                    "{} {} {}",
                    "Sink".red(),
                    sink_type,
                    "is not installed".red()
                );
                eprintln!(
                    "Install it with {} or by adding it to your $PATH",
                    format!("`apibara plugins install sink-{}`", sink_type).green()
                );
                std::process::exit(1);
            }
            Err(err)
                .change_context(CliError)
                .attach_printable("error while running sink")
        }
    }
}

fn get_sink_command(sink_type: &str) -> String {
    let dir = plugins_dir();
    let binary = format!("apibara-sink-{}", sink_type);

    // If the user hasn't installed the plugin, try to invoke from path.
    let installed = dir.join(&binary);
    if installed.exists() {
        return installed.to_string_lossy().to_string();
    } else {
        binary
    }
}
