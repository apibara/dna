use std::{io::ErrorKind, process};

use apibara_sink_common::{load_script, FullOptionsFromScript, ScriptOptions};
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
    transform: ScriptOptions,
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
    let script_options = args
        .transform
        .load_environment_variables()
        .change_context(CliError)
        .attach_printable("failed to parse script options")?
        .into_indexer_options();

    let mut script = load_script(&args.script, script_options).change_context(CliError)?;

    // Load the configuration from the script, but we don't need the full options yet.
    let configuration = script
        .configuration::<FullOptionsFromScript<DummyOptions>>()
        .await
        .change_context(CliError)?;

    // Delegate running the indexer to the sink command.
    let sink_type = configuration.sink.sink_type;
    let sink_command = get_sink_command(&sink_type);

    // Add back the script/transform arguments if specified.
    // TODO: There must be a better way to do this.
    let mut extra_args = args.args;
    if let Some(allow_env) = args.transform.allow_env {
        extra_args.push("--allow-env".to_string());
        extra_args.push(allow_env.to_string_lossy().to_string());
    };
    if let Some(allow_env_from_env) = args.transform.allow_env_from_env {
        extra_args.push("--allow-env-from-env".to_string());
        extra_args.push(allow_env_from_env.join(",").to_string());
    }
    if let Some(allow_net) = args.transform.allow_net {
        extra_args.push("--allow-net".to_string());
        extra_args.push(allow_net.join(",").to_string());
    };
    if let Some(allow_read) = args.transform.allow_read {
        extra_args.push("--allow-read".to_string());
        extra_args.push(allow_read.join(",").to_string());
    };
    if let Some(allow_write) = args.transform.allow_write {
        extra_args.push("--allow-write".to_string());
        extra_args.push(allow_write.join(",").to_string());
    };
    if let Some(transform_timeout) = args.transform.script_transform_timeout_seconds {
        extra_args.push("--script-transform-timeout-seconds".to_string());
        extra_args.push(transform_timeout.to_string());
    }
    if let Some(load_timeout) = args.transform.script_load_timeout_seconds {
        extra_args.push("--script-load-timeout-seconds".to_string());
        extra_args.push(load_timeout.to_string());
    }

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
