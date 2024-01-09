use std::{fs, path::Path};

use anstyle::{AnsiColor, Style};
use apibara_observability::init_opentelemetry;
use apibara_script::{Script, ScriptOptions};
use clap::builder::Styles;
use error_stack::{report, Result, ResultExt};
use tokio_util::sync::CancellationToken;

use crate::{SinkError, SinkErrorReportExt, SinkErrorResultExt};

pub fn load_script_from_path(path: &Path, options: ScriptOptions) -> Result<Script, SinkError> {
    let path = path.to_string_lossy();
    load_script(&path, options)
}

/// Load a script from a file.
pub fn load_script(path: &str, options: ScriptOptions) -> Result<Script, SinkError> {
    let Ok(_) = fs::metadata(path) else {
        return Err(SinkError::load_script(&format!(
            "script file not found: {}",
            path
        )));
    };

    let current_dir = std::env::current_dir().load_script("failed to get current directory")?;

    let script = Script::from_file(path, current_dir, options).map_err(|err| {
        report!(err).load_script(&format!("failed to load script at path: {}", path))
    })?;

    Ok(script)
}

/// Initialize opentelemetry and the sigint (ctrl-c) handler.
pub fn initialize_sink(ct: CancellationToken) -> Result<(), SinkError> {
    init_opentelemetry()
        .map_err(|err| report!(err).configuration("failed to initialize opentelemetry"))?;

    set_ctrlc_handler(ct).map_err(|err| report!(err).fatal("failed to setup ctrl-c handler"))?;

    Ok(())
}

/// Connect the cancellation token to the ctrl-c handler.
pub fn set_ctrlc_handler(ct: CancellationToken) -> Result<(), ctrlc::Error> {
    ctrlc::set_handler({
        move || {
            ct.cancel();
        }
    })
    .attach_printable("failed to register ctrl-c handler")?;

    Ok(())
}

/// A clap style for all Apibara CLI applications.
pub fn apibara_cli_style() -> Styles {
    Styles::styled()
        .header(Style::new().bold().fg_color(Some(AnsiColor::Yellow.into())))
        .error(Style::new().bold().fg_color(Some(AnsiColor::Red.into())))
        .usage(Style::new().bold().fg_color(Some(AnsiColor::Yellow.into())))
        .literal(Style::new().fg_color(Some(AnsiColor::BrightCyan.into())))
        .placeholder(Style::new())
        .valid(Style::new().fg_color(Some(AnsiColor::BrightBlue.into())))
        .invalid(
            Style::new()
                .underline()
                .fg_color(Some(AnsiColor::Red.into())),
        )
}
