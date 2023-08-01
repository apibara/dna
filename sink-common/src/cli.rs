use std::fs;

use anstyle::{AnsiColor, Style};
use apibara_script::{Script, ScriptError, ScriptOptions};
use clap::builder::Styles;
use tokio_util::sync::CancellationToken;

#[derive(Debug, thiserror::Error)]
pub enum LoadScriptError {
    #[error("Script not found {0}")]
    FileNotFound(String),
    #[error("Script error: {0}")]
    ScriptError(#[from] ScriptError),
}

/// Load a script from a file.
pub fn load_script(path: &str, options: ScriptOptions) -> Result<Script, LoadScriptError> {
    let Ok(_) = fs::metadata(path) else {
        return Err(LoadScriptError::FileNotFound(path.to_owned()));
    };

    let current_dir = std::env::current_dir().expect("current directory");
    let script = Script::from_file(path, current_dir, options)?;
    Ok(script)
}

/// Connect the cancellation token to the ctrl-c handler.
pub fn set_ctrlc_handler(ct: CancellationToken) -> color_eyre::eyre::Result<()> {
    ctrlc::set_handler({
        move || {
            ct.cancel();
        }
    })?;

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
