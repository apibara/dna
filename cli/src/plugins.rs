use std::{
    env, fs,
    os::unix::prelude::PermissionsExt,
    path::{Path, PathBuf},
    process,
};

use clap::{Args, Subcommand};
use color_eyre::eyre::{eyre, Context, Result};
use colored::*;
use tabled::{settings::Style, Table, Tabled};

use crate::paths::plugins_dir;

#[derive(Debug, Args)]
pub struct PluginsArgs {
    #[command(subcommand)]
    subcommand: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Install a new plugin.
    Install(InstallArgs),
    /// List all installed plugins.
    List(ListArgs),
    /// Remove an installed plugin.
    Remove(RemoveArgs),
    /// Update one or all installed plugins.
    Update,
}

#[derive(Debug, Tabled)]
#[tabled(rename_all = "SCREAMING_SNAKE_CASE")]
struct PluginInfo {
    name: String,
    kind: String,
    version: String,
}

#[derive(Debug, Args)]
pub struct InstallArgs {
    /// The name of the plugin to install, e.g. `sink-postgres`.
    name: Option<String>,
    /// Install the plugin from the given file.
    #[arg(long, short = 'f')]
    file: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct ListArgs {}

#[derive(Debug, Args)]
pub struct RemoveArgs {
    /// The type of plugin to remove, e.g. `sink`.
    kind: String,
    /// The name of the plugin to remove, e.g. `mongo`.
    name: String,
}

pub async fn run(args: PluginsArgs) -> Result<()> {
    match args.subcommand {
        Command::Install(args) => run_install(args).await,
        Command::List(args) => run_list(args),
        Command::Remove(args) => run_remove(args),
        Command::Update => {
            eprintln!("{}", "Updating plugins is not yet supported".red());
            process::exit(1);
        }
    }
}

async fn run_install(args: InstallArgs) -> Result<()> {
    let dir = plugins_dir();
    fs::create_dir_all(dir)?;
    let cwd = env::current_dir()?;

    if let Some(file) = args.file {
        install_from_file(cwd.join(file))?;
    } else if let Some(_name) = args.name {
        eprintln!(
            "{}",
            "Installing plugins by name is not yet supported".red()
        );
        process::exit(1);
    }

    Ok(())
}

fn install_from_file(file: impl AsRef<Path>) -> Result<()> {
    let (name, version) =
        get_binary_name_version(&file).wrap_err("Failed to get plugin name and version")?;

    println!("Installing {} v{}", name, version);

    let target = plugins_dir().join(name);
    // Copy the binary content to a new file to avoid copying the permissions.
    let content = fs::read(&file)?;
    fs::write(&target, content)?;
    fs::set_permissions(&target, fs::Permissions::from_mode(0o755))?;

    println!("Plugin installed to {}", target.display());

    Ok(())
}

fn run_list(_args: ListArgs) -> Result<()> {
    let dir = plugins_dir();
    let plugins = get_plugins(dir)?;

    let table = Table::new(plugins).with(Style::rounded()).to_string();
    println!("{}", table);

    Ok(())
}

fn run_remove(args: RemoveArgs) -> Result<()> {
    let dir = plugins_dir();
    let plugin = PluginInfo::from_kind_name(args.kind, args.name);
    let plugin_path = dir.join(plugin.binary_name());

    let (name, version) =
        get_binary_name_version(&plugin_path).wrap_err("Failed to get plugin name and version")?;

    println!("Removing {} v{}", name, version);
    fs::remove_file(plugin_path)?;

    Ok(())
}

fn get_plugins(dir: impl AsRef<Path>) -> Result<Vec<PluginInfo>> {
    if !dir.as_ref().is_dir() {
        return Ok(vec![]);
    }

    let mut plugins = Vec::default();
    for file in fs::read_dir(dir)? {
        let file = file?;

        let metadata = file.metadata()?;
        if !metadata.is_file() || !metadata.permissions().mode() & 0o111 != 0 {
            eprintln!(
                "{} {:?}",
                "Plugins directory contains non-executable file".yellow(),
                file.path()
            );
            continue;
        }

        let (name, version) = get_binary_name_version(file.path())?;
        let info = PluginInfo::from_name_version(name, version)?;
        plugins.push(info);
    }

    Ok(plugins)
}

/// Runs the given plugin binary to extract the name and version.
fn get_binary_name_version(file: impl AsRef<Path>) -> Result<(String, String)> {
    let output = process::Command::new(file.as_ref())
        .arg("--version")
        .output()?;

    let output = String::from_utf8(output.stdout)?;
    let (name, version) = output
        .trim()
        .split_once(' ')
        .ok_or_else(|| eyre!("Plugin --version output does not match spec"))?;
    Ok((name.to_string(), version.to_string()))
}

impl PluginInfo {
    pub fn from_kind_name(kind: String, name: String) -> Self {
        Self {
            name,
            kind,
            version: String::default(),
        }
    }

    pub fn from_name_version(name: String, version: String) -> Result<Self> {
        let mut parts = name.splitn(3, '-');
        let _ = parts.next().ok_or_else(|| eyre!("Plugin name is empty"))?;
        let kind = parts
            .next()
            .ok_or_else(|| eyre!("Plugin name does not contain a kind"))?
            .to_string();
        let name = parts
            .next()
            .ok_or_else(|| eyre!("Plugin name does not contain a kind"))?
            .to_string();

        Ok(Self {
            name,
            version,
            kind,
        })
    }

    pub fn binary_name(&self) -> String {
        format!("apibara-{}-{}", self.kind, self.name)
    }
}
