use std::{
    env, fs,
    io::ErrorKind,
    os::unix::prelude::PermissionsExt,
    path::{Path, PathBuf},
    process,
};

use async_compression::tokio::bufread::GzipDecoder;
use clap::{Args, Subcommand};
use color_eyre::eyre::{eyre, Context, Result};
use colored::*;
use futures::stream::TryStreamExt;
use reqwest::Url;
use tabled::{settings::Style, Table, Tabled};
use tokio_util::io::StreamReader;

use crate::paths::plugins_dir;

static GITHUB_REPO_ORG: &str = "apibara";
static GITHUB_REPO_NAME: &str = "dna";

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
    }
}

async fn run_install(args: InstallArgs) -> Result<()> {
    let dir = plugins_dir();
    fs::create_dir_all(dir)?;
    let cwd = env::current_dir()?;

    if let Some(file) = args.file {
        install_from_file(cwd.join(file))?;
    } else if let Some(name) = args.name {
        install_from_github(name).await?;
    }

    Ok(())
}

async fn install_from_github(name: String) -> Result<()> {
    let (kind, name) = name
        .split_once('-')
        .ok_or_else(|| eyre!("Plugin name must be in the format <kind>-<name>"))?;

    let releases = octocrab::instance()
        .repos(GITHUB_REPO_ORG, GITHUB_REPO_NAME)
        .releases()
        .list()
        .per_page(50)
        .send()
        .await
        .wrap_err("Failed to fetch GitHub releases")?;

    let tag_prefix = format!("{}-{}/", kind, name);
    let mut plugin_release = None;
    for release in octocrab::instance().all_pages(releases).await? {
        if release.tag_name.starts_with(&tag_prefix) {
            plugin_release = Some(release);
            break;
        }
    }

    let plugin_release = plugin_release.ok_or_else(|| {
        eyre!(
            "No release found for plugin {}-{}. Did you spell it correctly?",
            kind,
            name
        )
    })?;

    println!(
        "Found release {}",
        plugin_release
            .name
            .unwrap_or(plugin_release.tag_name)
            .green()
    );

    let info = PluginInfo::from_kind_name(kind.to_string(), name.to_string());

    let artifact_name = info.artifact_name(env::consts::OS, env::consts::ARCH);
    let asset = plugin_release
        .assets
        .iter()
        .find(|asset| asset.name == artifact_name)
        .ok_or_else(|| {
            eyre!(
                "No asset found for plugin {}-{} for your platform. OS={}, ARCH={}",
                kind,
                name,
                env::consts::OS,
                env::consts::ARCH
            )
        })?;

    println!("Downloading {}...", asset.name.blue());

    let target = plugins_dir().join(info.binary_name());
    download_artifact_to_path(asset.browser_download_url.clone(), &target).await?;

    println!("Plugin {} installed to {}", info.name, target.display());

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

        let (name, version) = get_binary_name_version(file.path()).wrap_err_with(|| {
            format!(
                "Failed to get plugin version: {}",
                file.file_name().to_string_lossy()
            )
        })?;
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

    pub fn artifact_name(&self, os: &str, arch: &str) -> String {
        format!("{}-{}-{}-{}.gz", self.kind, self.name, arch, os)
    }
}

async fn download_artifact_to_path(url: Url, dest: impl AsRef<Path>) -> Result<()> {
    let response = reqwest::get(url).await?;
    let stream = response
        .bytes_stream()
        .map_err(|err| std::io::Error::new(ErrorKind::Other, err));

    let stream_reader = StreamReader::new(stream);
    let mut decompressed = GzipDecoder::new(stream_reader);

    let mut file = tokio::fs::File::create(dest).await?;

    tokio::io::copy(&mut decompressed, &mut file).await?;

    file.set_permissions(fs::Permissions::from_mode(0o755))
        .await?;

    Ok(())
}
