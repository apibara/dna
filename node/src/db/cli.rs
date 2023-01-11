//! # Extensions and functions to manage cli db options

use std::path::PathBuf;

/// Returns the path to the local node's data dir.
pub fn default_data_dir() -> Option<PathBuf> {
    dirs::data_local_dir().map(|d| d.join("apibara"))
}
