//! # Extensions and functions to manage cli db options

use std::path::PathBuf;

use clap::{arg, Command};

pub trait DatabaseClapCommandExt {
    fn data_dir_args(self) -> Self;
}

impl<'help> DatabaseClapCommandExt for Command<'help> {
    fn data_dir_args(self) -> Self {
        self.arg(arg![-d --datadir <DIR> "Directory with node data"].required(false))
    }
}

/// Returns the path to the local node's data dir.
pub fn node_data_dir(name: &str) -> Option<PathBuf> {
    dirs::data_local_dir().map(|p| p.join("apibara").join(name))
}
