use std::{env, path::PathBuf};

/// Returns the local plugins directory.
pub fn plugins_dir() -> PathBuf {
    apibara_dir().join("plugins")
}

/// Returns the local apibara directory.
pub fn apibara_dir() -> PathBuf {
    match env::var("APIBARA_HOME") {
        Err(_) => dirs::data_local_dir()
            .expect("local data directory")
            .join("apibara"),
        Ok(path) => PathBuf::from(path),
    }
}
