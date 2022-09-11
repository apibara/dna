//! Apibara configuration.

use std::{collections::HashMap, path::Path};

use anyhow::Result;
use figment::{
    providers::{Env, Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

use crate::persistence::NetworkName;

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Configuration {
    pub admin: Admin,
    pub network: HashMap<NetworkName, Network>,
    pub server: Server,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Admin {
    pub storage: AdminStorage,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AdminStorage {
    pub connection_string: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Server {
    pub address: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Network {
    StarkNet(StarkNetNetwork),
    Ethereum(EthereumNetwork),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StarkNetNetwork {
    pub provider_url: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct EthereumNetwork {
    pub provider_url: String,
}

impl Configuration {
    pub fn from_path(path: &Path) -> Result<Configuration> {
        let config = Figment::from(Serialized::defaults(Configuration::default()))
            .merge(Toml::file(path))
            .merge(Env::prefixed("APIBARA_"))
            .extract()?;
        Ok(config)
    }
}

impl Default for AdminStorage {
    fn default() -> Self {
        AdminStorage {
            connection_string: "mongodb://localhost:27017".to_string(),
        }
    }
}

impl Default for Server {
    fn default() -> Self {
        Server {
            address: "0.0.0.0:7171".to_string(),
        }
    }
}
