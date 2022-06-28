//! Apibara configuration.

use std::path::Path;

use anyhow::Result;
use figment::{
    providers::{Format, Serialized, Toml},
    Figment,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Configuration {
    pub admin: Admin,
    pub network: Network,
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

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Network {
    pub starknet: NetworkConfiguration,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct NetworkConfiguration {
    pub provider_url: String,
}

impl Configuration {
    pub fn from_path(path: &Path) -> Result<Configuration> {
        let config = Figment::from(Serialized::defaults(Configuration::default()))
            .merge(Toml::file(path))
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

impl Default for NetworkConfiguration {
    fn default() -> Self {
        NetworkConfiguration {
            provider_url: "https://starknet-goerli.apibara.com:443".to_string(),
        }
    }
}
