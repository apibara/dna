//! Applications that handle on-chain and off-chain data.
use anyhow::{Error, Result};
use once_cell::sync::Lazy;
use regex::Regex;

use crate::indexer::IndexerConfig;

// The application id must be kebab-case
// must start with an alphabetic character
// can contain alphanumeric characters and -
// must end with an alphanumeric character
static APPLICATION_ID_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new("[[:alpha:]]([[:alnum:]]|-)*[[:alnum:]]").unwrap());

/// Unique application id.
#[derive(Debug, Clone)]
pub struct ApplicationId(String);

/// Application root object.
#[derive(Debug)]
pub struct Application {
    /// application id
    pub id: ApplicationId,
    pub indexer: IndexerConfig,
}

impl ApplicationId {
    pub fn new(s: &str) -> Result<ApplicationId> {
        if APPLICATION_ID_REGEX.is_match(s) {
            return Ok(ApplicationId(s.to_string()));
        }
        Err(Error::msg("invalid ApplicationId"))
    }
}

impl Application {
    /// Create a new application with the given id and indexer.
    pub fn new(id: ApplicationId, indexer: IndexerConfig) -> Application {
        Application { id, indexer }
    }

    pub async fn run(&self) -> Result<()> {
        futures::future::pending().await
    }
}
