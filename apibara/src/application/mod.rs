//! Applications that handle on-chain and off-chain data.
use anyhow::{Context, Error, Result};
use futures::StreamExt;
use once_cell::sync::Lazy;
use regex::Regex;
use std::sync::Arc;
use tracing::info;

use crate::{
    chain::starknet::StarkNetProvider,
    head_tracker::{HeadTracker, Message as HeadMessage},
    indexer::IndexerConfig,
};

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
        // TODO: create provider based on user configuration.
        let provider = Arc::new(StarkNetProvider::new("http://localhost:9545")?);
        let head_tracker = HeadTracker::new(provider.clone());
        let (head_tracker_handle, head_stream) = head_tracker
            .start()
            .await
            .context("failed to start head tracker")?;

        tokio::pin!(head_stream);
        tokio::pin!(head_tracker_handle);

        loop {
            tokio::select! {
                _ = &mut head_tracker_handle => {
                    return Err(Error::msg("head tracker service stopped"))
                }
                msg = head_stream.next() => {
                    match msg {
                        Some(HeadMessage::NewBlock(block)) => {
                            info!("⛏️ {} {}", block.number, block.hash);
                        }
                        Some(HeadMessage::Reorg(block)) => {
                            info!("🤕 {} {}", block.number, block.hash);
                        }
                        None => {
                            return Err(Error::msg("head stream interrupted"))
                        }
                    }
                }
            }
        }
    }
}
