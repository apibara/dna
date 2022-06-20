//! Persist application state.

use anyhow::{Context, Error, Result};
use async_trait::async_trait;
use futures::TryStreamExt;
use mongodb::{bson::doc, Client, Collection};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, str::FromStr, sync::Arc};
use tracing::debug;

// The application id must be kebab-case
// must start with an alphabetic character
// can contain alphanumeric characters and -
// must end with an alphanumeric character
static APPLICATION_ID_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new("[[:alpha:]]([[:alnum:]]|-)*[[:alnum:]]").unwrap());

/// Unique application id.
// TODO: check id on deserialize. Flatten on serialize.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplicationId(String);

impl ApplicationId {
    pub fn new(s: &str) -> Result<ApplicationId> {
        if APPLICATION_ID_REGEX.is_match(s) {
            return Ok(ApplicationId(s.to_string()));
        }
        Err(Error::msg("invalid ApplicationId"))
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

/// Application state.
#[derive(Debug, Serialize, Deserialize)]
pub struct State {
    /// The unique application id.
    pub id: ApplicationId,
    /// The block up to which the application was indexed.
    pub indexed_to_block: Option<u64>,
    /// The block from which to start indexing.
    pub index_from_block: u64,
}

/// Persist the application state to storage.
#[async_trait]
pub trait ApplicationPersistence: Send + Sync {
    /// Get the state (if any) of the application with the given id.
    async fn get_state(&self, id: &ApplicationId) -> Result<Option<State>>;

    /// Write the application state, overwriting the existing application state.
    async fn write_state(&self, state: &State) -> Result<()>;

    /// Delete the application state.
    async fn delete_state(&self, id: &ApplicationId) -> Result<()>;

    /// List all applications.
    async fn list_states(&self) -> Result<Vec<State>>;
}

/// Persist application state to a MongoDB database.
pub struct MongoPersistence {
    client: Arc<Client>,
}

impl MongoPersistence {
    /// Create a new `MongoPersistence`.
    pub async fn new_with_uri(uri: impl AsRef<str>) -> Result<Self> {
        let client = Client::with_uri_str(uri).await?;
        Ok(MongoPersistence {
            client: Arc::new(client),
        })
    }

    fn states_collection(&self) -> Collection<State> {
        // TODO: configure database name from config.
        let db = self.client.database("apibara");
        db.collection::<State>("states")
    }
}

#[async_trait]
impl ApplicationPersistence for MongoPersistence {
    async fn get_state(&self, id: &ApplicationId) -> Result<Option<State>> {
        debug!("get mongo state: {:?}", id);
        let state = self.states_collection().find_one(doc! {}, None).await?;
        Ok(state)
    }

    async fn write_state(&self, state: &State) -> Result<()> {
        debug!("write mongo state: {:?}", state);
        self.states_collection().insert_one(state, None).await?;
        Ok(())
    }

    async fn delete_state(&self, id: &ApplicationId) -> Result<()> {
        debug!("delete mongo state: {:?}", id);
        self.states_collection().delete_one(doc! {}, None).await?;
        Ok(())
    }

    async fn list_states(&self) -> Result<Vec<State>> {
        debug!("list mongo states");
        let states = self
            .states_collection()
            .find(doc! {}, None)
            .await
            .context("failed to find states")?
            .try_collect::<Vec<State>>()
            .await
            .context("failed to collector mongo cursor")?;
        Ok(states)
    }
}

impl Display for ApplicationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ApplicationId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ApplicationId::new(s)
    }
}
