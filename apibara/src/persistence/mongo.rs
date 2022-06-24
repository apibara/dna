//! MongoDB based persistence

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use mongodb::{
    bson::{doc, Bson},
    Client, Collection,
};
use tracing::debug;

use crate::{
    indexer::{IndexerPersistence, State as IndexerState},
    persistence::Id,
};

#[derive(Debug)]
pub struct MongoPersistence {
    client: Arc<Client>,
    db_name: String,
}

impl MongoPersistence {
    /// Create a new `MongoPersistence`.
    pub async fn new_with_uri(uri: impl AsRef<str>) -> Result<Self> {
        Self::new(uri, "apibara_admin").await
    }

    pub async fn new(uri: impl AsRef<str>, db: impl ToString) -> Result<Self> {
        let client = Client::with_uri_str(uri).await?;
        Ok(MongoPersistence {
            client: Arc::new(client),
            db_name: db.to_string(),
        })
    }

    fn indexers_collection(&self) -> Collection<IndexerState> {
        let db = self.client.database(&self.db_name);
        db.collection::<IndexerState>("indexers")
    }
}

#[async_trait]
impl IndexerPersistence for MongoPersistence {
    async fn get_indexer(&self, id: &Id) -> Result<Option<IndexerState>> {
        debug!(id=?id.to_str(), "mongo get indexer");
        let state = self
            .indexers_collection()
            .find_one(doc! { "id": id }, None)
            .await?;
        Ok(state)
    }

    async fn create_indexer(&self, state: &IndexerState) -> Result<()> {
        debug!(id=?state.id.to_str(), "mongo create indexer");
        self.indexers_collection().insert_one(state, None).await?;
        Ok(())
    }

    async fn delete_indexer(&self, id: &Id) -> Result<()> {
        debug!(id=?id.to_str(), "mongo delete indexer");
        self.indexers_collection()
            .delete_one(doc! { "id": id }, None)
            .await?;
        Ok(())
    }

    async fn update_indexer_block(&self, id: &Id, new_indexed_block: u64) -> Result<()> {
        debug!(id=?id.to_str(), new_indexed_block, "mongo update indexer");
        // bson does not support u64
        let indexed_to_block = new_indexed_block as i64;
        self.indexers_collection()
            .update_one(
                doc! { "id": id },
                doc! { "$set": { "indexed_to_block": indexed_to_block } },
                None,
            )
            .await?;
        Ok(())
    }
}

impl From<Id> for Bson {
    fn from(id: Id) -> Self {
        Bson::String(id.into_string())
    }
}
