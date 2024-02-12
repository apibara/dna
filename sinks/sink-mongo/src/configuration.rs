use apibara_sink_common::SinkOptions;
use clap::Args;
use mongodb::bson::Document;
use serde::Deserialize;

#[derive(Debug, Args, Default, SinkOptions)]
#[sink_options(tag = "mongo")]
pub struct SinkMongoOptions {
    /// The connection string to the MongoDB database.
    #[arg(long, env = "MONGO_CONNECTION_STRING")]
    pub connection_string: Option<String>,
    /// The database to use.
    #[arg(long, env = "MONGO_DATABASE")]
    pub database: Option<String>,
    /// The collection where to store the data.
    #[arg(
        long,
        env = "MONGO_COLLECTION_NAME",
        conflicts_with = "collection_names"
    )]
    pub collection_name: Option<String>,
    #[arg(long, env = "MONGO_COLLECTION_NAMES", value_delimiter = ',')]
    pub collection_names: Option<Vec<String>>,
    /// Enable storing records as entities.
    pub entity_mode: Option<bool>,
    #[clap(skip)]
    pub invalidate: Option<Document>,
    #[arg(long, env = "MONGO_BATCH_SECS")]
    pub batch_secs: Option<u64>,
}

impl SinkOptions for SinkMongoOptions {
    fn merge(self, other: Self) -> Self {
        Self {
            connection_string: self.connection_string.or(other.connection_string),
            database: self.database.or(other.database),
            collection_name: self.collection_name.or(other.collection_name),
            collection_names: self.collection_names.or(other.collection_names),
            entity_mode: self.entity_mode.or(other.entity_mode),
            invalidate: self.invalidate.or(other.invalidate),
            batch_secs: self.batch_secs.or(other.batch_secs),
        }
    }
}
