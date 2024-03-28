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
    /// The collections where to store the data.
    ///
    /// If this option is set, the `collection_name` option will be ignored.
    /// Use this option when writing to multiple collections from the same indexer.
    #[arg(long, env = "MONGO_COLLECTION_NAMES", value_delimiter = ',')]
    pub collection_names: Option<Vec<String>>,
    /// Enable storing records as entities.
    pub entity_mode: Option<bool>,
    /// Additional conditions to use when invalidating data.
    ///
    /// Use this option to run multiple indexers on the same collection.
    #[clap(skip)]
    pub invalidate: Option<Document>,
    /// The number of seconds to wait before flushing the batch.
    ///
    /// If this option is not set, the sink will flush the batch immediately.
    #[arg(long, env = "MONGO_BATCH_SECONDS")]
    pub batch_seconds: Option<u64>,
    /// Use a transaction to replace pending data.
    ///
    /// This option avoids data "flashing" when the previous pending data is replaced.
    /// Turning this flag on requires a MongoDB replica set. If you are using MongoDB
    /// Atlas, this is turned on by default. If you're using a standalone MongoDB it
    /// won't work.
    #[arg(long, env = "MONGO_REPLACE_DATA_INSIDE_TRANSACTION")]
    pub replace_data_inside_transaction: Option<bool>,
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
            batch_seconds: self.batch_seconds.or(other.batch_seconds),
            replace_data_inside_transaction: self
                .replace_data_inside_transaction
                .or(other.replace_data_inside_transaction),
        }
    }
}
