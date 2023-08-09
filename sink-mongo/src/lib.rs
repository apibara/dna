mod configuration;
mod sink;

pub use self::configuration::SinkMongoOptions;
pub use self::sink::{MockMongoCollection, MongoSink, SinkMongoError};
