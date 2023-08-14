use std::str::FromStr;

use apibara_sink_common::SinkOptions;
use clap::Args;
use tokio_postgres::Config;

use crate::sink::SinkPostgresError;

#[derive(Debug)]
pub struct SinkPostgresConfiguration {
    pub pg: Config,
    pub table_name: String,
}

#[derive(Debug, Args, Default, SinkOptions)]
#[sink_options(tag = "postgres")]
pub struct SinkPostgresOptions {
    /// Connection string to the PostgreSQL server.
    #[arg(long, env = "POSTGRES_CONNECTION_STRING")]
    pub connection_string: Option<String>,
    /// Target table name.
    ///
    /// The table must exist and have a schema compatible with the data returned by the
    /// transformation step.
    #[arg(long, env = "POSTGRES_TABLE_NAME")]
    pub table_name: Option<String>,
}

impl SinkOptions for SinkPostgresOptions {
    fn merge(self, other: SinkPostgresOptions) -> Self {
        Self {
            connection_string: self.connection_string.or(other.connection_string),
            table_name: self.table_name.or(other.table_name),
        }
    }
}

impl SinkPostgresOptions {
    pub fn to_postgres_configuration(self) -> Result<SinkPostgresConfiguration, SinkPostgresError> {
        let connection_string = self
            .connection_string
            .ok_or_else(|| SinkPostgresError::MissingConnectionString)?;
        let pg = Config::from_str(&connection_string)?;
        let table_name = self
            .table_name
            .ok_or_else(|| SinkPostgresError::MissingTableName)?;
        Ok(SinkPostgresConfiguration { pg, table_name })
    }
}
