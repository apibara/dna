use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::{DisplayCursor, Sink, ValueExt};
use async_trait::async_trait;
use serde_json::Value;
use tokio_postgres::types::Json;
use tokio_postgres::{Client, NoTls, Statement};
use tracing::{info, warn};

use crate::SinkPostgresOptions;

#[derive(Debug, thiserror::Error)]
pub enum SinkPostgresError {
    #[error("Missing connection string")]
    MissingConnectionString,
    #[error("Missing table name")]
    MissingTableName,
    #[error("Postgres error: {0}")]
    Postgres(#[from] tokio_postgres::Error),
}

pub struct PostgresSink {
    client: Client,
    insert_statement: Statement,
    delete_statement: Statement,
    delete_all_statement: Statement,
}

#[async_trait]
impl Sink for PostgresSink {
    type Options = SinkPostgresOptions;
    type Error = SinkPostgresError;

    async fn from_options(options: Self::Options) -> Result<Self, Self::Error> {
        info!("postgres: connecting to database");
        let config = options.to_postgres_configuration()?;
        let table_name = config.table_name;

        // TODO: add flag to use tls
        let (client, connection) = config.pg.connect(NoTls).await?;
        tokio::spawn(connection);

        info!("postgres: client connected successfully");

        let query = format!(
            "INSERT INTO {} SELECT * FROM json_populate_recordset(NULL::{}, $1::json)",
            &table_name, &table_name
        );
        let delete_query = format!("DELETE FROM {} WHERE _cursor > $1", &table_name);
        let delete_all_query = format!("DELETE FROM {}", &table_name);

        let insert_statement = client.prepare(&query).await?;
        let delete_statement = client.prepare(&delete_query).await?;
        let delete_all_statement = client.prepare(&delete_all_query).await?;

        Ok(Self {
            client,
            insert_statement,
            delete_statement,
            delete_all_statement,
        })
    }

    async fn handle_data(
        &mut self,
        cursor: &Option<Cursor>,
        end_cursor: &Cursor,
        finality: &DataFinality,
        batch: &Value,
    ) -> Result<(), Self::Error> {
        info!(
            cursor = %DisplayCursor(cursor),
            end_cursor = %end_cursor,
            finality = ?finality,
            "postgres: handling data"
        );

        let Some(values) = batch.as_array_of_objects() else {
            warn!("data is not an array of objects, skipping");
            return Ok(());
        };

        let batch = values
            .iter()
            .map(|value| {
                // Safety: we know that the batch is an array of objects
                let mut value = value.as_object().expect("value is an object").clone();
                value.insert("_cursor".into(), end_cursor.order_key.into());
                value
            })
            .collect::<Vec<_>>();

        self.client
            .execute(&self.insert_statement, &[&Json(batch)])
            .await?;

        Ok(())
    }

    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error> {
        info!(cursor = %DisplayCursor(cursor), "postgres: handling invalidate");

        if let Some(cursor) = cursor {
            // convert to i64 because that's the tokio_postgres type that maps to bigint
            let block_number = i64::try_from(cursor.order_key).unwrap();
            self.client
                .execute(&self.delete_statement, &[&block_number])
                .await?;
        } else {
            self.client.execute(&self.delete_all_statement, &[]).await?;
        }

        Ok(())
    }
}
