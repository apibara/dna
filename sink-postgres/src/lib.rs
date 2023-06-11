use apibara_core::node::v1alpha2::{Cursor, DataFinality};
use apibara_sink_common::{is_array_of_objects, Sink};
use async_trait::async_trait;

use serde_json::Value;
use std::str::FromStr;
use tokio_postgres::types::Json;
use tokio_postgres::{Client, Config, NoTls, Statement};
use tracing::{info, warn};

type Result<T> = std::result::Result<T, tokio_postgres::Error>;

pub struct PostgresSink {
    client: Client,
    insert_statement: Statement,
    delete_statement: Statement,
    delete_all_statement: Statement,
}

impl PostgresSink {
    pub async fn new(connection_string: String, table_name: String) -> Result<Self> {
        info!("postgres: connecting to database");
        let config = Config::from_str(&connection_string)?;
        let (client, connection) = config.connect(NoTls).await?;
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
}

#[async_trait]
impl Sink for PostgresSink {
    type Error = tokio_postgres::Error;

    async fn handle_data(
        &mut self,
        cursor: &Option<Cursor>,
        end_cursor: &Cursor,
        finality: &DataFinality,
        batch: &Value,
    ) -> Result<()> {
        info!(
            cursor = %cursor.clone().unwrap_or_default(),
            end_cursor = %end_cursor,
            finality = ?finality,
            "postgres: handling data"
        );

        if !is_array_of_objects(batch) {
            warn!("data is not an array of objects, skipping");
            return Ok(());
        }

        let batch: Vec<Value> = batch
            .as_array()
            .unwrap()
            .iter()
            .map(|element| {
                // TODO: why so much clones and mut stuff in here ?
                let mut map = element.clone().as_object_mut().unwrap().clone();
                map.insert("_cursor".into(), end_cursor.order_key.into());
                Value::Object(map.clone())
            })
            .collect();

        self.client
            .execute(&self.insert_statement, &[&Json(batch)])
            .await?;

        Ok(())
    }

    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<()> {
        let cursor_str = cursor
            .clone()
            .map(|c| c.to_string())
            .unwrap_or("genesis".into());

        info!(cursor = %cursor_str, "postgres: handling invalidate");

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
