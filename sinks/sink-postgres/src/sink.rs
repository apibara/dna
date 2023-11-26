use std::fmt;

use apibara_core::node::v1alpha2::Cursor;
use apibara_sink_common::{Context, CursorAction, DisplayCursor, Sink, ValueExt};
use async_trait::async_trait;
use error_stack::{Result, ResultExt};
use native_tls::{Certificate, TlsConnector};
use postgres_native_tls::MakeTlsConnector;
use serde_json::Value;
use tokio_postgres::types::Json;
use tokio_postgres::{Client, NoTls, Statement};
use tracing::{debug, info, warn};

use crate::configuration::{InvalidateColumn, TlsConfiguration};
use crate::{SinkPostgresConfiguration, SinkPostgresOptions};

#[derive(Debug)]
pub struct SinkPostgresError;
impl error_stack::Context for SinkPostgresError {}

impl fmt::Display for SinkPostgresError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("postgres sink operation failed")
    }
}

pub struct PostgresSink {
    inner: PostgresSinkInner,
}

enum PostgresSinkInner {
    Standard(StandardSink),
    Entity(EntitySink),
}

impl PostgresSink {
    pub fn client(&self) -> &Client {
        match self.inner {
            PostgresSinkInner::Standard(ref sink) => &sink.client,
            PostgresSinkInner::Entity(ref sink) => &sink.client,
        }
    }
}

#[async_trait]
impl Sink for PostgresSink {
    type Options = SinkPostgresOptions;
    type Error = SinkPostgresError;

    async fn from_options(options: Self::Options) -> Result<Self, Self::Error> {
        info!("connecting to database");
        let config = options.to_postgres_configuration()?;

        // Notice that all `connector` and `connection` types are different, so it's easier/cleaner
        // to just connect and spawn a connection inside each branch.
        let client = match config.tls {
            TlsConfiguration::NoTls => {
                info!("Using insecure connection");
                let (client, connection) = config
                    .pg
                    .connect(NoTls)
                    .await
                    .change_context(SinkPostgresError)
                    .attach_printable("failed to connect to postgres (no tls)")?;
                tokio::spawn(connection);
                client
            }
            TlsConfiguration::Tls {
                ref certificate,
                accept_invalid_hostnames,
                accept_invalid_certificates,
                disable_system_roots,
                use_sni,
            } => {
                info!("Configure TLS connection");
                let mut builder = TlsConnector::builder();

                if let Some(ref certificate) = certificate {
                    let certificate = tokio::fs::read(certificate)
                        .await
                        .change_context(SinkPostgresError)
                        .attach_printable_lazy(|| {
                            format!("failed to read tls certificate at {certificate:?}")
                        })?;
                    let certificate = Certificate::from_pem(&certificate)
                        .change_context(SinkPostgresError)
                        .attach_printable("failed to build certificate from PEM file")?;
                    builder.add_root_certificate(certificate);
                }

                if let Some(accept_invalid_certificates) = accept_invalid_certificates {
                    builder.danger_accept_invalid_certs(accept_invalid_certificates);
                }

                if let Some(disable_system_roots) = disable_system_roots {
                    builder.disable_built_in_roots(disable_system_roots);
                }

                if let Some(accept_invalid_hostnames) = accept_invalid_hostnames {
                    builder.danger_accept_invalid_hostnames(accept_invalid_hostnames);
                }

                if let Some(use_sni) = use_sni {
                    builder.use_sni(use_sni);
                }

                let connector = builder
                    .build()
                    .change_context(SinkPostgresError)
                    .attach_printable("failed to build tls connector")?;
                let connector = MakeTlsConnector::new(connector);
                let (client, connection) = config
                    .pg
                    .connect(connector)
                    .await
                    .change_context(SinkPostgresError)
                    .attach_printable("failed to connect to postgres (tls)")?;
                tokio::spawn(connection);
                client
            }
        };

        info!("client connected successfully");

        if config.entity_mode {
            let inner = EntitySink::new(client, &config).await?;
            Ok(Self {
                inner: PostgresSinkInner::Entity(inner),
            })
        } else {
            let inner = StandardSink::new(client, &config).await?;
            Ok(Self {
                inner: PostgresSinkInner::Standard(inner),
            })
        }
    }

    async fn handle_data(
        &mut self,
        ctx: &Context,
        batch: &Value,
    ) -> Result<CursorAction, Self::Error> {
        match self.inner {
            PostgresSinkInner::Standard(ref mut sink) => sink.handle_data(ctx, batch).await,
            PostgresSinkInner::Entity(ref mut sink) => sink.handle_data(ctx, batch).await,
        }
    }

    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error> {
        match self.inner {
            PostgresSinkInner::Standard(ref mut sink) => sink.handle_invalidate(cursor).await,
            PostgresSinkInner::Entity(ref mut sink) => sink.handle_invalidate(cursor).await,
        }
    }
}

struct StandardSink {
    pub client: Client,
    insert_statement: Statement,
    delete_statement: Statement,
    delete_all_statement: Statement,
}

impl StandardSink {
    async fn new(
        client: Client,
        config: &SinkPostgresConfiguration,
    ) -> Result<Self, SinkPostgresError> {
        let table_name = &config.table_name;
        let query = format!(
            "INSERT INTO {} SELECT * FROM json_populate_recordset(NULL::{}, $1::json)",
            &table_name, &table_name
        );

        let additional_conditions: String = if config.invalidate.is_empty() {
            "".into()
        } else {
            // TODO: this is quite fragile. It should properly format the column name
            // and value.
            config.invalidate.iter().fold(
                String::default(),
                |acc, InvalidateColumn { column, value }| {
                    format!("{acc} AND \"{column}\" = '{value}'")
                },
            )
        };

        let delete_query = format!(
            "DELETE FROM {} WHERE _cursor > $1 {}",
            table_name, additional_conditions
        );
        let delete_all_query = format!(
            "DELETE FROM {} WHERE true {}",
            table_name, additional_conditions
        );

        let insert_statement = client
            .prepare(&query)
            .await
            .change_context(SinkPostgresError)
            .attach_printable("failed to prepare insert data query")?;

        let delete_statement = client
            .prepare(&delete_query)
            .await
            .change_context(SinkPostgresError)
            .attach_printable("failed to prepare invalidate data query")?;

        let delete_all_statement = client
            .prepare(&delete_all_query)
            .await
            .change_context(SinkPostgresError)
            .attach_printable("failed to prepare invalidate all query")?;

        Ok(Self {
            client,
            insert_statement,
            delete_statement,
            delete_all_statement,
        })
    }

    async fn handle_data(
        &mut self,
        ctx: &Context,
        batch: &Value,
    ) -> Result<CursorAction, SinkPostgresError> {
        debug!(ctx = %ctx, "handling data");

        let Some(batch) = batch.as_array_of_objects() else {
            warn!("data is not an array of objects, skipping");
            return Ok(CursorAction::Persist);
        };

        if batch.is_empty() {
            return Ok(CursorAction::Persist);
        }

        let batch = batch
            .iter()
            .map(|value| {
                // Safety: we know that the batch is an array of objects
                let mut value = value.as_object().expect("value is an object").clone();
                value.insert("_cursor".into(), ctx.end_cursor.order_key.into());
                value
            })
            .collect::<Vec<_>>();

        self.client
            .execute(&self.insert_statement, &[&Json(batch)])
            .await
            .change_context(SinkPostgresError)
            .attach_printable("failed to run insert data query")?;

        Ok(CursorAction::Persist)
    }

    async fn handle_invalidate(
        &mut self,
        cursor: &Option<Cursor>,
    ) -> Result<(), SinkPostgresError> {
        debug!(cursor = %DisplayCursor(cursor), "handling invalidate");

        if let Some(cursor) = cursor {
            // convert to i64 because that's the tokio_postgres type that maps to bigint
            let block_number = i64::try_from(cursor.order_key).unwrap();
            self.client
                .execute(&self.delete_statement, &[&block_number])
                .await
                .change_context(SinkPostgresError)
                .attach_printable("failed to run invalidate data query")?;
        } else {
            self.client
                .execute(&self.delete_all_statement, &[])
                .await
                .change_context(SinkPostgresError)
                .attach_printable("failed to run invalidate all data query")?;
        }

        Ok(())
    }
}

struct EntitySink {
    client: Client,
    table_name: String,
}

impl EntitySink {
    async fn new(
        client: Client,
        config: &SinkPostgresConfiguration,
    ) -> Result<Self, SinkPostgresError> {
        if !config.invalidate.is_empty() {
            return Err(SinkPostgresError)
                .attach_printable("invalidate option is not supported in entity mode")
                .attach_printable("contact us on Discord to request this feature");
        }

        Ok(EntitySink {
            client,
            table_name: config.table_name.clone(),
        })
    }

    async fn handle_data(
        &mut self,
        ctx: &Context,
        batch: &Value,
    ) -> Result<CursorAction, SinkPostgresError> {
        debug!(ctx = %ctx, "handling data");

        let Some(batch) = batch.as_array_of_objects() else {
            warn!("data is not an array of objects, skipping");
            return Ok(CursorAction::Persist);
        };

        if batch.is_empty() {
            return Ok(CursorAction::Persist);
        }

        let txn = self
            .client
            .transaction()
            .await
            .change_context(SinkPostgresError)
            .attach_printable("failed to create postgres transaction")?;

        for item in batch {
            let Some(item) = item.as_object() else {
                warn!("item is not an object, skipping");
                continue;
            };

            if let Some(mut new_data) = item.get("insert").cloned() {
                if item.get("update").is_some() {
                    warn!("insert data contains update key. ignoring update data");
                }

                let Some(new_data) = new_data.as_object_mut() else {
                    warn!("insert data is not an object, skipping");
                    continue;
                };

                new_data.insert(
                    "_cursor".into(),
                    format!("[{},)", ctx.end_cursor.order_key).into(),
                );

                let query = format!(
                    "INSERT INTO {} SELECT * FROM json_populate_record(NULL::{}, $1::json)",
                    &self.table_name, &self.table_name
                );

                txn.execute(&query, &[&Json(new_data)])
                    .await
                    .change_context(SinkPostgresError)
                    .attach_printable("failed to run insert data query")?;
            } else if let Some(update) = item.get("update").cloned() {
                let Some(update) = update.as_object() else {
                    warn!("update data is not an object, skipping");
                    continue;
                };

                if item.get("insert").is_some() {
                    warn!("update data contains insert key. ignoring insert data");
                }

                let Some(entity) = item.get("entity") else {
                    warn!("update data does not contain entity key, skipping");
                    continue;
                };

                let Some(entity) = entity.as_object() else {
                    warn!("entity is not an object, skipping");
                    continue;
                };

                // Since the `entity` is a json-object, we cannot use it directly in the query.
                // We simulate filtering the records by joining with a single record.
                let join_columns = entity
                    .iter()
                    .map(|(k, _)| format!("t.{} = f.{}", k, k))
                    .collect::<Vec<_>>()
                    .join(" AND ");

                let query = format!(
                    "
                    SELECT row_to_json(x) AS value FROM (
                        SELECT t.*
                        FROM json_populate_record(NULL::{}, $1::json) f
                        LEFT JOIN {} t ON {}
                        WHERE upper_inf(t._cursor)
                    ) x
                    ",
                    &self.table_name, &self.table_name, &join_columns
                );

                let row = txn
                    .query_one(&query, &[&Json(entity)])
                    .await
                    .change_context(SinkPostgresError)
                    .attach_printable("failed to select existing entity")
                    .attach_printable("hint: do multiple entities with the same key exist?")?;

                // Clamp old data validity by updating its _cursor.
                {
                    let clamping_cursor = format!("[,{})", ctx.end_cursor.order_key);
                    let query = format!(
                        "
                        WITH f AS (
                            SELECT * FROM json_populate_record(NULL::{}, $1::json)
                        )
                        UPDATE {} t
                        SET _cursor = t._cursor * '{}'::int8range
                        FROM f
                        WHERE {} AND upper_inf(t._cursor)
                        ",
                        &self.table_name, &self.table_name, &clamping_cursor, &join_columns
                    );

                    txn.execute(&query, &[&Json(entity)])
                        .await
                        .change_context(SinkPostgresError)
                        .attach_printable("failed to clamp entity data")?;
                }

                // Update the existing row with the new rows.
                let mut duplicated = row
                    .try_get::<_, serde_json::Value>(0)
                    .change_context(SinkPostgresError)
                    .attach_printable("failed to get existing entity")?;

                {
                    let data = duplicated.as_object_mut().ok_or(SinkPostgresError)?;

                    for (k, v) in update {
                        data.insert(k.clone(), v.clone());
                    }

                    // Update _cursor as well.
                    data.insert(
                        "_cursor".into(),
                        format!("[{},)", ctx.end_cursor.order_key).into(),
                    );
                }

                {
                    let query = format!(
                        "INSERT INTO {} SELECT * FROM json_populate_record(NULL::{}, $1::json)",
                        &self.table_name, &self.table_name
                    );

                    // Insert duplicated + updated entity.
                    txn.execute(&query, &[&Json(duplicated)])
                        .await
                        .change_context(SinkPostgresError)
                        .attach_printable("failed to duplicate entity")?;
                }
            } else {
                warn!("item does not contain insert or update key, skipping");
            }
        }

        txn.commit()
            .await
            .change_context(SinkPostgresError)
            .attach_printable("failed to commit transaction")?;

        Ok(CursorAction::Persist)
    }

    async fn handle_invalidate(
        &mut self,
        cursor: &Option<Cursor>,
    ) -> Result<(), SinkPostgresError> {
        let cursor_lb = cursor
            .as_ref()
            .map(|c| c.order_key + 1) // add 1 because we compare with >=
            .unwrap_or(0) as i64;

        let txn = self
            .client
            .transaction()
            .await
            .change_context(SinkPostgresError)
            .attach_printable("failed to create postgres transaction")?;

        // delete data generated after the new head.
        let delete_query = format!(
            "DELETE FROM {} WHERE lower(_cursor) >= $1",
            &self.table_name,
        );

        txn.execute(&delete_query, &[&cursor_lb])
            .await
            .change_context(SinkPostgresError)
            .attach_printable("failed to run delete query on invalidate")?;

        // restore _cursor for data updated after the new head.
        let unclamp_query = format!(
            "
            UPDATE {}
            SET _cursor = concat('[', lower(_cursor), ',)')::int8range
            WHERE upper(_cursor) >= $1
            ",
            &self.table_name
        );

        txn.execute(&unclamp_query, &[&cursor_lb])
            .await
            .change_context(SinkPostgresError)
            .attach_printable("failed to run unclamp query on invalidate")?;

        txn.commit()
            .await
            .change_context(SinkPostgresError)
            .attach_printable("failed to commit transaction")?;

        Ok(())
    }
}
