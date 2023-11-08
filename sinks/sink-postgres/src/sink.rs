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
use tracing::{info, warn};

use crate::configuration::{InvalidateColumn, TlsConfiguration};
use crate::SinkPostgresOptions;

#[derive(Debug)]
pub struct SinkPostgresError;
impl error_stack::Context for SinkPostgresError {}

impl fmt::Display for SinkPostgresError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("postgres sink operation failed")
    }
}

pub struct PostgresSink {
    pub client: Client,
    insert_statement: Statement,
    delete_statement: Statement,
    delete_all_statement: Statement,
}

#[async_trait]
impl Sink for PostgresSink {
    type Options = SinkPostgresOptions;
    type Error = SinkPostgresError;

    async fn from_options(options: Self::Options) -> Result<Self, Self::Error> {
        info!("connecting to database");
        let config = options.to_postgres_configuration()?;
        let table_name = config.table_name;

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
                certificate,
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
    ) -> Result<CursorAction, Self::Error> {
        info!(ctx = ?ctx, "handling data");

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

    async fn handle_invalidate(&mut self, cursor: &Option<Cursor>) -> Result<(), Self::Error> {
        info!(cursor = %DisplayCursor(cursor), "handling invalidate");

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
