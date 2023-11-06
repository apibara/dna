use std::{path::PathBuf, str::FromStr};

use apibara_sink_common::SinkOptions;
use clap::Args;
use error_stack::{Result, ResultExt};
use serde::Deserialize;
use tokio_postgres::Config;

use crate::sink::SinkPostgresError;

#[derive(Debug)]
pub enum TlsConfiguration {
    NoTls,
    Tls {
        certificate: Option<PathBuf>,
        accept_invalid_certificates: Option<bool>,
        disable_system_roots: Option<bool>,
        accept_invalid_hostnames: Option<bool>,
        use_sni: Option<bool>,
    },
}

#[derive(Debug)]
pub struct SinkPostgresConfiguration {
    pub pg: Config,
    pub table_name: String,
    pub tls: TlsConfiguration,
    pub invalidate: Vec<InvalidateColumn>,
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
    /// Disable TLS when connecting to the PostgreSQL server.
    #[arg(long, env = "POSTGRES_NO_TLS")]
    pub no_tls: Option<bool>,
    /// Path to the PEM-formatted X509 TLS certificate file.
    #[arg(long, env = "POSTGRES_TLS_CERTIFICATE")]
    pub tls_certificate: Option<String>,
    /// Disable system root certificates.
    #[arg(long, env = "POSTGRES_TLS_DISABLE_SYSTEM_ROOTS")]
    pub tls_disable_system_roots: Option<bool>,
    /// Disable certificate validation.
    #[arg(long, env = "POSTGRES_TLS_ACCEPT_INVALID_CERTIFICATES")]
    pub tls_accept_invalid_certificates: Option<bool>,
    /// Disable hostname validation.
    #[arg(long, env = "POSTGRES_TLS_ACCEPT_INVALID_HOSTNAMES")]
    pub tls_accept_invalid_hostnames: Option<bool>,
    /// Use Server Name Indication (SNI).
    #[arg(long, env = "POSTGRES_TLS_USE_SNI")]
    pub tls_use_sni: Option<bool>,
    /// Additional conditions for the invalidate query.
    #[clap(skip)]
    pub invalidate: Option<Vec<InvalidateColumn>>,
}

#[derive(Debug, Default, Deserialize)]
pub struct InvalidateColumn {
    /// Column name.
    pub column: String,
    /// Column value.
    pub value: String,
}

impl SinkOptions for SinkPostgresOptions {
    fn merge(self, other: SinkPostgresOptions) -> Self {
        Self {
            connection_string: self.connection_string.or(other.connection_string),
            table_name: self.table_name.or(other.table_name),
            no_tls: self.no_tls.or(other.no_tls),
            tls_certificate: self.tls_certificate.or(other.tls_certificate),
            tls_disable_system_roots: self
                .tls_disable_system_roots
                .or(other.tls_disable_system_roots),
            tls_accept_invalid_certificates: self
                .tls_accept_invalid_certificates
                .or(other.tls_accept_invalid_certificates),
            tls_accept_invalid_hostnames: self
                .tls_accept_invalid_hostnames
                .or(other.tls_accept_invalid_hostnames),
            tls_use_sni: self.tls_use_sni.or(other.tls_use_sni),
            invalidate: self.invalidate.or(other.invalidate),
        }
    }
}

impl SinkPostgresOptions {
    pub fn to_postgres_configuration(self) -> Result<SinkPostgresConfiguration, SinkPostgresError> {
        let connection_string = self
            .connection_string
            .ok_or(SinkPostgresError)
            .attach_printable("missing connection string")?;
        let pg = Config::from_str(&connection_string)
            .change_context(SinkPostgresError)
            .attach_printable("failed to build postgres config from connection string")?;
        let table_name = self
            .table_name
            .ok_or(SinkPostgresError)
            .attach_printable("missing table name")?;

        let tls = if self.no_tls.unwrap_or(false) {
            TlsConfiguration::NoTls
        } else {
            TlsConfiguration::Tls {
                certificate: self.tls_certificate.map(PathBuf::from),
                accept_invalid_certificates: self.tls_accept_invalid_certificates,
                disable_system_roots: self.tls_disable_system_roots,
                accept_invalid_hostnames: self.tls_accept_invalid_hostnames,
                use_sni: self.tls_use_sni,
            }
        };

        let invalidate = self.invalidate.unwrap_or_default();

        Ok(SinkPostgresConfiguration {
            pg,
            table_name,
            tls,
            invalidate,
        })
    }
}
