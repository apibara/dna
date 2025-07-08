use clap::Args;
use error_stack::{Result, ResultExt};
use tonic::transport::Channel;
use wings_metadata_core::admin::RemoteAdminService;

use crate::error::CliError;

/// Arguments for configuring the remote server connection.
#[derive(Args, Debug, Clone)]
pub struct RemoteArgs {
    /// The address of the remote Wings admin server
    #[arg(long, default_value = "http://localhost:7777")]
    pub remote_address: String,
}

impl RemoteArgs {
    /// Create a new gRPC client for the admin service.
    pub async fn admin_client(&self) -> Result<RemoteAdminService<Channel>, CliError> {
        let channel = Channel::from_shared(self.remote_address.clone())
            .change_context(CliError::InvalidConfiguration {
                message: format!("invalid remote address: {}", self.remote_address),
            })
            .attach_printable("failed to parse remote address as URI")?
            .connect()
            .await
            .change_context(CliError::AdminApi {
                message: "failed to connect to admin service".to_string(),
            })
            .attach_printable_lazy(|| {
                format!(
                    "failed to establish gRPC connection to {}",
                    self.remote_address
                )
            })?;

        Ok(RemoteAdminService::new(channel))
    }
}
