use error_stack::ResultExt;
use tokio_util::sync::CancellationToken;
use wings_metadata_core::admin::{Admin, TenantName};

use crate::{
    error::{CliError, CliResult},
    remote::RemoteArgs,
};

#[derive(clap::Subcommand)]
pub enum AdminCommands {
    /// Create a new tenant
    CreateTenant {
        /// Tenant name
        name: String,
        #[clap(flatten)]
        remote: RemoteArgs,
    },
    /// List all tenants
    ListTenants {
        #[clap(flatten)]
        remote: RemoteArgs,
    },
    /// Create a new namespace
    CreateNamespace {
        /// Tenant name
        tenant: String,
        /// Namespace name
        namespace: String,
        #[clap(flatten)]
        remote: RemoteArgs,
    },
    /// List namespaces for a tenant
    ListNamespaces {
        /// Tenant name
        tenant: String,
        #[clap(flatten)]
        remote: RemoteArgs,
    },
}

impl AdminCommands {
    pub async fn run(self, _ct: CancellationToken) -> CliResult<()> {
        match self {
            AdminCommands::CreateTenant { name, remote } => {
                println!(
                    "Creating tenant: {} (remote: {})",
                    name, remote.remote_address
                );

                let client = remote.admin_client().await?;
                let tenant_name =
                    TenantName::new(name).map_err(|e| CliError::InvalidConfiguration {
                        message: format!("invalid tenant name: {}", e),
                    })?;
                let tenant = client
                    .create_tenant(tenant_name)
                    .await
                    .change_context(CliError::Remote)?;

                println!("{}", tenant.name);

                Ok(())
            }
            AdminCommands::ListTenants { remote } => {
                println!("Listing all tenants (remote: {})", remote.remote_address);
                let client = remote.admin_client().await?;
                todo!();
            }
            AdminCommands::CreateNamespace {
                tenant,
                namespace,
                remote,
            } => {
                println!(
                    "Creating namespace '{}' for tenant '{}' (remote: {})",
                    namespace, tenant, remote.remote_address
                );
                let _client = remote.admin_client().await?;
                todo!("Implement namespace creation")
            }
            AdminCommands::ListNamespaces { tenant, remote } => {
                println!(
                    "Listing namespaces for tenant: {} (remote: {})",
                    tenant, remote.remote_address
                );
                let _client = remote.admin_client().await?;
                todo!("Implement namespace listing")
            }
        }
    }
}
