use clap::Args;
use tokio_util::sync::CancellationToken;

use crate::error::CliResult;

#[derive(Debug, Args)]
pub struct DevArgs {
    #[arg(long, default_value = "127.0.0.1:7777")]
    bind: String,
}

impl DevArgs {
    pub async fn run(self, ct: CancellationToken) -> CliResult<()> {
        println!("Starting Wings in development mode on {}", self.bind);

        // Create a child token for the dev service
        let service_ct = ct.child_token();
        let _drop_guard = service_ct.drop_guard();

        todo!("Implement development mode startup")
    }
}
