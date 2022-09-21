use anyhow::Result;

use apibara_node::{
    application::{pb, Application},
    node::Node,
};

struct SimpleApplication {}

#[derive(Debug, thiserror::Error)]
pub enum SimpleApplicationError {
}

#[apibara_node::async_trait]
impl Application for SimpleApplication {
    type Error = SimpleApplicationError;

    async fn init(&self, request: pb::InitRequest) -> Result<pb::InitResponse, Self::Error> {
        let input = pb::InputStream {
            id: 0,
            url: "goerli.starknet.stream.apibara.com:443".to_string(),
            starting_sequence: 0
        };
        Ok(pb::InitResponse {
            inputs: vec![input]
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let app = SimpleApplication {};
    Node::with_application(app).start().await?;
    Ok(())
}
