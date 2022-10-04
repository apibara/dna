use anyhow::Result;

use apibara_node::{
    application::{pb, Application},
    node::Node,
};

struct SimpleApplication {}

#[derive(Debug, thiserror::Error)]
pub enum SimpleApplicationError {}

#[apibara_node::async_trait]
impl Application for SimpleApplication {
    type Error = SimpleApplicationError;

    async fn init(&mut self, _request: pb::InitRequest) -> Result<pb::InitResponse, Self::Error> {
        let input = pb::InputStream {
            id: 0,
            url: "goerli.starknet.stream.apibara.com:443".to_string(),
            starting_sequence: 0,
        };
        Ok(pb::InitResponse {
            inputs: vec![input],
        })
    }

    async fn receive_data(
        &mut self,
        request: pb::ReceiveDataRequest,
    ) -> Result<pb::ReceiveDataResponse, Self::Error> {
        println!("got data {:?}", request.sequence);
        Ok(pb::ReceiveDataResponse {
            data: Vec::default(),
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let app = SimpleApplication {};
    Node::with_application(app).start().await?;
    Ok(())
}
