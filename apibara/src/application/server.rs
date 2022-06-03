use std::pin::Pin;

use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{debug, info};

pub mod pb {
    tonic::include_proto!("apibara.application.v1alpha1");
}

use pb::{
    application_manager_server::ApplicationManagerServer, SubscribeLogsRequest,
    SubscribeLogsResponse,
};

type RpcResult<T> = Result<Response<T>, Status>;

type SubscribeLogsResponseStream =
    Pin<Box<dyn Stream<Item = Result<SubscribeLogsResponse, Status>> + Send>>;

pub fn application_manager_service() -> ApplicationManagerServer<ApplicationManager> {
    let manager = ApplicationManager {};
    ApplicationManagerServer::new(manager)
}

#[derive(Debug)]
pub struct ApplicationManager {}

#[tonic::async_trait]
impl pb::application_manager_server::ApplicationManager for ApplicationManager {
    type SubscribeLogsStream = SubscribeLogsResponseStream;

    async fn subscribe_logs(
        &self,
        request: Request<SubscribeLogsRequest>,
    ) -> RpcResult<Self::SubscribeLogsStream> {
        info!("subscribe_logs started: {:?}", request.remote_addr());
        todo!()
    }
}
