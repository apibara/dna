use std::{net::SocketAddr, pin::Pin};

use futures::{Future, FutureExt};
use tonic::transport::Server;
use tracing::info;

use crate::{
    application,
    error::{ApibaraError, Result},
};

pub fn start_server() -> Result<(Pin<Box<dyn Future<Output = Result<()>>>>, SocketAddr)> {
    let application_manager = application::application_manager_service();
    let addr: SocketAddr = "0.0.0.0:50051".parse().unwrap();

    let handle = Server::builder()
        .add_service(application_manager)
        .serve(addr)
        .map(|r| r.map_err(|_| ApibaraError::RpcServerError));

    let handle = Box::pin(handle);

    Ok((handle, addr))
}
