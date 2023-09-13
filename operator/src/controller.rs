use std::fmt::Debug;

use futures::{Future, Stream, StreamExt};
use k8s_openapi::api;
use kube::{
    api::ListParams,
    core::Resource,
    runtime::{
        controller::{self, Action},
        reflector::ObjectRef,
        watcher, Controller,
    },
    Api, Client,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::{
    configuration::Configuration,
    context::{Context, OperatorError},
    crd::Indexer,
    reconcile,
};

pub async fn start(
    client: Client,
    configuration: Configuration,
    ct: CancellationToken,
) -> Result<(), OperatorError> {
    info!("Starting controller");

    let ctx = Context {
        client,
        configuration,
    };

    let indexers = Api::<Indexer>::all(ctx.client.clone());

    if indexers.list(&ListParams::default()).await.is_err() {
        error!("Indexer CRD not installed");
        return Err(OperatorError::CrdNotInstalled("Indexer".to_string()));
    }

    info!("CRD installed. Starting controllor loop");

    let pods = Api::<api::core::v1::Pod>::all(ctx.client.clone());

    let controller = Controller::new(indexers, watcher::Config::default())
        .owns(pods, watcher::Config::default())
        .graceful_shutdown_on(async move {
            ct.cancelled().await;
        })
        .run(
            reconcile::reconcile_indexer,
            reconcile::error_policy,
            ctx.into(),
        );

    run_controller_to_end(controller).await;

    Ok(())
}

type ReconcileItem<K> =
    Result<(ObjectRef<K>, Action), controller::Error<OperatorError, watcher::Error>>;

fn run_controller_to_end<K>(
    controller_stream: impl Stream<Item = ReconcileItem<K>>,
) -> impl Future<Output = ()>
where
    K: Resource + Debug,
    <K as Resource>::DynamicType: Debug,
{
    controller_stream.for_each(|res| async move {
        match res {
            Ok((obj, action)) => info!(obj = ?obj, action = ?action, "reconcile success"),
            Err(err) => warn!(err = ?err, "reconcile error"),
        }
    })
}
