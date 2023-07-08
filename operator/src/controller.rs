use std::fmt::Debug;

use futures::{Future, Stream, StreamExt};
use kube::{
    core::Resource,
    runtime::{
        controller::{Action, Error as ControllerError},
        reflector::ObjectRef,
        watcher::Error as WatcherError,
    },
    Client,
};
use tracing::{info, warn};

use crate::{
    configuration::Configuration,
    reconcile::{Context, Error},
    sink::webhook,
};

pub type ReconcileItem<K> = Result<(ObjectRef<K>, Action), ControllerError<Error, WatcherError>>;

pub async fn start(client: Client, configuration: Configuration) -> Result<(), Error> {
    info!("controller started");

    let ctx = Context {
        client,
        configuration,
    };
    let webhook_controller = webhook::start_controller(ctx.clone()).await?;

    run_controller_to_end(webhook_controller).await;

    info!("controller terminated");
    Ok(())
}

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
            Err(err) => warn!(err = ?err, "reconcile failed"),
        }
    })
}
