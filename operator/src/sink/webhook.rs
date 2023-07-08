use std::{sync::Arc, time::Duration};

use futures::Stream;
use k8s_openapi::{
    api::{self, core::v1::EnvVarSource},
    apimachinery::pkg::{
        apis::meta::{self, v1::Condition},
        util::intstr::IntOrString,
    },
    chrono::{DateTime, Utc},
};
use kube::{
    api::{DeleteParams, ListParams, Patch, PatchParams},
    runtime::{controller::Action, finalizer, watcher::Config, Controller},
    Api, CustomResource, CustomResourceExt, Resource, ResourceExt,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tracing::{error, info, instrument, warn};

use super::common::{CommonSpec, CommonStatus};
use crate::reconcile::{Context, Error, ReconcileItem};

pub type HeaderValueSource = EnvVarSource;

static WEBHOOK_FINALIZER: &str = "sinkwebhook.apibara.com";

/// Run a sink that invokes a webhook for each batch of data.
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    kind = "SinkWebhook",
    group = "apibara.com",
    version = "v1alpha1",
    namespaced,
    printcolumn = r#"{"name": "Age", "type": "date", "jsonPath": ".metadata.creationTimestamp" }"#,
    printcolumn = r#"{"name": "Status", "type": "string", "jsonPath": ".status.phase" }"#,
    printcolumn = r#"{"name": "Instance", "type": "string", "jsonPath": ".status.instanceName" }"#,
    printcolumn = r#"{"name": "Restarts", "type": "number", "jsonPath": ".status.restartCount" }"#
)]
#[kube(status = "SinkWebhookStatus", shortname = "sinkwebhook")]
#[serde(rename_all = "camelCase")]
pub struct SinkWebhookSpec {
    #[serde(flatten)]
    pub common: CommonSpec,
    /// The target url to send the request to.
    pub target_url: String,
    /// Additional headers to send with the request.
    pub headers: Option<Vec<HeaderSpec>>,
}

#[derive(Deserialize, Serialize, Clone, Debug, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct HeaderSpec {
    /// The header name.
    pub name: String,
    /// The header value.
    pub value: Option<String>,
    /// Source for the header value.
    pub value_from: Option<HeaderValueSource>,
}

/// Most recent status of the webhook sink.
#[derive(Deserialize, Serialize, Clone, Debug, Default, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SinkWebhookStatus {
    #[serde(flatten)]
    pub common: CommonStatus,
}

impl SinkWebhook {
    #[instrument(skip_all)]
    async fn reconcile(&self, ctx: Arc<Context>) -> Result<Action, Error> {
        use api::core::v1::Pod;

        let ns = self.namespace().expect("webhook is namespaced");
        let name = self.name_any();

        let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), &ns);
        let webhooks: Api<SinkWebhook> = Api::namespaced(ctx.client.clone(), &ns);

        let metadata = self.object_metadata(&ctx);
        let spec = self.pod_spec(&ctx);
        let pod_manifest = Pod {
            metadata,
            spec: Some(spec),
            ..Pod::default()
        };

        let pod = pods
            .patch(
                &name,
                &PatchParams::apply("sinkwebhook"),
                &Patch::Apply(pod_manifest),
            )
            .await?;

        let pod_scheduled_condition = Condition {
            last_transition_time: pod
                .meta()
                .creation_timestamp
                .clone()
                .unwrap_or_else(|| meta::v1::Time(DateTime::<Utc>::MIN_UTC)),
            type_: "PodScheduled".to_string(),
            message: "Pod has been scheduled".to_string(),
            observed_generation: self.meta().generation,
            reason: "PodScheduled".to_string(),
            status: "True".to_string(),
        };

        let sink_restart_count = pod.status.as_ref().and_then(|status| {
            status
                .container_statuses
                .as_ref()
                .and_then(|statuses| statuses.first().map(|container| container.restart_count))
        });

        let status = json!({
            "status": SinkWebhookStatus {
                common: CommonStatus {
                    pod_created: pod.meta().creation_timestamp.clone(),
                    instance_name: pod.meta().name.clone(),
                    phase: Some("Running".to_string()),
                    conditions: Some(vec![pod_scheduled_condition]),
                    restart_count: sink_restart_count,
                }
            }
        });

        webhooks
            .patch_status(&name, &PatchParams::default(), &Patch::Merge(&status))
            .await?;

        Ok(Action::requeue(Duration::from_secs(10)))
    }

    #[instrument(skip_all)]
    async fn cleanup(&self, ctx: Arc<Context>) -> Result<Action, Error> {
        use api::core::v1::Pod;

        let ns = self.namespace().expect("webhook is namespaced");
        let name = self.name_any();
        let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), &ns);

        if let Some(_existing) = pods.get_opt(&name).await? {
            pods.delete(&name, &DeleteParams::default()).await?;
        }

        Ok(Action::requeue(Duration::from_secs(10)))
    }

    fn object_metadata(&self, _ctx: &Arc<Context>) -> meta::v1::ObjectMeta {
        use meta::v1::ObjectMeta;

        ObjectMeta {
            name: self.metadata.name.clone(),
            ..ObjectMeta::default()
        }
    }

    fn pod_spec(&self, ctx: &Arc<Context>) -> api::core::v1::PodSpec {
        use api::core::v1::{Container, ContainerPort, EnvVar, HTTPGetAction, PodSpec, Probe};

        let image = self
            .spec
            .common
            .image
            .as_ref()
            .and_then(|image| image.name.clone())
            .unwrap_or_else(|| ctx.configuration.webhook.image.clone());

        let image_pull_secrets = self
            .spec
            .common
            .image
            .as_ref()
            .and_then(|image| image.pull_secrets.clone());
        let image_pull_policy = self
            .spec
            .common
            .image
            .as_ref()
            .and_then(|image| image.pull_policy.clone());

        let probe = Probe {
            http_get: Some(HTTPGetAction {
                path: Some("/status".to_string()),
                port: IntOrString::Int(8118),
                scheme: Some("HTTP".to_string()),
                ..HTTPGetAction::default()
            }),
            ..Probe::default()
        };

        let args = vec!["--status-server-address=0.0.0.0:8118".to_string()];

        let mut volumes = vec![];
        let mut volume_mounts = vec![];
        let mut env = vec![EnvVar {
            name: "TARGET_URL".to_string(),
            value: Some(self.spec.target_url.clone()),
            ..EnvVar::default()
        }];

        // TODO: add headers environment variable, like METADATA

        env.extend(self.spec.common.to_env_var());

        if let Some((filter_volume, filter_mount, filter_env)) =
            self.spec.common.stream.filter_data()
        {
            volumes.push(filter_volume);
            volume_mounts.push(filter_mount);
            env.push(filter_env);
        }

        if let Some((transform_volume, transform_mount, transform_env)) =
            self.spec.common.stream.transform_data()
        {
            volumes.push(transform_volume);
            volume_mounts.push(transform_mount);
            env.push(transform_env);
        }

        let container = Container {
            name: "sink".to_string(),
            image: Some(image),
            args: Some(args),
            env: Some(env),
            ports: Some(vec![ContainerPort {
                container_port: 8118,
                name: Some("status".to_string()),
                ..ContainerPort::default()
            }]),
            image_pull_policy,
            liveness_probe: Some(probe.clone()),
            readiness_probe: Some(probe),
            volume_mounts: Some(volume_mounts),
            ..Container::default()
        };

        PodSpec {
            containers: vec![container],
            volumes: Some(volumes),
            image_pull_secrets,
            ..PodSpec::default()
        }
    }
}

async fn reconcile_webhook(webhook: Arc<SinkWebhook>, ctx: Arc<Context>) -> Result<Action, Error> {
    let ns = webhook.namespace().expect("webhook is namespaced");
    let webhooks: Api<SinkWebhook> = Api::namespaced(ctx.client.clone(), &ns);

    info!(
        webhook = %webhook.name_any(),
        namespace = %ns,
        "reconcile webhook sink",
    );

    finalizer::finalizer(&webhooks, WEBHOOK_FINALIZER, webhook, |event| async {
        use finalizer::Event::*;
        match event {
            Apply(webhook) => webhook.reconcile(ctx.clone()).await,
            Cleanup(webhook) => webhook.cleanup(ctx.clone()).await,
        }
    })
    .await
    .map_err(|err| Error::Finalizer(err.into()))
}

fn error_policy(_webhook: Arc<SinkWebhook>, error: &Error, _ctx: Arc<Context>) -> Action {
    warn!(error = ?error, "webhook reconcile error");
    Action::requeue(Duration::from_secs(30))
}

pub async fn start_controller(
    ctx: Context,
) -> Result<impl Stream<Item = ReconcileItem<SinkWebhook>>, Error> {
    let webhooks = Api::<SinkWebhook>::all(ctx.client.clone());

    if webhooks.list(&ListParams::default()).await.is_err() {
        error!("WebhookSink CRD not installed");
        return Err(Error::CrdNotInstalled(SinkWebhook::crd_name().to_string()));
    }

    info!("starting webhook sink controller");

    let pods = Api::<api::core::v1::Pod>::all(ctx.client.clone());
    let controller = Controller::new(webhooks, Config::default())
        .owns(pods, Config::default())
        .run(reconcile_webhook, error_policy, ctx.into());

    Ok(controller)
}
