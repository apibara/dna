use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};

use error_stack::{Report, Result, ResultExt};
use k8s_openapi::{
    api::{self, core::v1::ServiceSpec},
    apimachinery::pkg::apis::meta::{self, v1::Condition},
    chrono::{DateTime, Utc},
    Metadata,
};
use kube::{
    api::{DeleteParams, Patch, PatchParams},
    core::Resource,
    runtime::{controller::Action, finalizer},
    Api, ResourceExt,
};
use serde_json::json;
use tracing::{info, instrument, warn};

use crate::{
    context::{Context, OperatorError},
    crd::{Indexer, IndexerSource, IndexerStatus, SinkType},
};

static INDEXER_FINALIZER: &str = "indexer.apibara.com";
static GIT_CLONE_IMAGE: &str = "docker.io/alpine/git:latest";

pub struct ReconcileError(Report<OperatorError>);

impl Indexer {
    #[instrument(skip_all)]
    async fn reconcile(&self, ctx: Arc<Context>) -> Result<Action, OperatorError> {
        use api::core::v1::{Pod, Service};

        let name = self.name_any();

        let ns = self
            .namespace()
            .ok_or(OperatorError)
            .attach_printable("failed to get namespace")
            .attach_printable_lazy(|| format!("indexer: {name}"))?;

        let indexers: Api<Indexer> = Api::namespaced(ctx.client.clone(), &ns);
        let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), &ns);
        let services: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);

        // Check if the indexer needs to be restarted.
        let (restart_increment, error_condition) = if let Some(pod_name) = self.instance_name() {
            self.maybe_delete_pod(pod_name, &pods).await?
        } else {
            (0, None)
        };

        let metadata = self.object_metadata(&ctx);

        let mut phase = if error_condition.is_some() {
            "Error".to_string()
        } else {
            "Running".to_string()
        };

        let mut conditions = Vec::default();
        let mut pod_created = None;
        let mut instance_name = None;
        let mut status_service_name = None;

        match self.pod_and_status_svc_spec(&ctx) {
            None => {
                let pod_scheduled_condition = Condition {
                    last_transition_time: self
                        .meta()
                        .creation_timestamp
                        .clone()
                        .unwrap_or(meta::v1::Time(DateTime::<Utc>::MIN_UTC)),
                    type_: "PodNotScheduled".to_string(),
                    message: "The specified indexer type doesn't exist.".to_string(),
                    observed_generation: self.meta().generation,
                    reason: "ConfigurationError".to_string(),
                    status: "False".to_string(),
                };

                conditions.push(pod_scheduled_condition);
                phase = "Error".to_string();
            }
            Some((spec, svc_spec)) => {
                let svc_name = self.status_service_name();
                let svc_metadata = kube::core::ObjectMeta {
                    name: svc_name.clone().into(),
                    ..metadata.clone()
                };

                let pod_manifest = Pod {
                    metadata,
                    spec: Some(spec),
                    ..Pod::default()
                };

                let pod = pods
                    .patch(
                        &name,
                        &PatchParams::apply("indexer"),
                        &Patch::Apply(pod_manifest),
                    )
                    .await
                    .change_context(OperatorError)
                    .attach_printable("failed to update pod")
                    .attach_printable_lazy(|| format!("indexer: {name}"))?;

                let svc_manifest = Service {
                    metadata: svc_metadata,
                    spec: Some(svc_spec),
                    ..Service::default()
                };

                let service = services
                    .patch(
                        &svc_name,
                        &PatchParams::apply("indexer"),
                        &Patch::Apply(svc_manifest),
                    )
                    .await
                    .change_context(OperatorError)
                    .attach_printable("failed to update service")
                    .attach_printable_lazy(|| format!("indexer: {name}"))?;

                let pod_scheduled_condition = Condition {
                    last_transition_time: pod
                        .meta()
                        .creation_timestamp
                        .clone()
                        .unwrap_or(meta::v1::Time(DateTime::<Utc>::MIN_UTC)),
                    type_: "PodScheduled".to_string(),
                    message: "Pod has been scheduled".to_string(),
                    observed_generation: self.meta().generation,
                    reason: "PodScheduled".to_string(),
                    status: "True".to_string(),
                };

                let status_service_created = Condition {
                    last_transition_time: service
                        .meta()
                        .creation_timestamp
                        .clone()
                        .unwrap_or(meta::v1::Time(DateTime::<Utc>::MIN_UTC)),
                    type_: "StatusServiceCreated".to_string(),
                    message: "Status service created".to_string(),
                    observed_generation: self.meta().generation,
                    reason: "StatusServiceCreated".to_string(),
                    status: "True".to_string(),
                };

                conditions.push(pod_scheduled_condition);
                conditions.push(status_service_created);
                pod_created = pod.meta().creation_timestamp.clone();
                instance_name = pod.metadata().name.clone();
                status_service_name = service.metadata().name.clone();
            }
        }

        if let Some(error_condition) = error_condition {
            conditions.push(error_condition);
        }

        let restart_count = self
            .status
            .as_ref()
            .map(|status| status.restart_count.unwrap_or_default() + restart_increment);

        let status = json!({
            "status": IndexerStatus {
                pod_created,
                instance_name,
                status_service_name,
                phase: Some(phase),
                conditions: Some(conditions),
                restart_count,
            }
        });

        indexers
            .patch_status(&name, &PatchParams::default(), &Patch::Merge(&status))
            .await
            .change_context(OperatorError)
            .attach_printable("failed to update status")
            .attach_printable_lazy(|| format!("indexer: {name}"))?;

        Ok(Action::requeue(Duration::from_secs(10)))
    }

    #[instrument(skip_all)]
    async fn cleanup(&self, ctx: Arc<Context>) -> Result<Action, OperatorError> {
        use api::core::v1::{Pod, Service};

        let name = self.name_any();
        let svc_name = self.status_service_name();

        let ns = self
            .namespace()
            .ok_or(OperatorError)
            .attach_printable("failed to get namespace")
            .attach_printable_lazy(|| format!("indexer: {name}"))?;

        let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), &ns);
        let services: Api<Service> = Api::namespaced(ctx.client.clone(), &ns);

        if let Some(_existing) = pods
            .get_opt(&name)
            .await
            .change_context(OperatorError)
            .attach_printable("failed to get pod")
            .attach_printable_lazy(|| format!("indexer: {name}"))?
        {
            pods.delete(&name, &DeleteParams::default())
                .await
                .change_context(OperatorError)
                .attach_printable("failed to delete pod")
                .attach_printable_lazy(|| format!("indexer: {name}"))?;
        }

        if let Some(_existing) = services
            .get_opt(&svc_name)
            .await
            .change_context(OperatorError)
            .attach_printable("failed to get service")
            .attach_printable_lazy(|| format!("indexer: {name}"))?
        {
            services
                .delete(&svc_name, &DeleteParams::default())
                .await
                .change_context(OperatorError)
                .attach_printable("failed to delete service")
                .attach_printable_lazy(|| format!("indexer: {name}"))?;
        }

        Ok(Action::requeue(Duration::from_secs(10)))
    }

    fn instance_name(&self) -> Option<&str> {
        self.status
            .as_ref()
            .and_then(|status| status.instance_name.as_ref())
            .map(|name| name.as_str())
    }

    /// Check the pod status and delete it if it has failed.
    ///
    /// The pod is deleted so that it can be recreated once the
    /// status of the indexer has been updated.
    async fn maybe_delete_pod(
        &self,
        pod_name: &str,
        pods: &Api<api::core::v1::Pod>,
    ) -> Result<(i32, Option<Condition>), OperatorError> {
        let Some(pod) = pods
            .get_opt(pod_name)
            .await
            .change_context(OperatorError)
            .attach_printable("failed to get pod")
            .attach_printable_lazy(|| format!("indexer: {pod_name}"))? else {
            return Ok((0, None));
        };

        let container_status = pod.status.as_ref().and_then(|status| {
            status
                .container_statuses
                .as_ref()
                .and_then(|statuses| statuses.first())
        });

        let Some(finished_at) = container_status
            .and_then(|cs| cs.state.as_ref())
            .and_then(|st| st.terminated.clone())
            .and_then(|ts| ts.finished_at) else {
                return Ok((0, None));
            };

        let elapsed = (Utc::now().time() - finished_at.0.time())
            .to_std()
            .unwrap_or_default();

        // For now, restart the indexer every minute.
        // TODO: Only restart it based on the container exit code.
        if elapsed > Duration::from_secs(60) {
            info!(pod = %pod.name_any(), "deleting pod for restart");

            pods.delete(pod_name, &Default::default())
                .await
                .change_context(OperatorError)
                .attach_printable("failed to delete pod")
                .attach_printable_lazy(|| format!("indexer: {pod_name}"))?;
            Ok((1, None))
        } else {
            let error_condition = Condition {
                last_transition_time: finished_at,
                type_: "PodTerminated".to_string(),
                message: "Pod has been terminated".to_string(),
                observed_generation: self.meta().generation,
                reason: "PodTerminated".to_string(),
                status: "False".to_string(),
            };

            Ok((0, Some(error_condition)))
        }
    }

    fn status_service_name(&self) -> String {
        self.name_any() + "-status"
    }

    fn object_metadata(&self, ctx: &Arc<Context>) -> meta::v1::ObjectMeta {
        use meta::v1::ObjectMeta;

        ObjectMeta {
            name: self.metadata.name.clone(),
            labels: self.pod_labels(ctx).into(),
            ..ObjectMeta::default()
        }
    }

    fn pod_labels(&self, _ctx: &Arc<Context>) -> BTreeMap<String, String> {
        let name = self.name_any();
        BTreeMap::from([
            ("app.kubernetes.io/name".to_string(), name.clone()),
            ("app.kubernetes.io/instance".to_string(), name),
        ])
    }

    fn pod_and_status_svc_spec(
        &self,
        ctx: &Arc<Context>,
    ) -> Option<(api::core::v1::PodSpec, api::core::v1::ServiceSpec)> {
        use api::core::v1::{PodSpec, ServicePort};

        // Initialize volume, volume mounts, and env vars.
        let mut volumes = Vec::new();
        let mut volume_mounts = Vec::new();
        let env = self.spec.env.clone().unwrap_or_default();

        if let Some(volumes_from_config) = self.spec.volumes.as_ref() {
            for volume in volumes_from_config {
                volumes.push(volume.volume.clone());
                volume_mounts.push(volume.volume_mount.clone());
            }
        }

        let mut init_containers = Vec::new();
        let mut containers = Vec::new();

        let (init_container, workdir) =
            self.source_container(&mut volumes, &mut volume_mounts, &env, ctx);

        if let Some(init_container) = init_container {
            init_containers.push(init_container);
        }

        let container = self.sink_container(&workdir, &volume_mounts, &env, ctx)?;
        containers.push(container);

        let pod_spec = PodSpec {
            init_containers: Some(init_containers),
            containers,
            volumes: Some(volumes),
            restart_policy: Some("Never".to_string()),
            ..PodSpec::default()
        };

        let svc_spec = ServiceSpec {
            selector: self.pod_labels(ctx).into(),
            ports: Some(vec![ServicePort {
                port: ctx.configuration.status_port,
                ..ServicePort::default()
            }]),
            ..ServiceSpec::default()
        };

        Some((pod_spec, svc_spec))
    }

    fn source_container(
        &self,
        volumes: &mut Vec<api::core::v1::Volume>,
        volume_mounts: &mut Vec<api::core::v1::VolumeMount>,
        env: &[api::core::v1::EnvVar],
        _ctx: &Arc<Context>,
    ) -> (Option<api::core::v1::Container>, String) {
        use api::core::v1::{Container, Volume, VolumeMount};

        match &self.spec.source {
            IndexerSource::GitHub(github) => {
                // This init container clones the indexer source code from GitHub.
                let mut args = vec!["clone".to_string(), "--depth=1".to_string()];

                if let Some(flags) = &github.git_flags {
                    args.extend_from_slice(flags);
                }

                args.extend_from_slice(&[
                    format!("--branch={}", github.branch),
                    format!("https://github.com/{}/{}.git", github.owner, github.repo),
                    "/code".to_string(),
                ]);

                // Create an emptyDir volume where the source code will be cloned.
                volumes.push(Volume {
                    name: "code".to_string(),
                    empty_dir: Some(Default::default()),
                    ..Volume::default()
                });

                volume_mounts.push(VolumeMount {
                    name: "code".to_string(),
                    mount_path: "/code".to_string(),
                    ..VolumeMount::default()
                });

                let workdir = match github.subpath {
                    None => "/code".to_string(),
                    Some(ref subpath) => format!("/code/{}", subpath),
                };

                let container = Container {
                    name: "clone-github-repo".to_string(),
                    image: Some(GIT_CLONE_IMAGE.to_string()),
                    args: Some(args),
                    volume_mounts: Some(volume_mounts.clone()),
                    env: Some(env.to_owned()),
                    ..Container::default()
                };

                (Some(container), workdir)
            }
            IndexerSource::Volume(fs) => (None, fs.path.clone()),
        }
    }

    fn sink_container(
        &self,
        workdir: &str,
        volume_mounts: &[api::core::v1::VolumeMount],
        env: &[api::core::v1::EnvVar],
        ctx: &Arc<Context>,
    ) -> Option<api::core::v1::Container> {
        use api::core::v1::Container;

        let image = match &self.spec.sink.sink {
            SinkType::Type { r#type } => {
                let Some(config) = ctx.configuration.sinks.get(r#type) else {
                    return None;
                };
                config.image.clone()
            }
            SinkType::Image { image } => image.clone(),
        };

        let script = self.spec.sink.script.clone();

        let port = ctx.configuration.status_port;
        let mut args = vec![
            "run".to_string(),
            script,
            "--status-server-address".to_string(),
            format!("0.0.0.0:{port}"),
        ];
        use api::core::v1::ContainerPort;

        if let Some(extra_args) = &self.spec.sink.args {
            args.extend_from_slice(extra_args);
        }

        let container = Container {
            name: "sink".to_string(),
            image: Some(image),
            args: Some(args),
            volume_mounts: Some(volume_mounts.to_owned()),
            env: Some(env.to_owned()),
            working_dir: Some(workdir.to_string()),
            ports: Some(vec![ContainerPort {
                container_port: port,
                name: Some("status".to_string()),
                ..ContainerPort::default()
            }]),
            ..Container::default()
        };

        Some(container)
    }
}

pub async fn reconcile_indexer(
    indexer: Arc<Indexer>,
    ctx: Arc<Context>,
) -> std::result::Result<Action, ReconcileError> {
    reconcile_indexer_impl(indexer, ctx)
        .await
        .map_err(ReconcileError)
}

async fn reconcile_indexer_impl(
    indexer: Arc<Indexer>,
    ctx: Arc<Context>,
) -> Result<Action, OperatorError> {
    let ns = indexer
        .namespace()
        .ok_or(OperatorError)
        .attach_printable("failed to get namespace")?;

    let indexers: Api<Indexer> = Api::namespaced(ctx.client.clone(), &ns);

    info!(
        indexer = %indexer.name_any(),
        namespace = %ns,
        "reconcile indexer"
    );

    finalizer::finalizer(&indexers, INDEXER_FINALIZER, indexer, |event| async {
        use finalizer::Event::*;
        match event {
            Apply(indexer) => indexer
                .reconcile(ctx.clone())
                .await
                .map_err(|err| err.into_error()),
            Cleanup(indexer) => indexer
                .cleanup(ctx.clone())
                .await
                .map_err(|err| err.into_error()),
        }
    })
    .await
    .change_context(OperatorError)
    .attach_printable("finalizer operation failed")
}

pub fn error_policy(_indexer: Arc<Indexer>, err: &ReconcileError, _ctx: Arc<Context>) -> Action {
    warn!(err = ?err, "Error reconciling indexer");

    Action::requeue(Duration::from_secs(30))
}

impl fmt::Debug for ReconcileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

impl fmt::Display for ReconcileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for ReconcileError {}
