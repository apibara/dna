//! gRPC server implementation for the offset registry.

use std::sync::Arc;

use async_trait::async_trait;
use error_stack::Report;
use tonic::{Request, Response, Status};

use crate::admin::{NamespaceName, TopicName};
use crate::offset_registry::{BatchToCommit, OffsetRegistry};
use crate::protocol::wings::v1::{
    self as pb,
    offset_registry_service_server::{
        OffsetRegistryService as OffsetRegistryServiceTrait, OffsetRegistryServiceServer,
    },
};
use crate::resource::ResourceError;

use super::OffsetRegistryError;

/// gRPC server implementation for the OffsetRegistryService.
pub struct OffsetRegistryService {
    offset_registry: Arc<dyn OffsetRegistry>,
}

impl OffsetRegistryService {
    /// Create a new OffsetRegistryService server with the given offset registry implementation.
    pub fn new(offset_registry: Arc<dyn OffsetRegistry>) -> Self {
        Self { offset_registry }
    }

    pub fn into_service(self) -> OffsetRegistryServiceServer<Self> {
        OffsetRegistryServiceServer::new(self)
    }
}

#[async_trait]
impl OffsetRegistryServiceTrait for OffsetRegistryService {
    async fn commit_folio(
        &self,
        request: Request<pb::CommitFolioRequest>,
    ) -> Result<Response<pb::CommitFolioResponse>, Status> {
        let request = request.into_inner();

        let namespace_name = NamespaceName::parse(&request.namespace)
            .map_err(OffsetRegistryError::InvalidResourceName)
            .map_err(Into::into)
            .map_err(offset_registry_error_to_status)?;

        let batches: Vec<BatchToCommit> = request
            .batches
            .into_iter()
            .map(|batch| batch.try_into())
            .collect::<Result<Vec<_>, Report<OffsetRegistryError>>>()
            .map_err(offset_registry_error_to_status)?;

        let committed_batches = self
            .offset_registry
            .commit_folio(namespace_name, request.file_ref, &batches)
            .await
            .map_err(offset_registry_error_to_status)?;

        let response = pb::CommitFolioResponse {
            batches: committed_batches.into_iter().map(Into::into).collect(),
        };

        Ok(Response::new(response))
    }

    async fn offset_location(
        &self,
        request: Request<pb::OffsetLocationRequest>,
    ) -> Result<Response<pb::OffsetLocationResponse>, Status> {
        let request = request.into_inner();

        let topic_name = TopicName::parse(&request.topic)
            .map_err(OffsetRegistryError::InvalidResourceName)
            .map_err(Into::into)
            .map_err(offset_registry_error_to_status)?;

        let partition_value = request
            .partition
            .map(TryFrom::try_from)
            .transpose()
            .map_err(offset_registry_error_to_status)?;

        let offset_location = self
            .offset_registry
            .offset_location(topic_name, partition_value, request.offset)
            .await
            .map_err(offset_registry_error_to_status)?;

        Ok(Response::new(offset_location.into()))
    }
}

fn offset_registry_error_to_status(
    error: error_stack::Report<crate::offset_registry::error::OffsetRegistryError>,
) -> Status {
    use crate::offset_registry::error::OffsetRegistryError;

    match error.current_context() {
        OffsetRegistryError::DuplicatePartitionValue(topic, partition) => {
            Status::already_exists(format!(
                "duplicate partition value: topic={}, partition={:?}",
                topic, partition
            ))
        }
        OffsetRegistryError::NamespaceNotFound(namespace) => {
            Status::not_found(format!("namespace not found: {}", namespace))
        }
        OffsetRegistryError::OffsetNotFound(topic, partition, offset) => {
            Status::not_found(format!(
                "offset not found: topic={}, partition={:?}, offset={}",
                topic, partition, offset
            ))
        }
        OffsetRegistryError::InvalidOffsetRange => Status::invalid_argument("invalid offset range"),
        OffsetRegistryError::InvalidArgument(message) => Status::invalid_argument(message.clone()),
        OffsetRegistryError::InvalidResourceName(resource_error) => match resource_error {
            ResourceError::InvalidFormat { expected, actual } => Status::invalid_argument(format!(
                "invalid resource name format: expected '{}' but got '{}'",
                expected, actual
            )),
            ResourceError::InvalidName { name } => {
                Status::invalid_argument(format!("invalid resource name: {}", name))
            }
            ResourceError::MissingParent { name } => {
                Status::invalid_argument(format!("missing parent resource in name: {}", name))
            }
            ResourceError::InvalidResourceId { id } => {
                Status::invalid_argument(format!("invalid resource id: {}", id))
            }
        },
        OffsetRegistryError::Internal(message) => {
            Status::internal(format!("internal error: {}", message))
        }
    }
}
