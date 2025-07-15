use std::sync::Arc;

use bytes::BytesMut;
use error_stack::ResultExt;
use object_store::{PutMode, PutOptions, PutPayload};
use wings_metadata_core::{
    admin::{NamespaceName, NamespaceRef},
    offset_registry::BatchToCommit,
};
use wings_object_store::ObjectStoreFactory;

use crate::{
    batcher::namespace::PartitionFolioWithMetadata,
    error::{IngestorError, IngestorResult},
};

/// Trait for generating unique IDs for folios.
pub trait FolioIdGenerator {
    fn generate_id(&self) -> String;
}

/// A folio ready for upload.
#[derive(Debug)]
pub struct FolioToUpload {
    pub namespace: NamespaceRef,
    pub partitions: Vec<PartitionFolioWithMetadata>,
}

#[derive(Clone)]
pub struct FolioUploader {
    pub id_generator: Arc<dyn FolioIdGenerator>,
    pub object_store_factory: Arc<dyn ObjectStoreFactory>,
}

pub struct UploadedFolio {
    pub namespace: NamespaceName,
    pub file_ref: String,
    pub batches: Vec<BatchToCommit>,
}

/// Generates unique IDs using the ULID algorithm.
#[derive(Debug, Clone)]
pub struct UlidFolioIdGenerator;

impl FolioUploader {
    pub fn new(
        id_generator: Arc<dyn FolioIdGenerator>,
        object_store_factory: Arc<dyn ObjectStoreFactory>,
    ) -> Self {
        FolioUploader {
            id_generator,
            object_store_factory,
        }
    }

    pub fn new_ulid(object_store_factory: Arc<dyn ObjectStoreFactory>) -> Self {
        FolioUploader::new(Arc::new(UlidFolioIdGenerator), object_store_factory)
    }

    pub async fn upload_folio(&self, folio: FolioToUpload) -> IngestorResult<UploadedFolio> {
        let secret_name = &folio.namespace.default_object_store_config;

        let object_store = self
            .object_store_factory
            .create_object_store(secret_name.clone())
            .await
            .change_context(IngestorError::ObjectStoreError {
                message: format!("could not create object store from secret {}", secret_name),
            })?;

        let folio_id = self.id_generator.generate_id();
        let path = format!("{}/folio/{}", folio.namespace.name, folio_id);

        // TODO: estimate size before initializing content
        let mut content = BytesMut::new();
        let mut batches = Vec::with_capacity(folio.partitions.len());

        // TODO: sort by (topic, partition)
        for partition in folio.partitions {
            let offset_bytes = content.len() as _;
            let batch_size_bytes = partition.folio.data.len() as _;
            content.extend_from_slice(&partition.folio.data);
            let num_rows = partition
                .folio
                .batches
                .iter()
                .fold(0, |acc, m| acc + m.batch_size as u32);

            batches.push(BatchToCommit {
                topic_id: partition.topic.clone(),
                partition_value: partition.partition.clone(),
                num_messages: num_rows,
                offset_bytes,
                batch_size_bytes,
            })
        }

        object_store
            .put_opts(
                &path.clone().into(),
                PutPayload::from_static(&[0; 512]),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
            .change_context(IngestorError::ObjectStoreError {
                message: format!("could not create object store from secret {}", secret_name),
            })?;

        // TODO: must return the initial batch metadata (per partition) to then return the correct offsets
        Ok(UploadedFolio {
            namespace: folio.namespace.name.clone(),
            file_ref: path,
            batches,
        })
    }
}

impl FolioIdGenerator for UlidFolioIdGenerator {
    fn generate_id(&self) -> String {
        ulid::Ulid::new().to_string()
    }
}
