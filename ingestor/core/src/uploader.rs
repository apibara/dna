use wings_metadata_core::admin::NamespaceName;

use crate::batcher::namespace::PartitionFolioWithMetadata;

#[derive(Debug)]
pub struct FolioToUpload {
    pub namespace: NamespaceName,
    pub partitions: Vec<PartitionFolioWithMetadata>,
}
