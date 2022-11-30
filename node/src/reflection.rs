//! Utilities for gRPC reflection.

use std::collections::HashMap;

use apibara_core::node::v1alpha1::pb::node_file_descriptor_set;
use prost::{DecodeError, Message};
use prost_types::{FileDescriptorProto, FileDescriptorSet};

/// Creates a new `FileDescriptorSet` that contains the streaming data definitions.
pub fn merge_node_service_descriptor_set(
    service_name: &str,
    node_data: FileDescriptorSet,
) -> Result<FileDescriptorSet, DecodeError> {
    let node_set = FileDescriptorSet::decode(node_file_descriptor_set())?;
    let mut filename_to_descriptor = HashMap::<String, FileDescriptorProto>::default();
    for mut file in node_set.file {
        let name = file.name().to_string();
        if name == "node.proto" {
            file.dependency.push(service_name.to_string());
        }
        filename_to_descriptor.insert(name, file);
    }
    for file in node_data.file {
        let name = file.name().to_string();
        filename_to_descriptor.insert(name, file);
    }
    let file = filename_to_descriptor.into_values().collect();
    Ok(FileDescriptorSet { file })
}

/// Creates a new `FileDescriptorSet` that contains the streaming data definitions.
pub fn merge_encoded_node_service_descriptor_set(
    service_name: &str,
    node_data: &[u8],
) -> Result<FileDescriptorSet, DecodeError> {
    let data_set = FileDescriptorSet::decode(node_data)?;
    merge_node_service_descriptor_set(service_name, data_set)
}
