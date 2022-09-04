pub mod stream;

pub mod pb {
    tonic::include_proto!("apibara.node.v1alpha1");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("node_descriptor");

    pub fn node_file_descriptor_set() -> &'static [u8] {
        FILE_DESCRIPTOR_SET
    }
}
