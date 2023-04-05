pub mod v1alpha2 {
    tonic::include_proto!("apibara.ethereum.v1alpha2");
    tonic::include_proto!("apibara.ethereum.v1alpha2.serde");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("ethereum_v1alpha2_descriptor");

    pub fn ethereum_file_descriptor_set() -> &'static [u8] {
        FILE_DESCRIPTOR_SET
    }
}
