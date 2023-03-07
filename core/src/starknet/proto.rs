pub mod v1alpha2 {
    tonic::include_proto!("apibara.starknet.v1alpha2");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("starknet_v1alpha2_descriptor");

    pub fn starknet_file_descriptor_set() -> &'static [u8] {
        FILE_DESCRIPTOR_SET
    }
}
