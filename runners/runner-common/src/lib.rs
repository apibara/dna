pub mod error;
pub mod runner {
    pub mod v1 {
        tonic::include_proto!("apibara.runner.v1");

        const FILE_DESCRIPTOR_SET: &[u8] =
            tonic::include_file_descriptor_set!("runner_v1_descriptor");

        pub fn runner_file_descriptor_set() -> &'static [u8] {
            FILE_DESCRIPTOR_SET
        }
    }
}
