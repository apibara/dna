tonic::include_proto!("mod");

pub const ADMIN_DESCRIPTOR_SET: &[u8] = tonic::include_file_descriptor_set!("wings_v1_admin");

pub fn admin_file_descriptor_set() -> &'static [u8] {
    ADMIN_DESCRIPTOR_SET
}
