pub mod v1alpha2 {
    tonic::include_proto!("apibara.starknet.v1alpha2");
    tonic::include_proto!("apibara.starknet.v1alpha2.serde");

    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("starknet_v1alpha2_descriptor");

    pub fn starknet_file_descriptor_set() -> &'static [u8] {
        FILE_DESCRIPTOR_SET
    }
}

#[cfg(test)]
mod tests {
    use super::v1alpha2;

    #[test]
    pub fn test_field_element_as_hex_string() {
        let fe = v1alpha2::FieldElement::from_u64(0x1234567890abcdef);
        let as_hex = serde_json::to_string(&fe).unwrap();
        assert_eq!(
            as_hex,
            r#""0x0000000000000000000000000000000000000000000000001234567890abcdef""#
        );
        let back = serde_json::from_str::<v1alpha2::FieldElement>(&as_hex).unwrap();
        assert_eq!(fe, back);
    }
}
