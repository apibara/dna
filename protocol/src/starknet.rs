use error_stack::{report, Result, ResultExt};

use crate::helpers::{
    from_be_bytes_slice, impl_from_to_bytes, impl_scalar_helpers, impl_scalar_traits,
};

tonic::include_proto!("starknet.v2");

impl_scalar_traits!(FieldElement);
impl_from_to_bytes!(FieldElement, 32);
impl_scalar_helpers!(FieldElement, 32);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    pub fn test_field_element() {
        let hex = "0x9df92d765b5aa041fd4bbe8d5878eb89290efa78e444c1a603eecfae2ea05fa4";
        let field_element = FieldElement::from_hex(hex).unwrap();
        let back = field_element.to_hex();
        assert_eq!(hex, &back);
    }
}
