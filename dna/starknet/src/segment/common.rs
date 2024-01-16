use apibara_dna_common::error::Result;
use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::provider::models;

use super::store::FieldElement;

pub trait SegmentTable {
    type S<'a>;
    fn create<'a>(&self, builder: &mut FlatBufferBuilder<'a>) -> Result<WIPOffset<Self::S<'a>>>;
}

impl From<models::FieldElement> for FieldElement {
    fn from(field_element: models::FieldElement) -> Self {
        FieldElement(field_element.to_bytes_be())
    }
}

impl From<&models::FieldElement> for FieldElement {
    fn from(field_element: &models::FieldElement) -> Self {
        FieldElement(field_element.to_bytes_be())
    }
}
