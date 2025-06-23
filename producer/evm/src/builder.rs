use std::sync::Arc;

use arrow::{
    array::{ArrayRef, ArrowPrimitiveType},
    datatypes::{UInt8Type, UInt32Type, UInt64Type},
};
use arrow_schema::DataType;

pub struct PrimitiveBuilder<T: ArrowPrimitiveType>(arrow::array::PrimitiveBuilder<T>);

pub type U8Builder = PrimitiveBuilder<UInt8Type>;
pub type U32Builder = PrimitiveBuilder<UInt32Type>;
pub type U64Builder = PrimitiveBuilder<UInt64Type>;

pub struct FixedSizeBinaryBuilder<const SIZE: usize>(arrow::array::FixedSizeBinaryBuilder);

pub type AddressBuilder = FixedSizeBinaryBuilder<20>;
pub type B256Builder = FixedSizeBinaryBuilder<32>;
pub type U256Builder = FixedSizeBinaryBuilder<32>;

pub struct BinaryBuilder(arrow::array::BinaryBuilder);

impl<T: ArrowPrimitiveType> PrimitiveBuilder<T> {
    pub const fn data_type() -> DataType {
        T::DATA_TYPE
    }

    #[inline]
    pub fn append_null(&mut self) {
        self.0.append_null();
    }

    #[inline]
    pub fn append_value(&mut self, value: T::Native) {
        self.0.append_value(value);
    }

    #[inline]
    pub fn finish(&mut self) -> ArrayRef {
        Arc::new(self.0.finish())
    }
}

impl<T: ArrowPrimitiveType> Default for PrimitiveBuilder<T> {
    fn default() -> Self {
        Self(arrow::array::PrimitiveBuilder::default())
    }
}

impl<const SIZE: usize> FixedSizeBinaryBuilder<SIZE> {
    pub const fn data_type() -> DataType {
        DataType::FixedSizeBinary(SIZE as i32)
    }

    #[inline]
    pub fn append_null(&mut self) {
        self.0.append_null();
    }

    #[inline]
    pub fn append_value(&mut self, value: &[u8; SIZE]) {
        self.0.append_value(value).expect("invalid message size");
    }

    #[inline]
    pub fn finish(&mut self) -> ArrayRef {
        Arc::new(self.0.finish())
    }
}

impl<const SIZE: usize> Default for FixedSizeBinaryBuilder<SIZE> {
    fn default() -> Self {
        Self(arrow::array::FixedSizeBinaryBuilder::new(SIZE as i32))
    }
}

impl BinaryBuilder {
    pub const fn data_type() -> DataType {
        DataType::Binary
    }

    #[inline]
    pub fn append_null(&mut self) {
        self.0.append_null();
    }

    #[inline]
    pub fn append_value(&mut self, value: &[u8]) {
        self.0.append_value(value);
    }

    #[inline]
    pub fn finish(&mut self) -> ArrayRef {
        Arc::new(self.0.finish())
    }
}

impl Default for BinaryBuilder {
    fn default() -> Self {
        Self(arrow::array::BinaryBuilder::default())
    }
}
