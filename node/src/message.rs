//! Messages sent through the streams.
use apibara_core::stream::Sequence;

/// Message trait.
///
/// Messages must be protobuf messages with a default value and a
/// sequence number.
pub trait Message: prost::Message + Default {
    /// Returns the message sequence number.
    fn sequence(&self) -> Sequence;
}
