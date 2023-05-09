use apibara_core::node::v1alpha2::Cursor as ProtoCursor;

/// A cursor is a position in a stream.
pub trait Cursor: Sized + Default + Clone + std::fmt::Debug {
    /// Create a new cursor from a proto cursor.
    fn from_proto(cursor: &ProtoCursor) -> Option<Self>;

    /// Returns the proto cursor.
    fn to_proto(&self) -> ProtoCursor;
}
