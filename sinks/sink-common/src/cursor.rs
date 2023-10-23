use std::fmt::{self, Display};

use apibara_core::node::v1alpha2::Cursor;

/// A newtype to display a cursor that may be `None` as "genesis".
pub struct DisplayCursor<'a>(pub &'a Option<Cursor>);

impl<'a> Display for DisplayCursor<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(cursor) => write!(f, "{}", cursor),
            None => write!(f, "Cursor(genesis)"),
        }
    }
}
