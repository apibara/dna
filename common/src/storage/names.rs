use crate::core::Cursor;

pub static BLOCK_NAME: &str = "block";

pub fn segment_prefix(segment_name: impl AsRef<str>) -> String {
    format!("segment/{}", segment_name.as_ref())
}

pub fn block_prefix(cursor: &Cursor) -> String {
    format!("block/{}-{}", cursor.number, cursor.hash_as_hex())
}
