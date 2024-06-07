use crate::core::Cursor;

#[derive(Debug, Clone)]
pub enum ChainChange {
    /// First message in the stream, with the starting state.
    Initialize { head: Cursor, finalized: Cursor },
    /// A new head has been detected.
    NewHead(Cursor),
    /// A new finalized block has been detected.
    NewFinalized(Cursor),
    /// The chain reorganized.
    Invalidate,
}
