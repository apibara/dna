use crate::core::Cursor;

/// Events about blocks that are being ingested.
#[derive(Debug, Clone)]
pub enum IngestionMessage<C: Cursor> {
    /// Finalized block ingested.
    Finalized(C),
    /// Accepted block ingested.
    Accepted(C),
    /// Pending block ingested.
    Pending(C),
    /// Chain reorganization with root at the given block.
    /// Notice that the given root belongs to the new chain
    /// and is now the tip of it.
    Invalidate(C),
}
