//! Stream data from StarkNet.
mod accepted;
mod aggregate;
mod finalized;

pub use self::finalized::FinalizedBlockStream;
