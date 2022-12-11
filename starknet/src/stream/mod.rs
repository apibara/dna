//! Stream data from StarkNet.
mod finalized;
mod accepted;
mod aggregate;

pub use self::finalized::FinalizedBlockStream;