//! Stream data from StarkNet.
mod accepted;
mod aggregate;
mod finalized;

pub use self::{
    aggregate::{BlockDataAggregator, DatabaseBlockDataAggregator},
    finalized::FinalizedBlockStream,
};
