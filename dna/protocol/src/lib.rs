pub mod dna;
pub mod ingestion;

pub mod evm {
    tonic::include_proto!("apibara.evm.v2");
}
