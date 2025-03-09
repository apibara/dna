use apibara_observability::{Gauge, Histogram, RequestMetrics};

#[derive(Debug, Clone)]
pub struct IngestionMetrics {
    pub up: Gauge<u64>,
    pub state: Gauge<u64>,
    pub head: Gauge<u64>,
    pub ingested: Gauge<u64>,
    pub finalized: Gauge<u64>,
    pub block_size: Histogram<u64>,
    pub rpc: RequestMetrics,
    pub block_upload: RequestMetrics,
    pub ingestion_latency: Histogram<f64>,
}

impl Default for IngestionMetrics {
    fn default() -> Self {
        let meter = apibara_observability::meter("dna_ingestion");

        Self {
            up: meter
                .u64_gauge("dna.ingestion.up")
                .with_description("ingestion service is up")
                .build(),
            state: meter
                .u64_gauge("dna.ingestion.state")
                .with_description("ingestion service state. 1 = ingesting, 2 = recovering")
                .build(),
            head: meter
                .u64_gauge("dna.ingestion.head")
                .with_description("chain's head block")
                .with_unit("{block}")
                .build(),
            ingested: meter
                .u64_gauge("dna.ingestion.ingested")
                .with_description("latest block ingested")
                .with_unit("{block}")
                .build(),
            finalized: meter
                .u64_gauge("dna.ingestion.finalized")
                .with_description("chain's finalized block")
                .with_unit("{block}")
                .build(),
            block_size: meter
                .u64_histogram("dna.ingestion.block_size")
                .with_description("block size in bytes")
                .with_unit("By")
                .with_boundaries(vec![
                    1_000.0,
                    5_000.0,
                    10_000.0,
                    25_000.0,
                    50_000.0,
                    100_000.0,
                    250_000.0,
                    500_000.0,
                    1_000_000.0,
                    10_000_000.0,
                    25_000_000.0,
                    100_000_000.0,
                    500_000_000.0,
                    1_000_000_000.0,
                ])
                .build(),
            rpc: RequestMetrics::new("dna_ingestion", "dna.ingestion.rpc"),
            block_upload: RequestMetrics::new("dna_ingestion", "dna.ingestion.block_upload"),
            ingestion_latency: meter
                .f64_histogram("dna.ingestion.latency")
                .with_description(
                    "end-to-end time it takes from queueing ingestion to finishing it",
                )
                .with_unit("s")
                .with_boundaries(vec![
                    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5,
                    10.0, 20.0, 30.0, 60.0, 120.0,
                ])
                .build(),
        }
    }
}
