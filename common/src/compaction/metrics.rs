use apibara_observability::{Gauge, Histogram, RequestMetrics};

#[derive(Debug, Clone)]
pub struct CompactionMetrics {
    pub up: Gauge<u64>,
    pub segmented: Gauge<u64>,
    pub grouped: Gauge<u64>,
    pub pruned: Gauge<u64>,
    pub block_download: RequestMetrics,
    pub segment_creation: RequestMetrics,
    pub segment_upload: RequestMetrics,
    pub segment_size: Histogram<u64>,
    pub segment_download: RequestMetrics,
    pub group_creation: RequestMetrics,
    pub group_upload: RequestMetrics,
    pub group_size: Histogram<u64>,
}

impl Default for CompactionMetrics {
    fn default() -> Self {
        let meter = apibara_observability::meter("dna_compaction");

        Self {
            up: meter
                .u64_gauge("dna.compaction.up")
                .with_description("dna compaction service is up")
                .build(),
            segmented: meter
                .u64_gauge("dna.compaction.segmented")
                .with_description("dna compaction most recent segmented block")
                .build(),
            grouped: meter
                .u64_gauge("dna.compaction.grouped")
                .with_description("dna compaction most recent grouped block")
                .build(),
            pruned: meter
                .u64_gauge("dna.compaction.pruned")
                .with_description("dna compaction most recent pruned block")
                .build(),
            block_download: RequestMetrics::new("dna_compaction", "dna.compaction.block_download"),
            segment_creation: RequestMetrics::new(
                "dna_compaction",
                "dna.compaction.segment_creation",
            ),
            segment_upload: RequestMetrics::new("dna_compaction", "dna.compaction.segment_upload"),
            segment_size: meter
                .u64_histogram("dna.compaction.segment_size")
                .with_description("segment size in bytes")
                .with_unit("By")
                .with_boundaries(vec![
                    500_000.0,
                    1_000_000.0,
                    10_000_000.0,
                    25_000_000.0,
                    50_000_000.0,
                    75_000_000.0,
                    100_000_000.0,
                    250_000_000.0,
                    500_000_000.0,
                    1_000_000_000.0,
                    10_000_000_000.0,
                ])
                .build(),
            segment_download: RequestMetrics::new(
                "dna_compaction",
                "dna.compaction.segment_download",
            ),
            group_creation: RequestMetrics::new("dna_compaction", "dna.compaction.group_creation"),
            group_upload: RequestMetrics::new("dna_compaction", "dna.compaction.group_upload"),
            group_size: meter
                .u64_histogram("dna.compaction.group_size")
                .with_description("group size in bytes")
                .with_unit("By")
                .with_boundaries(vec![
                    500_000.0,
                    1_000_000.0,
                    10_000_000.0,
                    25_000_000.0,
                    50_000_000.0,
                    75_000_000.0,
                    100_000_000.0,
                    250_000_000.0,
                    500_000_000.0,
                    1_000_000_000.0,
                    10_000_000_000.0,
                ])
                .build(),
        }
    }
}
