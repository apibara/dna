use apibara_observability::Gauge;

#[derive(Debug, Clone)]
pub struct ChainViewMetrics {
    pub up: Gauge<u64>,
    pub head: Gauge<u64>,
    pub finalized: Gauge<u64>,
    pub segmented: Gauge<u64>,
    pub grouped: Gauge<u64>,
}

impl Default for ChainViewMetrics {
    fn default() -> Self {
        let meter = apibara_observability::meter("dna_chain_view");

        Self {
            up: meter
                .u64_gauge("dna.chain_view.up")
                .with_description("chain view is up")
                .build(),
            head: meter
                .u64_gauge("dna.chain_view.head")
                .with_description("chain view's head block")
                .build(),
            finalized: meter
                .u64_gauge("dna.chain_view.finalized")
                .with_description("chain view's finalized block")
                .build(),
            segmented: meter
                .u64_gauge("dna.chain_view.segmented")
                .with_description("chain view's segmented block")
                .build(),
            grouped: meter
                .u64_gauge("dna.chain_view.grouped")
                .with_description("chain view's grouped block")
                .build(),
        }
    }
}
