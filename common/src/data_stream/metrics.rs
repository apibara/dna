use apibara_observability::{Histogram, RequestMetrics, UpDownCounter};

#[derive(Debug, Clone)]
pub struct DataStreamMetrics {
    pub active: UpDownCounter<i64>,
    pub block_size: Histogram<u64>,
    pub fragment_size: Histogram<u64>,
    pub time_in_queue: Histogram<f64>,
    pub block: RequestMetrics,
    pub segment: RequestMetrics,
    pub group: RequestMetrics,
}

impl Default for DataStreamMetrics {
    fn default() -> Self {
        let meter = apibara_observability::meter("dna_data_stream");

        Self {
            active: meter
                .i64_up_down_counter("dna.data_stream.active")
                .with_description("number of active data streams")
                .with_unit("{connection}")
                .build(),
            block_size: meter
                .u64_histogram("dna.data_stream.block_size")
                .with_description("size (in bytes) of blocks sent to the client")
                .with_unit("By")
                .with_boundaries(vec![
                    1_000.0,
                    10_000.0,
                    100_000.0,
                    1_000_000.0,
                    5_000_000.0,
                    10_000_000.0,
                    25_000_000.0,
                    50_000_000.0,
                    100_000_000.0,
                    1_000_000_000.0,
                ])
                .build(),
            fragment_size: meter
                .u64_histogram("dna.data_stream.fragment_size")
                .with_description("size (in bytes) of fragments sent to the client")
                .with_unit("By")
                .with_boundaries(vec![
                    1_000.0,
                    10_000.0,
                    100_000.0,
                    1_000_000.0,
                    5_000_000.0,
                    10_000_000.0,
                    25_000_000.0,
                    50_000_000.0,
                    100_000_000.0,
                    1_000_000_000.0,
                ])
                .build(),
            time_in_queue: meter
                .f64_histogram("dna.data_stream.time_in_queue")
                .with_description("time (in seconds) spent in the prefetch queue")
                .with_unit("s")
                .with_boundaries(vec![
                    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05,
                    0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0,
                ])
                .build(),
            block: RequestMetrics::new_with_boundaries(
                "dna_data_stream",
                "dna.data_stream.block",
                vec![
                    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05,
                    0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0,
                ],
            ),
            segment: RequestMetrics::new_with_boundaries(
                "dna_data_stream",
                "dna.data_stream.segment",
                vec![
                    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05,
                    0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0,
                ],
            ),
            group: RequestMetrics::new_with_boundaries(
                "dna_data_stream",
                "dna.data_stream.group",
                vec![
                    0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.0075, 0.01, 0.025, 0.05,
                    0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0,
                ],
            ),
        }
    }
}
