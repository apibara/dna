use apibara_observability::Counter;

#[derive(Debug, Clone)]
pub struct ObjectStoreMetrics {
    pub get: Counter<u64>,
    pub put: Counter<u64>,
    pub delete: Counter<u64>,
    pub list: Counter<u64>,
}

impl Default for ObjectStoreMetrics {
    fn default() -> Self {
        let meter = apibara_observability::meter("object_store");

        Self {
            get: meter
                .u64_counter("dna.object_store.get")
                .with_description("number of get operations")
                .with_unit("{op}")
                .build(),
            put: meter
                .u64_counter("dna.object_store.put")
                .with_description("number of put operations")
                .with_unit("{op}")
                .build(),
            delete: meter
                .u64_counter("dna.object_store.delete")
                .with_description("number of delete operations")
                .with_unit("{op}")
                .build(),
            list: meter
                .u64_counter("dna.object_store.list")
                .with_description("number of list operations")
                .with_unit("{op}")
                .build(),
        }
    }
}
