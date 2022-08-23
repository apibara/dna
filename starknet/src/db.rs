pub mod tables {
    use apibara_node::db::Table;
    use prost::Message;

    #[derive(Debug, Clone, Copy, Default)]
    pub struct NodeStateTable;

    #[derive(Clone, PartialEq, Message)]
    pub struct NodeState {
        /// Latest indexed block number, as felt.
        #[prost(bytes, tag = "1")]
        pub block_number: Vec<u8>,
        /// Latest indexed block hash, as felt.
        #[prost(bytes, tag = "2")]
        pub block_hash: Vec<u8>,
    }

    impl Table for NodeStateTable {
        type Key = ();
        type Value = NodeState;

        fn db_name() -> &'static str {
            "NodeState"
        }
    }
}
