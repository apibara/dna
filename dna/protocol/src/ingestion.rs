tonic::include_proto!("dna.v2.ingestion");

pub const INGESTION_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("ingestion_v2_descriptor");

pub fn ingestion_file_descriptor_set() -> &'static [u8] {
    INGESTION_DESCRIPTOR_SET
}

impl SubscribeResponse {
    pub fn as_snapshot(&self) -> Option<&Snapshot> {
        self.message.as_ref().and_then(|msg| match msg {
            subscribe_response::Message::Snapshot(snapshot) => Some(snapshot),
            _ => None,
        })
    }
}
