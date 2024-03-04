tonic::include_proto!("apibara.dna.v2");

pub const DNA_STREAM_DESCRIPTOR_SET: &[u8] =
    tonic::include_file_descriptor_set!("dna_stream_v2_descriptor");

pub fn dna_stream_file_descriptor_set() -> &'static [u8] {
    DNA_STREAM_DESCRIPTOR_SET
}
