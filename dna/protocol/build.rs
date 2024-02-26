use std::{env, io::Result, path::PathBuf, println};

static DNA_STREAM_DESCRIPTOR_FILE: &str = "dna_stream_v2_descriptor.bin";
static INGESTION_DESCRIPTOR_FILE: &str = "ingestion_v2_descriptor.bin";

fn main() -> Result<()> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    println!("cargo:rerun-if-changed=proto");

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join(DNA_STREAM_DESCRIPTOR_FILE))
        .compile(&["proto/dna/v2/stream.proto"], &["proto/dna/"])?;

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join(INGESTION_DESCRIPTOR_FILE))
        .compile(&["proto/dna/v2/ingestion.proto"], &["proto/dna/"])?;

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_well_known_types(true)
        .extern_path(".google.protobuf", "::pbjson_types")
        .compile(
            &["proto/evm/v2/data.proto", "proto/evm/v2/filter.proto"],
            &["proto/evm/"],
        )?;

    Ok(())
}
