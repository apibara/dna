use std::{env, io::Result, path::PathBuf, println};

static DNA_STREAM_DESCRIPTOR_FILE: &str = "dna_stream_v2_descriptor.bin";
static INGESTION_DESCRIPTOR_FILE: &str = "ingestion_v2_descriptor.bin";
static EVM_DESCRIPTOR_FILE: &str = "evm_descriptor.bin";

fn main() -> Result<()> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    println!("cargo:rerun-if-changed=proto");

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join(DNA_STREAM_DESCRIPTOR_FILE))
        .compile(
            &["proto/dna/v2/common.proto", "proto/dna/v2/stream.proto"],
            &["proto/dna/"],
        )?;

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join(INGESTION_DESCRIPTOR_FILE))
        .compile(
            &["proto/dna/v2/ingestion.proto", "proto/dna/v2/common.proto"],
            &["proto/dna/"],
        )?;

    /*
     * EVM
     */
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_well_known_types(true)
        .file_descriptor_set_path(out_dir.join(EVM_DESCRIPTOR_FILE))
        .extern_path(".google.protobuf", "::pbjson_types")
        .compile(
            &["proto/evm/v2/data.proto", "proto/evm/v2/filter.proto"],
            &["proto/evm/"],
        )?;

    let evm_descriptor_set = std::fs::read(out_dir.join(EVM_DESCRIPTOR_FILE))?;
    pbjson_build::Builder::new()
        .register_descriptors(&evm_descriptor_set)?
        .exclude([
            ".evm.v2.Address",
            ".evm.v2.U128",
            ".evm.v2.U256",
            ".evm.v2.B256",
            ".evm.v2.HexData",
            ".evm.v2.Topic",
        ])
        .build(&[".evm"])?;

    Ok(())
}
