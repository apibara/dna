use std::{env, io::Result, path::PathBuf, println};

static DNA_STREAM_DESCRIPTOR_FILE: &str = "dna_stream_v2_descriptor.bin";
static EVM_DESCRIPTOR_FILE: &str = "evm_descriptor.bin";
static STARKNET_DESCRIPTOR_FILE: &str = "starknet_descriptor.bin";

fn main() -> Result<()> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    println!("cargo:rerun-if-changed=proto");

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .bytes([".dna.v2.stream.Data.data"])
        .file_descriptor_set_path(out_dir.join(DNA_STREAM_DESCRIPTOR_FILE))
        .compile(&["proto/dna/v2/stream.proto"], &["proto/dna/"])?;

    /*
     * EVM
     */
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join(EVM_DESCRIPTOR_FILE))
        .compile(
            &["proto/evm/v2/data.proto", "proto/evm/v2/filter.proto"],
            &["proto/evm/"],
        )?;

    /*
     * Starknet
     */
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join(STARKNET_DESCRIPTOR_FILE))
        .compile(
            &[
                "proto/starknet/v2/data.proto",
                "proto/starknet/v2/filter.proto",
            ],
            &["proto/starknet/"],
        )?;

    /*
     * Beacon Chain
     */
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join(STARKNET_DESCRIPTOR_FILE))
        .compile(
            &[
                "proto/beaconchain/v2/data.proto",
                "proto/beaconchain/v2/filter.proto",
            ],
            &["proto/beaconchain/"],
        )?;

    Ok(())
}
