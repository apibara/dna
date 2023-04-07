use std::{env, path::PathBuf};

static NODE_DESCRIPTOR_FILE: &str = "node_v1alpha2_descriptor.bin";
static STARKNET_DESCRIPTOR_FILE: &str = "starknet_v1alpha2_descriptor.bin";
static ETHEREUM_DESCRIPTOR_FILE: &str = "ethereum_v1alpha2_descriptor.bin";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(out_dir.join(NODE_DESCRIPTOR_FILE))
        .compile(&["proto/node/v1alpha2/stream.proto"], &["proto/node"])?;

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(out_dir.join(STARKNET_DESCRIPTOR_FILE))
        .compile_well_known_types(true)
        .extern_path(".google.protobuf", "::pbjson_types")
        .compile(
            &[
                "proto/starknet/v1alpha2/starknet.proto",
                "proto/starknet/v1alpha2/filter.proto",
            ],
            &["proto/starknet"],
        )?;

    // add jsonpb definitions, but only for the data types
    let starknet_description_set = std::fs::read(out_dir.join(STARKNET_DESCRIPTOR_FILE))?;
    pbjson_build::Builder::new()
        .register_descriptors(&starknet_description_set)?
        .exclude([".apibara.starknet.v1alpha2.FieldElement"])
        .build(&[".apibara"])?;

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join(ETHEREUM_DESCRIPTOR_FILE))
        .compile_well_known_types(true)
        .extern_path(".google.protobuf", "::pbjson_types")
        .compile(
            &[
                "proto/ethereum/v1alpha2/ethereum.proto",
                "proto/ethereum/v1alpha2/filter.proto",
            ],
            &["proto/ethereum"],
        )?;

    // add jsonpb definitions, but only for the data types
    let ethereum_description_set = std::fs::read(out_dir.join(ETHEREUM_DESCRIPTOR_FILE))?;
    pbjson_build::Builder::new()
        .register_descriptors(&ethereum_description_set)?
        // TODO: implement hex encoder/decoder
        /*
        .exclude([
            ".apibara.ethereum.v1alpha2.H256",
            ".apibara.ethereum.v1alpha2.H160",
        ])
        */
        .build(&[".apibara"])?;

    Ok(())
}
