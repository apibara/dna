use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("node_v1alpha2_descriptor.bin"))
        .compile(&["proto/node/v1alpha2/stream.proto"], &["proto/node"])?;

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("starknet_v1alpha2_descriptor.bin"))
        .compile(
            &[
                "proto/starknet/v1alpha2/starknet.proto",
                "proto/starknet/v1alpha2/filter.proto",
            ],
            &["proto/starknet"],
        )?;
    Ok(())
}
