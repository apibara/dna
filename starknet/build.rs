use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("starknet_descriptor_v1alpha2.bin"))
        .compile(&["proto/v1alpha2/starknet.proto"], &["proto"])?;
    Ok(())
}
