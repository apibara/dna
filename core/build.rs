use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("node_descriptor.bin"))
        .compile(&["proto/v1alpha1/node.proto"], &["proto"])?;
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("application_descriptor.bin"))
        .compile(&["proto/application.proto"], &["proto"])?;
    Ok(())
}
