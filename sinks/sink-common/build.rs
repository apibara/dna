use std::{env, path::PathBuf};

static SINK_DESCRIPTOR_FILE: &str = "sink_v1alpha2_descriptor.bin";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    println!("cargo:rerun-if-changed=proto/sink");

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(out_dir.join(SINK_DESCRIPTOR_FILE))
        .compile(&["proto/sink/v1/status.proto"], &["proto/sink"])?;

    Ok(())
}
