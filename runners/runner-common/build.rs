use std::{env, path::PathBuf};

static RUNNER_DESCRIPTOR_FILE: &str = "runner_v1_descriptor.bin";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    println!("cargo:rerun-if-changed=proto/runner/v1");

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .protoc_arg("--experimental_allow_proto3_optional")
        .file_descriptor_set_path(out_dir.join(RUNNER_DESCRIPTOR_FILE))
        .compile(&["proto/runner/v1/runner.proto"], &["proto/runner"])?;

    Ok(())
}
