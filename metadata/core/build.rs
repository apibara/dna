use std::{env, io::Result, path::PathBuf, println};

static ADMIN_DESCRIPTOR_FILE: &str = "wings_v1_admin.bin";

fn main() -> Result<()> {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    println!("cargo:rerun-if-changed=proto");

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .include_file("mod.rs")
        .file_descriptor_set_path(out_dir.join(ADMIN_DESCRIPTOR_FILE))
        .compile_protos(
            &[
                "proto/wings/admin.proto",
                "proto/wings/offset_registry.proto",
            ],
            &["proto/"],
        )?;

    Ok(())
}
