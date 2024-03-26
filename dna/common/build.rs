use std::{fs, io::Result, path::PathBuf, println, str::FromStr};

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=schema");

    let out_dir =
        PathBuf::from_str(&format!("{}/src/segment/store", env!("CARGO_MANIFEST_DIR"))).unwrap();

    if out_dir.exists() {
        fs::remove_dir_all(&out_dir).unwrap();
    }

    fs::create_dir_all(&out_dir).unwrap();

    let output = std::process::Command::new("flatc")
        .args([
            "--rust",
            "--gen-all",
            "--gen-mutable",
            "--gen-name-strings",
            "--rust-module-root-file",
            "-o",
            out_dir.to_str().unwrap(),
            &format!("{}/schema/store.fbs", env!("CARGO_MANIFEST_DIR")),
        ])
        .output()
        .expect("failed to execute flatc");

    if !output.status.success() {
        panic!("{}", String::from_utf8_lossy(&output.stdout));
    }

    Ok(())
}
