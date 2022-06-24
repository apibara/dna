fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(false)
        .build_server(true)
        .compile(
            &["proto/apibara/application/indexer_service.proto"],
            &["proto/apibara"],
        )?;
    Ok(())
}
