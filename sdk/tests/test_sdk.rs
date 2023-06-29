use apibara_core::starknet::v1alpha2::{Block, Filter, HeaderFilter};
use apibara_sdk::{configuration, ClientBuilder, Configuration, Uri};
use futures_util::{StreamExt, TryStreamExt};

#[tokio::test]
#[ignore]
async fn test_apibara_high_level_api() -> Result<(), Box<dyn std::error::Error>> {
    let (config_client, config_stream) = configuration::channel(128);

    config_client
        .send(
            Configuration::<Filter>::default()
                .with_starting_block(800_000)
                .with_filter(|mut filter| filter.with_header(HeaderFilter::new()).build()),
        )
        .await?;

    let uri = Uri::from_static("https://goerli.starknet.a5a.ch");
    let stream = ClientBuilder::<Filter, Block>::default()
        .with_bearer_token("my_auth_token".into())
        .connect(uri, config_stream)
        .await?;

    let mut stream = stream.take(2);
    while let Some(response) = stream.try_next().await.unwrap() {
        println!("Response: {:?}", response);
    }

    Ok(())
}
