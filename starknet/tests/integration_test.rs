use std::time::Duration;

use anyhow::{Context, Result};
use apibara_starknet::{start_starknet_source_node, HttpProvider};
use starknet::{
    accounts::{Account, Call, SingleOwnerAccount},
    core::{chain_id, types::FieldElement, utils::get_selector_from_name},
    providers::{Provider, SequencerGatewayProvider},
    signers::{LocalWallet, SigningKey},
};
use tokio_util::sync::CancellationToken;
use url::Url;

#[tokio::test]
async fn test_starknet_integration() -> Result<()> {
    let starknet_provider = new_devnet_client()?;
    seed_devnet_transactions(&starknet_provider).await?;

    let datadir = tempfile::tempdir()?;
    let sequencer_gateway = HttpProvider::from_custom_str("http://localhost:5050/feeder_gateway")?;

    let cts = CancellationToken::new();

    tokio::spawn({
        let ct = cts.clone();
        async move {
            start_starknet_source_node(
                datadir.into_path(),
                sequencer_gateway,
                Duration::from_millis(200),
                ct.clone(),
            )
            .await
            .unwrap();
        }
    });

    // TODO: stream data from node with local client
    tokio::time::sleep(Duration::from_secs(5)).await;
    cts.cancel();
    Ok(())
}

fn new_devnet_client() -> Result<SequencerGatewayProvider> {
    let base_url = "http://localhost:5050".to_string();
    let gateway_url: Url = format!("{}/gateway", base_url).parse()?;
    let feeder_gateway_url: Url = format!("{}/feeder_gateway", base_url).parse()?;
    let provider = SequencerGatewayProvider::new(gateway_url, feeder_gateway_url);
    Ok(provider)
}

async fn seed_devnet_transactions(provider: &SequencerGatewayProvider) -> Result<()> {
    // Account-0 in starknet-devnet
    let secret_key = FieldElement::from_hex_be("0xe3e70682c2094cac629f6fbed82c07cd")
        .context("parse secret key")?;
    let address = FieldElement::from_hex_be(
        "0x7e00d496e324876bbc8531f2d9a82bf154d1a04a50218ee74cdd372f75a551a",
    )
    .context("parse address")?;

    let signer = LocalWallet::from(SigningKey::from_secret_scalar(secret_key));
    let account = SingleOwnerAccount::new(provider, signer, address, chain_id::TESTNET);

    let eth_address = FieldElement::from_hex_be(
        "0x62230ea046a9a5fbc261ac77d03c8d41e5d442db2284587570ab46455fd2488",
    )
    .context("parse eth address")?;

    let receiver_address = FieldElement::from_hex_be(
        "0x69b49c2cc8b16e80e86bfc5b0614a59aa8c9b601569c7b80dde04d3f3151b79",
    )
    .context("parse receiver address")?;

    // send a bunch of transactions
    for _ in 0..20 {
        account
            .execute(&[Call {
                to: eth_address,
                selector: get_selector_from_name("transfer").unwrap(),
                calldata: vec![
                    receiver_address,
                    FieldElement::from_dec_str("100000000000000000").unwrap(),
                    FieldElement::ZERO,
                ],
            }])
            .send()
            .await?;
    }

    // get block number, check that devnet created some
    let block = provider
        .get_block(starknet::core::types::BlockId::Latest)
        .await?;
    assert!(block.block_number.unwrap() > 0);

    Ok(())
}
