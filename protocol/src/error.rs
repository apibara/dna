use snafu::Snafu;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum DecodeError {
    #[snafu(display("Size too big"))]
    SizeTooBig,
    #[snafu(display("Missing 0x prefix"))]
    Missing0xPrefix,
    #[snafu(display("Failed to decode from hex"))]
    DecodeFromHex { source: hex::FromHexError },
}
