use crate::provider::models::{B256, B384};

pub fn kzg_commitment_to_versioned_hash(commitment: &B384) -> B256 {
    alloy_eips::eip4844::kzg_to_versioned_hash(&commitment.to_be_bytes::<48>())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::provider::models::{B256, B384};

    #[test]
    pub fn test_kzg_commitment_to_versioned_hash() {
        // commitment - hash
        let test_cases = [
            ("8a2461b2ad767d96d11fe783fc63023fcde21d8dd03064056fa522ebbfae185ec1f82025b627977603b014ec64c5ee19", "0x011a3bb3c2a1d4bf04e4501628ba351bd2a5eb8971daf6d6d47ca5a79d8589bb"),
            ("9007ff0d9ca54b8fe0b25ae5bdb8fa2ee30249f88c4da33a6a8d8ab09828c1100353a0f6dd0f97dfc493ac942462e2e0", "0x012ba1b06de5dfa8cf48db8e1b4934b6f4011c5ca31afeffd4c990e3b45464c5")
        ];

        for (commitment, expected) in test_cases {
            let commitment = B384::from_str_radix(commitment, 16).unwrap();
            let hash = super::kzg_commitment_to_versioned_hash(&commitment);
            let expected = B256::from_str(expected).unwrap();
            assert_eq!(hash, expected);
        }
    }
}
