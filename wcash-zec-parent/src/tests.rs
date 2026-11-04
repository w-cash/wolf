use super::*;

const FIXTURES: [(&str, u32, &str, &str); 2] = [
    (
        include_str!("../test-vectors/nu7-v5-coinbase.hex"),
        5,
        "cdf0b8040c1bf060fbdb32e0073483054581adfdd25a57406695a9df74096876",
        "1d338a9291232e6d46273f21482c7a918548c04f3622af67f06e61990b92fa31",
    ),
    (
        include_str!("../test-vectors/nu7-v6-coinbase.hex"),
        6,
        "7a3eb6ba3f4cf7b19c7f7b2380d2fb7a691e158c17ac883f7e838683f4d43cc7",
        "554b5da95a3e645775ed8ce2f82dd8fb44857fbec99d4517f6181003d07d8048",
    ),
];

#[test]
fn constructed_nu7_coinbases_match_independent_raw_digest_vectors() {
    use chain::serialization::ZcashSerialize;

    // Generated directly from the v5/v6 wire grammar. Python hashlib computes
    // the expected ZIP-244/229 digests independently; these are not solved or
    // parent-consensus-valid reward fixtures.
    for (encoded, version, txid, auth) in FIXTURES {
        let bytes = hex::decode(encoded.trim()).unwrap();
        assert_eq!(&bytes[8..12], &NU7_BRANCH_ID.to_le_bytes());
        let tx = canonical_nu7_coinbase(&bytes).unwrap();
        assert_eq!(tx.version(), version);
        assert_eq!(u32::from(tx.consensus_branch_id()), NU7_BRANCH_ID);
        assert_eq!(hex::encode(tx.hash().0), txid);
        assert_eq!(hex::encode(tx.auth_digest().unwrap().0), auth);
        assert!(matches!(
            tx.inputs()[0],
            chain::transparent::Input::Coinbase {
                height: chain::block::Height(3_543_405),
                ..
            }
        ));
        assert_eq!(tx.zcash_serialize_to_vec().unwrap(), bytes);
        assert!(tx.inputs()[0]
            .miner_data()
            .unwrap()
            .starts_with(b"nu7-fixture"));
    }
}

#[test]
fn nu7_parser_rejects_noncanonical_wrong_domain_and_noncoinbase_bytes() {
    for (encoded, _, _, _) in FIXTURES {
        let bytes = hex::decode(encoded.trim()).unwrap();
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(matches!(
            canonical_nu7_coinbase(&trailing),
            Err(ParentError::NonCanonicalCoinbase)
        ));
        let mut old_branch = bytes.clone();
        old_branch[8..12].copy_from_slice(&0x37a5_165bu32.to_le_bytes());
        assert!(matches!(
            canonical_nu7_coinbase(&old_branch),
            Err(ParentError::UnsupportedBranch(0x37a5_165b))
        ));
        let mut not_coinbase = bytes.clone();
        not_coinbase[21] = 1;
        assert!(matches!(
            canonical_nu7_coinbase(&not_coinbase),
            Err(ParentError::NotCoinbase)
        ));
        let mut bad_height = bytes.clone();
        bad_height[58] = 0;
        assert!(canonical_nu7_coinbase(&bad_height).is_err());
        let mut oversized_count = bytes[..20].to_vec();
        oversized_count.push(0xff);
        oversized_count.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(canonical_nu7_coinbase(&oversized_count).is_err());
        for length in [0, 8, 12, 20, 58, bytes.len() - 1] {
            assert!(canonical_nu7_coinbase(&bytes[..length]).is_err());
        }
    }
    assert!(matches!(
        canonical_nu7_coinbase(&vec![0; MAX_PARENT_COINBASE_BYTES + 1]),
        Err(ParentError::CoinbaseTooLarge)
    ));
}
