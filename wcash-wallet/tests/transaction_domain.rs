//! Cross-crate tests for Wcash transaction-domain propagation.

use pczt::roles::creator::Creator;
use zcash_primitives::transaction::builder::{BuildConfig, Builder};
use zcash_protocol::{
    consensus::{BlockHeight, BranchId},
    constants::{V6_TX_VERSION, V6_VERSION_GROUP_ID},
};
use zebra_chain::parameters::Network;

#[test]
fn wcash_builder_rejects_standard_nu6_3_branch() {
    let params = Network::new_wcash_testnet();
    let target_height = BlockHeight::from_u32(1);
    let build_config = || BuildConfig::Coinbase { miner_data: None };

    Builder::new_with_branch_id(
        params.clone(),
        target_height,
        BranchId::WcashTestnetV1,
        build_config(),
    )
    .unwrap_or_else(|error| panic!("exact Wcash branch must be accepted: {error}"));

    let mismatch =
        match Builder::new_with_branch_id(params, target_height, BranchId::Nu6_3, build_config()) {
            Ok(_) => panic!("standard Zcash NU6.3 branch must be rejected for Wcash"),
            Err(error) => error,
        };

    assert_eq!(mismatch.expected(), BranchId::WcashTestnetV1);
    assert_eq!(mismatch.actual(), BranchId::Nu6_3);
}

#[test]
fn wcash_pczt_creator_preserves_v6_custom_branch() {
    let branch_id = u32::from(BranchId::WcashTestnetV1);
    let pczt = Creator::new(branch_id, 2, 133, Some([0; 32]), Some([0; 32]))
        .expect("the Wcash branch must be recognized")
        .build()
        .expect("an empty Wcash PCZT must be constructible");

    let encoded = pczt
        .serialize()
        .expect("the Wcash PCZT must have a stable wire encoding");
    let decoded = pczt::parse(&encoded).expect("the encoded Wcash PCZT must parse");

    assert_eq!(*decoded.global().tx_version(), V6_TX_VERSION);
    assert_eq!(*decoded.global().version_group_id(), V6_VERSION_GROUP_ID);
    assert_eq!(*decoded.global().consensus_branch_id(), branch_id);
}
