//! Contains code that interfaces with the zcash_note_encryption crate from
//! librustzcash.

use std::ops::Deref;

use crate::{
    block::Height,
    parameters::{Network, NetworkUpgrade},
    transaction::Transaction,
};
use zcash_address::{unified::Receiver, ZcashAddress};
use zcash_transparent::address::TransparentAddress;

/// Returns the total publicly recoverable coinbase value sent to `expected_address`, provided
/// every shielded output uses one of its receivers.
///
/// Standard Zcash shielded coinbase outputs use the all-zero outgoing viewing
/// key, so their actual recipients can be recovered and checked by independent
/// template consumers. Transparent outputs sent to the expected receiver are included; other
/// transparent outputs are allowed because a standard Zcash coinbase can contain mandatory
/// funding-stream outputs. Callers must compare those outputs with an independent template to
/// prevent a template source from changing them. `None` means the transaction is not a supported
/// coinbase, an output could not be decoded or recovered, or a shielded output pays a different
/// receiver.
///
/// This is intentionally unsuitable for private Wcash Ironwood coinbases, whose miner output uses
/// ordinary note encryption and must not be recoverable this way.
pub fn publicly_recoverable_coinbase_value_to(
    transaction: &Transaction,
    expected_address: &ZcashAddress,
) -> Option<u64> {
    publicly_recoverable_coinbase_value_and_output_count_to(transaction, expected_address)
        .map(|(value, _output_count)| value)
}

/// Returns the total publicly recoverable coinbase value sent to `expected_address` and the
/// number of matching outputs, provided every shielded output uses one of its receivers.
///
/// The count distinguishes a genuine zero-valued payout in the subsidy tail from a coinbase that
/// has no output for the configured miner. See [`publicly_recoverable_coinbase_value_to`] for the
/// payout-recovery and validation rules shared by both APIs.
pub fn publicly_recoverable_coinbase_value_and_output_count_to(
    transaction: &Transaction,
    expected_address: &ZcashAddress,
) -> Option<(u64, usize)> {
    publicly_recoverable_coinbase_value_to_inner(transaction, expected_address, true)
}

/// Returns only the publicly recoverable shielded coinbase value sent to
/// `expected_address`, provided every shielded output uses one of its receivers.
///
/// This compatibility API deliberately excludes transparent output values. New
/// template consumers that support transparent miner payouts should call
/// [`publicly_recoverable_coinbase_value_to`] instead.
pub fn publicly_recoverable_coinbase_shielded_value_to(
    transaction: &Transaction,
    expected_address: &ZcashAddress,
) -> Option<u64> {
    publicly_recoverable_coinbase_value_to_inner(transaction, expected_address, false)
        .map(|(value, _output_count)| value)
}

fn publicly_recoverable_coinbase_value_to_inner(
    transaction: &Transaction,
    expected_address: &ZcashAddress,
    include_transparent: bool,
) -> Option<(u64, usize)> {
    if !transaction.is_coinbase() {
        return None;
    }

    // Reject unsupported consensus branch IDs before reading the already-native
    // librustzcash representation.
    transaction.network_upgrade()?;
    let transaction = transaction.inner();
    let mut recovered_value = 0u64;
    let mut matching_output_count = 0usize;

    if let Some(bundle) = transaction.transparent_bundle() {
        for output in &bundle.vout {
            let receiver = match output.recipient_address()? {
                TransparentAddress::PublicKeyHash(bytes) => Receiver::P2pkh(bytes),
                TransparentAddress::ScriptHash(bytes) => Receiver::P2sh(bytes),
            };
            if include_transparent && expected_address.matches_receiver(&receiver) {
                recovered_value = recovered_value.checked_add(output.value().into_u64())?;
                matching_output_count = matching_output_count.checked_add(1)?;
            }
        }
    }

    let mut add_shielded = |receiver: Receiver, value: u64| -> Option<()> {
        if !expected_address.matches_receiver(&receiver) {
            return None;
        }
        recovered_value = recovered_value.checked_add(value)?;
        matching_output_count = matching_output_count.checked_add(1)?;
        Some(())
    };

    let zero_sapling_ovk = sapling_crypto::keys::OutgoingViewingKey([0u8; 32]);
    if let Some(bundle) = transaction.sapling_bundle() {
        for output in bundle.shielded_outputs() {
            let (note, recipient, _) =
                sapling_crypto::note_encryption::try_sapling_output_recovery(
                    &zero_sapling_ovk,
                    output,
                    sapling_crypto::note_encryption::Zip212Enforcement::On,
                )?;
            add_shielded(
                Receiver::Sapling(recipient.to_bytes()),
                note.value().inner(),
            )?;
        }
    }

    let zero_orchard_ovk = orchard::keys::OutgoingViewingKey::from([0u8; 32]);
    if let Some(bundle) = transaction.orchard_bundle() {
        for action in bundle.actions() {
            let (note, recipient, _) = zcash_note_encryption::try_output_recovery_with_ovk(
                &orchard::note_encryption::OrchardDomain::for_action(action),
                &zero_orchard_ovk,
                action,
                action.cv_net(),
                &action.encrypted_note().out_ciphertext,
            )?;
            add_shielded(
                Receiver::Orchard(recipient.to_raw_address_bytes()),
                note.value().inner(),
            )?;
        }
    }

    if let Some(bundle) = transaction.ironwood_bundle() {
        for action in bundle.actions() {
            let (note, recipient, _) = zcash_note_encryption::try_output_recovery_with_ovk(
                &orchard::note_encryption::IronwoodDomain::for_action(action),
                &zero_orchard_ovk,
                action,
                action.cv_net(),
                &action.encrypted_note().out_ciphertext,
            )?;
            add_shielded(
                Receiver::Orchard(recipient.to_raw_address_bytes()),
                note.value().inner(),
            )?;
        }
    }

    Some((recovered_value, matching_output_count))
}

/// Returns true if all Sapling, Orchard, or Ironwood outputs, if any, decrypt successfully
/// with an all-zeroes outgoing viewing key.
pub fn decrypts_successfully(tx: &Transaction, network: &Network, height: Height) -> bool {
    let nu = NetworkUpgrade::current(network, height);

    let null_sapling_ovk = sapling_crypto::keys::OutgoingViewingKey([0u8; 32]);

    // Note that, since this function is used to validate coinbase transactions, we can ignore
    // the "grace period" mentioned in ZIP-212.
    let zip_212_enforcement = if nu >= NetworkUpgrade::Canopy {
        sapling_crypto::note_encryption::Zip212Enforcement::On
    } else {
        sapling_crypto::note_encryption::Zip212Enforcement::Off
    };

    if let Some(bundle) = tx.inner().deref().sapling_bundle() {
        for output in bundle.shielded_outputs().iter() {
            let recovery = sapling_crypto::note_encryption::try_sapling_output_recovery(
                &null_sapling_ovk,
                output,
                zip_212_enforcement,
            );
            if recovery.is_none() {
                return false;
            }
        }
    }

    if let Some(bundle) = tx.inner().deref().orchard_bundle() {
        for act in bundle.actions() {
            if zcash_note_encryption::try_output_recovery_with_ovk(
                &orchard::note_encryption::OrchardDomain::for_action(act),
                &orchard::keys::OutgoingViewingKey::from([0u8; 32]),
                act,
                act.cv_net(),
                &act.encrypted_note().out_ciphertext,
            )
            .is_none()
            {
                return false;
            }
        }
    }

    // From NU6.3, newly shielded coinbase value is routed to the Ironwood pool, so the coinbase
    // output-decryptability rule must cover Ironwood actions too. The Ironwood bundle reuses the
    // Orchard action shape but its notes use the `IronwoodDomain` (V3) note-plaintext version.
    if let Some(bundle) = tx.ironwood_bundle() {
        for act in bundle.actions() {
            if zcash_note_encryption::try_output_recovery_with_ovk(
                &orchard::note_encryption::IronwoodDomain::for_action(act),
                &orchard::keys::OutgoingViewingKey::from([0u8; 32]),
                act,
                act.cv_net(),
                &act.encrypted_note().out_ciphertext,
            )
            .is_none()
            {
                return false;
            }
        }
    }

    true
}

/// Returns true only if no Ironwood action can be recovered with Zcash's
/// conventional all-zero outgoing viewing key.
///
/// Wcash uses this as a consensus privacy rule for coinbase transactions. It
/// checks every action independently: one deliberately public reward cannot be
/// hidden by adding a private padding action. A transaction conversion failure
/// returns false so callers fail closed.
pub fn ironwood_outputs_are_private_from_zero_ovk(
    tx: &Transaction,
    network: &Network,
    height: Height,
) -> bool {
    let nu = NetworkUpgrade::current(network, height);
    if tx.network_upgrade() != Some(nu) {
        return false;
    }
    let tx = tx.inner();
    let zero_ovk = orchard::keys::OutgoingViewingKey::from([0u8; 32]);

    tx.ironwood_bundle().is_none_or(|bundle| {
        bundle.actions().iter().all(|action| {
            zcash_note_encryption::try_output_recovery_with_ovk(
                &orchard::note_encryption::IronwoodDomain::for_action(action),
                &zero_ovk,
                action,
                action.cv_net(),
                &action.encrypted_note().out_ciphertext,
            )
            .is_none()
        })
    })
}

#[cfg(test)]
mod tests {
    use zcash_address::{
        unified::{Address as UnifiedAddress, Encoding},
        ToAddress,
    };
    use zcash_protocol::{
        consensus::{BranchId, NetworkType},
        value::ZatBalance,
    };

    use crate::{
        amount::{Amount, NonNegative},
        parameters::{NetworkKind, NetworkUpgrade},
        transaction::{arbitrary::fake_orchard_bundle_with_note, LockTime},
        transparent::{Address as TransparentAddress, Input, Output, Script},
    };

    use super::*;

    fn coinbase_inputs() -> Vec<Input> {
        vec![Input::Coinbase {
            height: Height(1),
            data: vec![0x51],
            sequence: u32::MAX,
        }]
    }

    fn transparent_output(receiver: Receiver, value: i64) -> Output {
        let address = match receiver {
            Receiver::P2pkh(hash) => {
                TransparentAddress::from_pub_key_hash(NetworkKind::Testnet, hash)
            }
            Receiver::P2sh(hash) => {
                TransparentAddress::from_script_hash(NetworkKind::Testnet, hash)
            }
            _ => panic!("transparent fixture requires a transparent receiver"),
        };
        Output::new(
            Amount::<NonNegative>::try_from(value).expect("fixture value is non-negative"),
            address.script(),
        )
    }

    fn transparent_coinbase(outputs: Vec<Output>) -> Transaction {
        Transaction::test_v6(
            NetworkUpgrade::Nu6_3,
            coinbase_inputs(),
            outputs,
            LockTime::unlocked(),
            Height(0),
        )
    }

    #[test]
    fn recoverable_coinbase_value_matches_transparent_receiver_types() {
        let funding_output = transparent_output(Receiver::P2sh([0xf0; 20]), 25_000);
        let cases = [
            (
                ZcashAddress::from_transparent_p2pkh(NetworkType::Test, [0x11; 20]),
                Receiver::P2pkh([0x11; 20]),
            ),
            (
                ZcashAddress::from_transparent_p2sh(NetworkType::Test, [0x22; 20]),
                Receiver::P2sh([0x22; 20]),
            ),
            (
                ZcashAddress::from_tex(NetworkType::Test, [0x33; 20]),
                Receiver::P2pkh([0x33; 20]),
            ),
        ];

        for (expected_address, receiver) in cases {
            let coinbase = transparent_coinbase(vec![
                transparent_output(receiver, 625_012_345),
                funding_output.clone(),
            ]);
            assert_eq!(
                publicly_recoverable_coinbase_value_to(&coinbase, &expected_address),
                Some(625_012_345),
                "only outputs owned by the configured receiver are counted"
            );
            assert_eq!(
                publicly_recoverable_coinbase_value_and_output_count_to(
                    &coinbase,
                    &expected_address
                ),
                Some((625_012_345, 1)),
                "the matching-output count authenticates the configured recipient"
            );
            assert_eq!(
                publicly_recoverable_coinbase_shielded_value_to(&coinbase, &expected_address),
                Some(0),
                "the compatibility API continues to exclude transparent value"
            );
        }
    }

    #[test]
    fn recoverable_coinbase_count_distinguishes_zero_payout_from_missing_recipient() {
        let expected_address = ZcashAddress::from_transparent_p2pkh(NetworkType::Test, [0x11; 20]);
        let other_receiver = Receiver::P2pkh([0x22; 20]);

        let zero_payout =
            transparent_coinbase(vec![transparent_output(Receiver::P2pkh([0x11; 20]), 0)]);
        assert_eq!(
            publicly_recoverable_coinbase_value_and_output_count_to(
                &zero_payout,
                &expected_address
            ),
            Some((0, 1)),
            "a zero-valued output still authenticates its recipient"
        );

        let missing_recipient =
            transparent_coinbase(vec![transparent_output(other_receiver, 25_000)]);
        assert_eq!(
            publicly_recoverable_coinbase_value_and_output_count_to(
                &missing_recipient,
                &expected_address
            ),
            Some((0, 0)),
            "a zero sum alone must not imply that the configured recipient is present"
        );
    }

    #[test]
    fn recoverable_coinbase_value_rejects_nonstandard_transparent_output() {
        let expected_address = ZcashAddress::from_transparent_p2pkh(NetworkType::Test, [0x11; 20]);
        let nonstandard = Output::new(
            Amount::<NonNegative>::try_from(1).expect("one zatoshi is valid"),
            Script::new(&[0x6a]),
        );
        let coinbase = transparent_coinbase(vec![
            transparent_output(Receiver::P2pkh([0x11; 20]), 625_000_000),
            nonstandard,
        ]);

        assert_eq!(
            publicly_recoverable_coinbase_value_to(&coinbase, &expected_address),
            None,
            "an unclassified transparent output must fail closed"
        );
    }

    #[test]
    fn recoverable_coinbase_value_rejects_wrong_shielded_receiver() {
        use crate::transaction::arbitrary::outputs_enabled_flags;

        let vector = &zebra_test::vectors::ORCHARD_NOTE_ENCRYPTION_ZERO_VECTOR[0];
        let bundle_version =
            zcash_primitives::transaction::components::orchard::bundle_version_for_branch(
                BranchId::Nu5,
                orchard::ValuePool::Orchard,
            )
            .expect("NU5 defines the Orchard pool");
        let bundle = fake_orchard_bundle_with_note(
            outputs_enabled_flags(bundle_version),
            ZatBalance::from_i64(0).expect("zero is a valid balance"),
            bundle_version,
            &vector.cv_net,
            &vector.rho,
            &vector.cmx,
            vector.ephemeral_key,
            vector.c_enc,
            vector.c_out,
        );
        let coinbase = Transaction::test_v5_with_orchard(
            NetworkUpgrade::Nu5,
            coinbase_inputs(),
            Vec::new(),
            LockTime::unlocked(),
            Height(0),
            Some(bundle),
        );
        let mut raw_receiver = [0; 43];
        raw_receiver[..11].copy_from_slice(&vector.default_d);
        raw_receiver[11..].copy_from_slice(&vector.default_pk_d);
        let matching_address = ZcashAddress::from_unified(
            NetworkType::Test,
            UnifiedAddress::try_from_items(vec![Receiver::Orchard(raw_receiver)])
                .expect("the vector receiver forms a Unified Address"),
        );
        let wrong_address = ZcashAddress::from_transparent_p2pkh(NetworkType::Test, [0x44; 20]);

        assert_eq!(
            publicly_recoverable_coinbase_value_to(&coinbase, &matching_address),
            Some(vector.v),
            "the zero-OVK vector recovers to its exact receiver"
        );
        assert_eq!(
            publicly_recoverable_coinbase_value_to(&coinbase, &wrong_address),
            None,
            "a publicly recoverable shielded output to another receiver must fail closed"
        );
    }
}
