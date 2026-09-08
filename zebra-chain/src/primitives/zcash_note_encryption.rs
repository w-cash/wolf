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

/// Returns the total publicly recoverable shielded coinbase value sent to
/// `expected_address`, provided every shielded output uses one of its receivers.
///
/// Standard Zcash shielded coinbase outputs use the all-zero outgoing viewing
/// key, so their actual recipients can be recovered and checked by independent
/// template consumers. Transparent outputs are decoded but deliberately not
/// included; callers can compare them with an independent template to separate
/// mandatory funding outputs from miner-controlled outputs. `None` means the
/// transaction is not a supported coinbase, an output could not be decoded or
/// recovered, or a shielded output pays a different receiver.
///
/// This is intentionally unsuitable for Wcash coinbases, whose Ironwood miner
/// output uses private note encryption and must not be recoverable this way.
pub fn publicly_recoverable_coinbase_shielded_value_to(
    transaction: &Transaction,
    expected_address: &ZcashAddress,
) -> Option<u64> {
    if !transaction.is_coinbase() {
        return None;
    }

    // Reject unsupported consensus branch IDs before reading the already-native
    // librustzcash representation.
    transaction.network_upgrade()?;
    let transaction = transaction.inner();
    let mut recovered_value = 0u64;
    let mut add_shielded = |receiver: Receiver, value: u64| -> Option<()> {
        if !expected_address.matches_receiver(&receiver) {
            return None;
        }
        recovered_value = recovered_value.checked_add(value)?;
        Some(())
    };

    if let Some(bundle) = transaction.transparent_bundle() {
        for output in &bundle.vout {
            match output.recipient_address()? {
                TransparentAddress::PublicKeyHash(_) | TransparentAddress::ScriptHash(_) => {}
            }
            let _ = output.value().into_u64();
        }
    }

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

    Some(recovered_value)
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
