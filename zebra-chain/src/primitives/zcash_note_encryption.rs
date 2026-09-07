//! Contains code that interfaces with the zcash_note_encryption crate from
//! librustzcash.

use crate::{
    block::Height,
    parameters::{Network, NetworkUpgrade},
    transaction::Transaction,
};

/// Returns true if all Sapling, Orchard, or Ironwood outputs, if any, decrypt successfully
/// with an all-zeroes outgoing viewing key.
pub fn decrypts_successfully(tx: &Transaction, network: &Network, height: Height) -> bool {
    let nu = NetworkUpgrade::current(network, height);

    let Ok(tx) = tx.to_librustzcash(nu) else {
        return false;
    };

    let null_sapling_ovk = sapling_crypto::keys::OutgoingViewingKey([0u8; 32]);

    // Note that, since this function is used to validate coinbase transactions, we can ignore
    // the "grace period" mentioned in ZIP-212.
    let zip_212_enforcement = if nu >= NetworkUpgrade::Canopy {
        sapling_crypto::note_encryption::Zip212Enforcement::On
    } else {
        sapling_crypto::note_encryption::Zip212Enforcement::Off
    };

    if let Some(bundle) = tx.sapling_bundle() {
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

    if let Some(bundle) = tx.orchard_bundle() {
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
    let Ok(tx) = tx.to_librustzcash(nu) else {
        return false;
    };
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
