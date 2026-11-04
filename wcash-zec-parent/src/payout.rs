//! Exact recipient recovery for standard Zcash parent coinbase transactions.

use crate::{chain::transaction::Transaction, ZcashAddress};
use zec_parent_address::unified::Receiver;
use zec_parent_transparent::address::TransparentAddress;

/// Recovers all outputs to the configured parent recipient using the coinbase zero OVK.
/// Unrecoverable or differently addressed shielded outputs, unsupported domains,
/// or amount/count overflows fail closed. Mandatory transparent funding outputs
/// are checked independently when validating the exact parent proposal.
pub fn recover_coinbase_payout(
    transaction: &Transaction,
    expected_address: &ZcashAddress,
) -> Option<(u64, usize)> {
    if !transaction.is_coinbase() {
        return None;
    }

    // Reject unsupported consensus branch IDs before reading the already-native
    // librustzcash representation.
    transaction.network_upgrade()?;
    let transaction = &**transaction;
    let mut recovered_value = 0u64;
    let mut matching_output_count = 0usize;

    if let Some(bundle) = transaction.transparent_bundle() {
        for output in &bundle.vout {
            let receiver = match output.recipient_address()? {
                TransparentAddress::PublicKeyHash(bytes) => Receiver::P2pkh(bytes),
                TransparentAddress::ScriptHash(bytes) => Receiver::P2sh(bytes),
            };
            if expected_address.matches_receiver(&receiver) {
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

#[cfg(test)]
mod tests;
