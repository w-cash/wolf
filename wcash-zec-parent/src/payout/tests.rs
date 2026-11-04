//! These are genuine NU7 v6 encodings and real zero-OVK note ciphertexts.
//! Their bundle proofs and binding signatures are deliberately zeroed: they
//! exercise payout recovery, not proof verification or block acceptance.

use std::convert::Infallible;

use orchard::{
    bundle::{Authorized as OrchardAuthorized, BundleVersion},
    keys::{FullViewingKey, OutgoingViewingKey, Scope, SpendingKey},
    note::{ExtractedNoteCommitment, NoteVersion, RandomSeed, Rho},
    note_encryption::{IronwoodDomain, IronwoodNoteEncryption},
    value::{NoteValue, ValueCommitTrapdoor, ValueCommitment},
    Bundle, Note,
};
use zcash_note_encryption::Domain;
use zec_parent_address::{
    unified::{Address as UnifiedAddress, Encoding, Receiver, Revision, Uitem},
    ToAddress,
};
use zec_parent_chain::{
    block::Height,
    parameters::NetworkUpgrade,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transaction::{
        arbitrary::{fake_orchard_bundle_with_note, outputs_enabled_flags},
        LockTime,
    },
    transparent::Input,
};
use zec_parent_primitives::transaction::{Authorized, TransactionData};
use zec_parent_protocol::{
    consensus::{BlockHeight, BranchId, NetworkType},
    value::ZatBalance,
};

use super::*;

const HEIGHT: u32 = 3_543_405;
const VALUE: u64 = 625_012_345;

/// ZIP-212 and Ironwood derive the ephemeral secret from the fixed note seed;
/// Some(ovk) derives outgoing encryption too. These fixtures need no randomness.
struct NoRandomness;
impl rand_core::TryRng for NoRandomness {
    type Error = Infallible;
    fn try_next_u32(&mut self) -> Result<u32, Self::Error> {
        panic!("fixture unexpectedly requested randomness")
    }
    fn try_next_u64(&mut self) -> Result<u64, Self::Error> {
        panic!("fixture unexpectedly requested randomness")
    }
    fn try_fill_bytes(&mut self, _: &mut [u8]) -> Result<(), Self::Error> {
        panic!("fixture unexpectedly requested randomness")
    }
}

#[derive(Clone, Copy)]
enum Corruption {
    None,
    NoteCiphertext,
    OutgoingCiphertext,
}

fn parent(
    sapling: Option<sapling_crypto::Bundle<sapling_crypto::bundle::Authorized, ZatBalance>>,
    ironwood: Option<Bundle<OrchardAuthorized, ZatBalance>>,
) -> Transaction {
    let baseline = Transaction::test_v6(
        NetworkUpgrade::Nu7,
        vec![Input::Coinbase {
            height: Height(HEIGHT),
            data: vec![0x51],
            sequence: u32::MAX,
        }],
        vec![],
        LockTime::unlocked(),
        Height(HEIGHT),
    );
    let data = TransactionData::<Authorized>::from_parts_v6(
        BranchId::Nu7,
        0,
        BlockHeight::from_u32(HEIGHT),
        baseline.transparent_bundle().cloned(),
        sapling,
        None,
        ironwood,
    );
    let tx = data.freeze().unwrap();
    let mut bytes = Vec::new();
    tx.write(&mut bytes).unwrap();
    assert_eq!(&bytes[8..12], &0x7719_0ad9u32.to_le_bytes());
    let parsed: Transaction = bytes.as_slice().zcash_deserialize_into().unwrap();
    assert_eq!(parsed.network_upgrade(), Some(NetworkUpgrade::Nu7));
    assert_eq!(parsed.zcash_serialize_to_vec().unwrap(), bytes);
    parsed
}

fn ironwood(ovk: [u8; 32], corruption: Corruption) -> (Transaction, ZcashAddress) {
    let sk = SpendingKey::from_bytes([7; 32]).unwrap();
    let recipient = FullViewingKey::from(&sk).address_at(0u32, Scope::External);
    let rho = Rho::from_bytes(&[2; 32]).unwrap();
    let rseed = RandomSeed::from_bytes([3; 32], &rho).unwrap();
    let note = Note::from_parts(
        recipient,
        NoteValue::from_raw(VALUE),
        rho,
        rseed,
        NoteVersion::V3,
    )
    .unwrap();
    let cv = ValueCommitment::derive(
        NoteValue::ZERO - NoteValue::from_raw(VALUE),
        ValueCommitTrapdoor::from_bytes([0; 32]).unwrap(),
    );
    let cmx = ExtractedNoteCommitment::from(note.commitment());
    let encryption =
        IronwoodNoteEncryption::new(Some(OutgoingViewingKey::from(ovk)), note, [0x37; 512]);
    let mut ciphertext = encryption.encrypt_note_plaintext().0;
    let mut outgoing = encryption.encrypt_outgoing_plaintext(&cv, &cmx, &mut NoRandomness);
    match corruption {
        Corruption::None => {}
        Corruption::NoteCiphertext => ciphertext[0] ^= 1,
        Corruption::OutgoingCiphertext => outgoing[0] ^= 1,
    }
    let version = BundleVersion::ironwood_v3();
    let bundle = fake_orchard_bundle_with_note(
        outputs_enabled_flags(version),
        ZatBalance::from_i64(-i64::try_from(VALUE).unwrap()).unwrap(),
        version,
        &cv.to_bytes(),
        &rho.to_bytes(),
        &cmx.to_bytes(),
        IronwoodDomain::epk_bytes(encryption.epk()).0,
        ciphertext,
        outgoing,
    );
    let address = ZcashAddress::from_unified(
        NetworkType::Main,
        UnifiedAddress::try_from_items(
            Revision::R0,
            vec![Uitem::Data(Receiver::Orchard(
                recipient.to_raw_address_bytes(),
            ))],
        )
        .unwrap(),
    );
    (parent(None, Some(bundle)), address)
}

fn sapling(ovk: [u8; 32], corruption: Corruption) -> (Transaction, ZcashAddress) {
    use sapling_crypto::{
        bundle::{Authorized as SaplingAuthorized, OutputDescription},
        keys::OutgoingViewingKey,
        note_encryption::{sapling_note_encryption, SaplingDomain},
        value::{NoteValue, ValueCommitTrapdoor, ValueCommitment},
        zip32::ExtendedSpendingKey,
        Rseed,
    };
    let recipient = ExtendedSpendingKey::master(&[9; 32])
        .unwrap()
        .default_address()
        .1;
    let value = NoteValue::from_raw(VALUE);
    let note = recipient.create_note(value, Rseed::AfterZip212([0x42; 32]));
    let cmu = note.cmu();
    let cv = ValueCommitment::derive(value, ValueCommitTrapdoor::from_bytes([0; 32]).unwrap());
    let encryption = sapling_note_encryption(
        Some(OutgoingViewingKey(ovk)),
        note,
        [0x37; 512],
        &mut NoRandomness,
    );
    let mut ciphertext = encryption.encrypt_note_plaintext().0;
    let mut outgoing = encryption.encrypt_outgoing_plaintext(&cv, &cmu, &mut NoRandomness);
    match corruption {
        Corruption::None => {}
        Corruption::NoteCiphertext => ciphertext[0] ^= 1,
        Corruption::OutgoingCiphertext => outgoing[0] ^= 1,
    }
    let output = OutputDescription::from_parts(
        cv,
        cmu,
        SaplingDomain::epk_bytes(encryption.epk()),
        ciphertext,
        outgoing,
        [0; 192],
    );
    let bundle = sapling_crypto::Bundle::from_parts(
        vec![],
        vec![output],
        ZatBalance::from_i64(-i64::try_from(VALUE).unwrap()).unwrap(),
        SaplingAuthorized {
            binding_sig: [0u8; 64].into(),
        },
    )
    .unwrap();
    let address = ZcashAddress::from_unified(
        NetworkType::Main,
        UnifiedAddress::try_from_items(
            Revision::R0,
            vec![Uitem::Data(Receiver::Sapling(recipient.to_bytes()))],
        )
        .unwrap(),
    );
    (parent(Some(bundle), None), address)
}

#[test]
fn nu7_ironwood_zero_ovk_recovers_the_exact_nonzero_ua_recipient() {
    let (transaction, address) = ironwood([0; 32], Corruption::None);
    assert_eq!(
        recover_coinbase_payout(&transaction, &address),
        Some((VALUE, 1))
    );
    let wrong = ZcashAddress::from_transparent_p2pkh(NetworkType::Main, [0x66; 20]);
    assert_eq!(recover_coinbase_payout(&transaction, &wrong), None);
    let (unrecoverable, address) = ironwood([1; 32], Corruption::None);
    assert_eq!(recover_coinbase_payout(&unrecoverable, &address), None);
}

#[test]
fn nu7_ironwood_corrupted_note_or_outgoing_ciphertext_fails_closed() {
    for corruption in [Corruption::NoteCiphertext, Corruption::OutgoingCiphertext] {
        let (transaction, address) = ironwood([0; 32], corruption);
        assert_eq!(recover_coinbase_payout(&transaction, &address), None);
    }
}

#[test]
fn nu7_sapling_zero_ovk_recovers_the_exact_nonzero_ua_recipient() {
    let (transaction, address) = sapling([0; 32], Corruption::None);
    assert_eq!(
        recover_coinbase_payout(&transaction, &address),
        Some((VALUE, 1))
    );
    let wrong = ZcashAddress::from_transparent_p2pkh(NetworkType::Main, [0x77; 20]);
    assert_eq!(recover_coinbase_payout(&transaction, &wrong), None);
    let (unrecoverable, address) = sapling([1; 32], Corruption::None);
    assert_eq!(recover_coinbase_payout(&unrecoverable, &address), None);
}

#[test]
fn nu7_sapling_corrupted_note_or_outgoing_ciphertext_fails_closed() {
    for corruption in [Corruption::NoteCiphertext, Corruption::OutgoingCiphertext] {
        let (transaction, address) = sapling([0; 32], corruption);
        assert_eq!(recover_coinbase_payout(&transaction, &address), None);
    }
}
