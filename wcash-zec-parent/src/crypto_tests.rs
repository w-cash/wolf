//! Raw ChaCha20 known-answer coverage, separate from shielded payout recovery.
//!
//! Inputs and expected ciphertext are frozen from RFC 8439 section 2.4.2:
//! https://www.rfc-editor.org/rfc/rfc8439.html#section-2.4.2
//! These host tests do not establish forced x86 SSE2 backend coverage; that
//! remains part of off-production Linux qualification.

use chacha20_legacy::cipher::{KeyIvInit as _, StreamCipher as _, StreamCipherSeek as _};
use chacha20_modern::cipher::{KeyIvInit as _, StreamCipher as _, StreamCipherSeek as _};

const KEY: [u8; 32] = [
    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
    0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
];
const NONCE: [u8; 12] = [0, 0, 0, 0, 0, 0, 0, 0x4a, 0, 0, 0, 0];
const PLAINTEXT: &[u8; 114] = b"Ladies and Gentlemen of the class of '99: If I could offer you only one tip for the future, sunscreen would be it.";
const CIPHERTEXT: &str = concat!(
    "6e2e359a2568f98041ba0728dd0d6981",
    "e97e7aec1d4360c20a27afccfd9fae0b",
    "f91b65c5524733ab8f593dabcd62b357",
    "1639d624e65152ab8f530c359f0861d8",
    "07ca0dbf500d6a6156a38e088a22b65e",
    "52bc514d16ccf806818ce91ab7793736",
    "5af90bbf74a35be6b40b8eedf2785e42",
    "874d",
);
// Both APIs express stream position in bytes: block counter 1 starts at 64.
const COUNTER_ONE_POSITION: u64 = 64;

#[test]
fn rfc8439_counter_one_ciphertext_matches_both_chacha_api_versions() {
    let expected = hex::decode(CIPHERTEXT).expect("the RFC ciphertext is fixed hexadecimal");
    let mut legacy = chacha20_legacy::ChaCha20::new(&KEY.into(), &NONCE.into());
    let mut modern = chacha20_modern::ChaCha20::new(&KEY.into(), &NONCE.into());
    legacy.seek(COUNTER_ONE_POSITION);
    modern.seek(COUNTER_ONE_POSITION);
    assert_eq!(legacy.current_pos::<u64>(), COUNTER_ONE_POSITION);
    assert_eq!(modern.current_pos::<u64>(), COUNTER_ONE_POSITION);

    let mut legacy_bytes = *PLAINTEXT;
    let mut modern_bytes = *PLAINTEXT;
    legacy.apply_keystream(&mut legacy_bytes);
    modern.apply_keystream(&mut modern_bytes);
    assert_eq!(legacy_bytes.as_slice(), expected.as_slice());
    assert_eq!(modern_bytes.as_slice(), expected.as_slice());
    assert_eq!(legacy.current_pos::<u64>(), 178);
    assert_eq!(modern.current_pos::<u64>(), 178);
}

#[test]
fn rfc8439_chunking_and_reseek_preserve_frozen_ciphertext_on_both_versions() {
    let expected = hex::decode(CIPHERTEXT).expect("the RFC ciphertext is fixed hexadecimal");
    let mut legacy = chacha20_legacy::ChaCha20::new(&KEY.into(), &NONCE.into());
    let mut modern = chacha20_modern::ChaCha20::new(&KEY.into(), &NONCE.into());
    legacy.seek(COUNTER_ONE_POSITION);
    modern.seek(COUNTER_ONE_POSITION);
    let mut legacy_bytes = *PLAINTEXT;
    let mut modern_bytes = *PLAINTEXT;
    for chunk in legacy_bytes.chunks_mut(17) {
        legacy.apply_keystream(chunk);
    }
    for chunk in modern_bytes.chunks_mut(17) {
        modern.apply_keystream(chunk);
    }
    assert_eq!(legacy_bytes.as_slice(), expected.as_slice());
    assert_eq!(modern_bytes.as_slice(), expected.as_slice());

    // Reuse consumed cipher instances and seek backward/forward around the
    // 64-byte boundary, checking each suffix against the published vector.
    for offset in [65usize, 1, 64, 0, 113, 63] {
        let position = COUNTER_ONE_POSITION + u64::try_from(offset).unwrap();
        legacy.seek(position);
        modern.seek(position);
        let mut legacy_suffix = PLAINTEXT[offset..].to_vec();
        let mut modern_suffix = PLAINTEXT[offset..].to_vec();
        legacy.apply_keystream(&mut legacy_suffix);
        modern.apply_keystream(&mut modern_suffix);
        assert_eq!(legacy_suffix.as_slice(), &expected[offset..]);
        assert_eq!(modern_suffix.as_slice(), &expected[offset..]);
    }
}

#[test]
fn published_chacha_rng_words_and_64_bit_counter_continuation_are_preserved() {
    use chacha20_modern::ChaCha20Rng;
    use rand_core::{Rng as _, SeedableRng as _};

    // Frozen published chacha20 0.10.2 tests/rng.rs values from
    // test_chacha_multiple_blocks and test_chacha_true_values_a:
    // https://docs.rs/crate/chacha20/0.10.2/source/tests/rng.rs
    let seed: [u8; 32] =
        hex::decode("0000000001000000020000000300000004000000050000000600000007000000")
            .unwrap()
            .try_into()
            .unwrap();
    let expected_samples = [
        0xf225_c81a,
        0x6ab1_be57,
        0x04d4_2951,
        0x7085_8036,
        0x4988_4684,
        0x64ef_ec72,
        0x4be2_d186,
        0x3615_b384,
        0x11cf_a18e,
        0xd3c5_0049,
        0x75c7_75f6,
        0x434c_6530,
        0x2c5b_ad8f,
        0x8988_81dc,
        0x5f1c_86d9,
        0xc1f8_e7f4,
    ];
    let mut rng = ChaCha20Rng::from_seed(seed);
    for expected in expected_samples {
        assert_eq!(rng.next_u32(), expected);
        // The published vector samples word 17*i across 17 blocks, exercising
        // continuation beyond each four-block backend buffer refill.
        for _ in 0..16 {
            rng.next_u32();
        }
    }
    assert_eq!(rng.get_word_pos(), 272);

    let zero_seed_first_block = [
        0xade0_b876,
        0x903d_f1a0,
        0xe56a_5d40,
        0x28bd_8653,
        0xb819_d2bd,
        0x1aed_8da0,
        0xccef_36a8,
        0xc70d_778b,
        0x7c59_41da,
        0x8d48_5751,
        0x3fe0_2477,
        0x374a_d8b8,
        0xf4b8_436a,
        0x1ca1_1815,
        0x69b6_87c3,
        0x8665_eeb2,
    ];
    let mut boundary_rng = ChaCha20Rng::from_seed([0; 32]);
    boundary_rng.set_block_pos(u64::from(u32::MAX));
    let mut boundary_words = [0u32; 16 * 5];
    for word in &mut boundary_words {
        *word = boundary_rng.next_u32();
    }
    // The RNG uses a 64-bit block counter. Crossing 2^32 must neither reset
    // the logical position nor repeat the published counter-zero block.
    assert_eq!(boundary_rng.get_block_pos(), (1u64 << 32) + 4);
    assert_eq!(boundary_rng.get_word_pos(), ((1u128 << 32) + 4) * 16);
    assert_ne!(&boundary_words[16..32], &zero_seed_first_block);
}
