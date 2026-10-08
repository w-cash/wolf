// Frozen journal writer from Wolf 1ecc5a4d611ad0cc4aa97e8cf1417345a4368e15.
// These function bodies are copied verbatim from wcash-wallet/src/wallet.rs;
// only visibility differs. Keep this snapshot independent of the live writer so
// recovery tests detect v2 format or request-digest incompatibilities.
use super::*;

const PAYOUT_BATCH_FORMAT_VERSION: u32 = 2;
const PAYOUT_JOURNAL_SCHEMA_VERSION: i64 = 1;

fn expected_payout_identity(
    network: WalletNetwork,
    account_id: Uuid,
    collector_payout_commitment: [u8; 32],
    synchronized: bool,
) -> PayoutWalletIdentity {
    PayoutWalletIdentity {
        protocol_version: PAYOUT_BATCH_FORMAT_VERSION,
        network,
        genesis_hash: zebra_chain::block::Hash(network.genesis_hash()).to_string(),
        branch_id: network.branch_id_hex(),
        account_id: account_id.hyphenated().to_string(),
        collector_payout_commitment: hex::encode(collector_payout_commitment),
        fund_source: PayoutFundSource::Ironwood,
        synchronized,
    }
}

pub(super) fn prepare_payout_request(
    request: &PayoutBatchRequest,
    network: WalletNetwork,
    account_id: Uuid,
    collector_payout_commitment: [u8; 32],
) -> Result<PreparedPayoutBatch, WalletServiceError> {
    let expected_identity =
        expected_payout_identity(network, account_id, collector_payout_commitment, true);
    if request.identity != expected_identity {
        return Err(WalletServiceError::InvalidRequest(
            "payout wallet identity does not match the synchronized wallet network".to_owned(),
        ));
    }
    if request.confirmations < 100 {
        return Err(WalletServiceError::UnsafeConfirmations);
    }
    if request.max_fee_zat == 0 || i64::try_from(request.max_fee_zat).is_err() {
        return Err(WalletServiceError::InvalidRequest(
            "maximum payout fee must be a positive signed 64-bit zatoshi value".to_owned(),
        ));
    }
    if request.outputs.is_empty() || request.outputs.len() > MAX_TRANSFER_RECIPIENTS {
        return Err(WalletServiceError::InvalidRequest(format!(
            "payout output count must be in 1..={MAX_TRANSFER_RECIPIENTS}"
        )));
    }
    let batch_id = canonical_uuid("batch_id", &request.batch_id)?;
    let request_commitment = canonical_hex32("request_commitment", &request.request_commitment)?;
    let mut allocation_ids = HashSet::with_capacity(request.outputs.len());
    let mut outputs = Vec::with_capacity(request.outputs.len());
    for output in &request.outputs {
        if output.receiver_kind != crate::WcashReceiverKind::Ironwood {
            return Err(WalletServiceError::InvalidRequest(
                "every payout receiver_kind must be ironwood".to_owned(),
            ));
        }
        let allocation_id = canonical_uuid("allocation_id", &output.allocation_id)?;
        if !allocation_ids.insert(allocation_id) {
            return Err(WalletServiceError::InvalidRequest(
                "allocation_id values must be unique within a payout batch".to_owned(),
            ));
        }
        let validated = crate::validate_wcash_address(&output.canonical_address, network)?;
        if validated.receiver_kind != crate::WcashReceiverKind::Ironwood
            || validated.canonical != output.canonical_address
        {
            return Err(WalletServiceError::InvalidRequest(
                "payout address must be canonical and Ironwood-capable".to_owned(),
            ));
        }
        if output.amount_zat == 0 || i64::try_from(output.amount_zat).is_err() {
            return Err(WalletServiceError::InvalidRequest(
                "payout amount must be a positive signed 64-bit zatoshi value".to_owned(),
            ));
        }
        let memo = canonical_hex("memo_hex", &output.memo_hex, 512)?;
        outputs.push(PreparedPayoutOutput {
            allocation_id,
            canonical_address: output.canonical_address.clone(),
            amount_zat: output.amount_zat,
            memo,
        });
    }
    let request_facts_digest = payout_request_facts_digest(
        batch_id,
        request_commitment,
        &expected_identity,
        request.confirmations,
        request.max_fee_zat,
        &outputs,
    );
    Ok(PreparedPayoutBatch {
        batch_id,
        request_commitment,
        request_facts_digest,
        account_uuid: account_id,
        confirmations: request.confirmations,
        max_fee_zat: request.max_fee_zat,
        outputs,
    })
}

pub(super) fn payout_request_facts_digest(
    batch_id: Uuid,
    request_commitment: [u8; 32],
    identity: &PayoutWalletIdentity,
    confirmations: u32,
    max_fee_zat: u64,
    outputs: &[PreparedPayoutOutput],
) -> [u8; 32] {
    let mut state = blake2b_simd::Params::new()
        .hash_length(32)
        .personal(b"WcashPayFactsV2!")
        .to_state();
    state.update(batch_id.as_bytes());
    state.update(&request_commitment);
    state.update(&identity.protocol_version.to_be_bytes());
    update_length_prefixed(&mut state, identity.genesis_hash.as_bytes());
    update_length_prefixed(&mut state, identity.branch_id.as_bytes());
    update_length_prefixed(&mut state, identity.account_id.as_bytes());
    update_length_prefixed(&mut state, identity.collector_payout_commitment.as_bytes());
    state.update(&confirmations.to_be_bytes());
    state.update(&max_fee_zat.to_be_bytes());
    state.update(&(outputs.len() as u32).to_be_bytes());
    for (ordinal, output) in outputs.iter().enumerate() {
        state.update(&(ordinal as u32).to_be_bytes());
        state.update(output.allocation_id.as_bytes());
        update_length_prefixed(&mut state, output.canonical_address.as_bytes());
        state.update(&output.amount_zat.to_be_bytes());
        update_length_prefixed(&mut state, &output.memo);
    }
    state
        .finalize()
        .as_bytes()
        .try_into()
        .expect("32-byte digest")
}

pub(super) fn update_length_prefixed(state: &mut blake2b_simd::State, bytes: &[u8]) {
    state.update(&(bytes.len() as u32).to_be_bytes());
    state.update(bytes);
}

pub(super) fn insert_payout_batch_intent(
    extension: &ExtensionTransaction<'_>,
    batch: &PreparedPayoutBatch,
) -> Result<(), WalletServiceError> {
    let inserted = extension.execute(
        "INSERT INTO ext_wcash_payout_batches (
            batch_id, format_version, request_commitment, request_facts_digest,
            account_uuid, confirmations, max_fee_zat
         ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            batch.batch_id.as_bytes().as_slice(),
            PAYOUT_JOURNAL_SCHEMA_VERSION,
            &batch.request_commitment[..],
            &batch.request_facts_digest[..],
            batch.account_uuid.as_bytes().as_slice(),
            i64::from(batch.confirmations),
            i64::try_from(batch.max_fee_zat).map_err(|_| WalletServiceError::InvalidRequest(
                "maximum payout fee is not representable".to_owned()
            ))?,
        ],
    )?;
    if inserted != 1 {
        return Err(WalletServiceError::Database(
            "payout batch intent was not inserted".to_owned(),
        ));
    }
    for (ordinal, output) in batch.outputs.iter().enumerate() {
        let inserted = extension.execute(
            "INSERT INTO ext_wcash_payout_outputs (
                batch_id, ordinal, allocation_id, canonical_address, amount_zat, memo
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                batch.batch_id.as_bytes().as_slice(),
                i64::try_from(ordinal).map_err(|_| WalletServiceError::InvalidRequest(
                    "payout ordinal is not representable".to_owned()
                ))?,
                output.allocation_id.as_bytes().as_slice(),
                &output.canonical_address,
                i64::try_from(output.amount_zat).map_err(|_| {
                    WalletServiceError::InvalidRequest(
                        "payout amount is not representable".to_owned(),
                    )
                })?,
                &output.memo,
            ],
        )?;
        if inserted != 1 {
            return Err(WalletServiceError::Database(
                "payout output was not inserted".to_owned(),
            ));
        }
    }
    Ok(())
}

pub(super) fn complete_payout_batch(
    extension: &ExtensionTransaction<'_>,
    batch: &PreparedPayoutBatch,
    signed: &SignedTransaction,
    txid: zcash_protocol::TxId,
    raw: &[u8],
) -> Result<(), WalletServiceError> {
    if signed.fee_zat > batch.max_fee_zat {
        return Err(WalletServiceError::PayoutFeeExceeded {
            actual_zat: signed.fee_zat,
            maximum_zat: batch.max_fee_zat,
        });
    }
    let raw_sha256: [u8; 32] = Sha256::digest(raw).into();
    let updated = extension.execute(
        "UPDATE ext_wcash_payout_batches SET
            txid = ?2, raw_transaction = ?3, raw_sha256 = ?4,
            unsigned_digest = ?5, fee_zat = ?6, target_height = ?7,
            expiry_height = ?8, internal_change_receiver_verified = 1
         WHERE batch_id = ?1 AND txid IS NULL",
        params![
            batch.batch_id.as_bytes().as_slice(),
            txid.as_ref(),
            raw,
            &raw_sha256[..],
            txid.as_ref(),
            i64::try_from(signed.fee_zat).map_err(|_| WalletServiceError::InvalidRequest(
                "payout fee is not representable".to_owned()
            ))?,
            i64::from(signed.target_height),
            i64::from(signed.expiry_height),
        ],
    )?;
    if updated != 1 {
        return Err(WalletServiceError::Database(
            "payout batch result was not atomically completed".to_owned(),
        ));
    }
    Ok(())
}
