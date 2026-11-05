//! Offline synthetic NU7 cryptographic vectors. Normal tests never run a solver.
//!
//! Generation is explicitly ignored and requires a task-owned output directory.
//! A real Equihash solution establishes cryptographic validity only: these
//! fixtures have no canonical parent-chain, reward, activation or full-stack
//! qualification. The Wcash network profiles remain unchanged.

use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{mpsc, Arc, Mutex},
    thread,
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};
use wcash_zcash_aux::{miner_data_commitment, validate_miner_data_commitment, AuxPowError};
use wcash_zec_parent::chain::{
    block::Block as ParentBlock,
    serialization::{ZcashDeserializeInto as ParentDeserialize, ZcashSerialize as ParentSerialize},
};
use zebra_chain::{
    block::Block,
    serialization::{ZcashDeserializeInto as ChildDeserialize, ZcashSerialize},
    work::difficulty::CompactDifficulty,
};

use super::*;

type VectorResult<T> = Result<T, String>;
const OUTPUT_DIRECTORY_ENV: &str = "WCASH_NU7_VECTOR_OUTPUT_DIR";
const GLOBAL_LIMIT: Duration = Duration::from_secs(120);
const MAX_NONCES_PER_CASE: u64 = 8;
const CHILD_BITS: u32 = 0x203f_ffff;
const CHAIN_HISTORY_ROOT: [u8; 32] = [0x22; 32];
const AUXILIARY_NONCE: u32 = 7;
const QUALIFICATION: &str = "synthetic cryptographic vector only; not canonical parent-chain, reward, network activation, or full-stack qualification";
const SOURCE_CHILD: &str = include_str!("../vectors/wcash-valid-auxpow-block-nonzero-prev.txt");

#[derive(Clone, Copy)]
struct Case {
    name: &'static str,
    version: u32,
    dual: bool,
    parent_bits: u32,
}

const CASES: [Case; 4] = [
    Case {
        name: "nu7-v5-wec-only",
        version: 5,
        dual: false,
        parent_bits: 0x1f00_ffff,
    },
    Case {
        name: "nu7-v5-dual",
        version: 5,
        dual: true,
        parent_bits: 0x207f_ffff,
    },
    Case {
        name: "nu7-v6-wec-only",
        version: 6,
        dual: false,
        parent_bits: 0x1f00_ffff,
    },
    Case {
        name: "nu7-v6-dual",
        version: 6,
        dual: true,
        parent_bits: 0x207f_ffff,
    },
];

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    version: u32,
    name: String,
    qualification: String,
    parent_coinbase_version: u32,
    parent_branch_id: u32,
    dual_target_winner: bool,
    attempted_nonces: u64,
    child_bits: u32,
    parent_bits: u32,
    child_hash_le: String,
    parent_hash_le: String,
    child_target_le: String,
    parent_target_le: String,
    proof_sha256: String,
    source_child_sha256: String,
    /// Complete synthetic child bytes, with the exact proof attached.
    child_block_hex: String,
    /// Exact synthetic parent bytes, not a contextual consensus assertion.
    parent_block_hex: String,
}

fn require(condition: bool, message: &str) -> VectorResult<()> {
    condition.then_some(()).ok_or_else(|| message.to_string())
}

fn target(bits: u32) -> VectorResult<Target> {
    let compact = CompactDifficulty::from_bytes_in_display_order(&bits.to_be_bytes())
        .map_err(|error| error.to_string())?;
    let expanded = compact.to_expanded().ok_or("invalid compact target")?;
    require(
        expanded.to_compact() == compact,
        "non-canonical compact target",
    )?;
    let expanded: U256 = expanded.into();
    Target::from_le_bytes(expanded.to_little_endian()).map_err(|error| error.to_string())
}

fn synthetic_child() -> VectorResult<Block> {
    let bytes = hex::decode(SOURCE_CHILD.trim()).map_err(|error| error.to_string())?;
    let mut child: Block = ChildDeserialize::zcash_deserialize_into(bytes.as_slice())
        .map_err(|error| error.to_string())?;
    let header = Arc::make_mut(&mut child.header);
    header.difficulty_threshold =
        CompactDifficulty::from_bytes_in_display_order(&CHILD_BITS.to_be_bytes())
            .map_err(|error| error.to_string())?;
    header.solution = Solution::for_wcash(Vec::<u8>::new()).map_err(|error| error.to_string())?;
    Ok(child)
}

fn coinbase_for_child(version: u32, child_hash: [u8; 32]) -> VectorResult<Vec<u8>> {
    let fixture = match version {
        5 => include_str!("../../../wcash-zec-parent/test-vectors/nu7-v5-coinbase.hex"),
        6 => include_str!("../../../wcash-zec-parent/test-vectors/nu7-v6-coinbase.hex"),
        _ => return Err("unsupported fixture transaction version".to_string()),
    };
    let mut bytes = hex::decode(fixture.trim()).map_err(|error| error.to_string())?;
    let original =
        wcash_zec_parent::canonical_nu7_coinbase(&bytes).map_err(|error| error.to_string())?;
    require(original.version() == version, "fixture version mismatch")?;
    let inputs = original.inputs();
    let input = inputs.first().ok_or("fixture has no coinbase input")?;
    let miner_data = input.miner_data().ok_or("fixture has no miner data")?;
    let old = miner_data
        .get(
            miner_data
                .len()
                .checked_sub(44)
                .ok_or("missing fixture carrier")?..,
        )
        .ok_or("missing fixture carrier")?;
    require(
        old[..4] == wcash_zcash_aux::MERGED_MINING_MARKER
            && old[36..40] == 1u32.to_le_bytes()
            && old[40..44] == AUXILIARY_NONCE.to_le_bytes(),
        "fixture carrier shape changed",
    )?;
    let script = input.coinbase_script().ok_or("invalid fixture script")?;
    // Both canonical v5/v6 fixtures have a 20-byte prefix, one input, a
    // 36-byte null prevout, and a one-byte coinbase-script length.
    let script_start = 20 + 1 + 36 + 1;
    require(
        bytes.get(script_start - 1).copied() == u8::try_from(script.len()).ok(),
        "fixture script encoding changed",
    )?;
    let end = script_start + script.len();
    require(
        bytes.get(script_start..end) == Some(script.as_slice()),
        "fixture script differs from parsed bytes",
    )?;
    let carrier = miner_data_commitment(child_hash, &[], 0, AUXILIARY_NONCE)
        .map_err(|error| error.to_string())?;
    let start = end
        .checked_sub(carrier.len())
        .ok_or("missing fixture carrier")?;
    bytes[start..end].copy_from_slice(&carrier);
    let updated =
        wcash_zec_parent::canonical_nu7_coinbase(&bytes).map_err(|error| error.to_string())?;
    require(
        updated.hash() == original.hash(),
        "carrier changed ZIP-244 transaction ID",
    )?;
    require(
        u32::from(updated.consensus_branch_id()) == wcash_zec_parent::NU7_BRANCH_ID,
        "fixture did not retain the actual NU7 domain",
    )?;
    validate_miner_data_commitment(
        updated.inputs()[0]
            .miner_data()
            .ok_or("missing updated miner data")?,
        child_hash,
        &[],
        0,
    )
    .map_err(|error| error.to_string())?;
    Ok(bytes)
}

fn prepared(case: Case, child: &Block) -> VectorResult<PreparedJob> {
    let coinbase = coinbase_for_child(case.version, child.hash().0)?;
    let parsed =
        wcash_zec_parent::canonical_nu7_coinbase(&coinbase).map_err(|error| error.to_string())?;
    let mut input = [0; HEADER_INPUT_BYTES];
    input[..4].copy_from_slice(&4u32.to_le_bytes());
    input[4..36].copy_from_slice(&[0x55; 32]);
    input[36..68].copy_from_slice(&parsed.hash().0);
    input[68..100].copy_from_slice(&block_commitments_hash(
        CHAIN_HISTORY_ROOT,
        parsed.auth_digest().ok_or("missing auth digest")?.0,
    ));
    input[100..104].copy_from_slice(&1u32.to_le_bytes());
    input[104..108].copy_from_slice(&case.parent_bits.to_le_bytes());
    PreparedJob::from_live_template_with_parent_rules(
        child.hash().0,
        target(CHILD_BITS)?,
        coinbase,
        input,
        Vec::new(),
        Vec::new(),
        CHAIN_HISTORY_ROOT,
        AUXILIARY_NONCE,
        ParentCoinbaseRules::LegacyAndNu7,
    )
    .map_err(|error| error.to_string())
}

fn altered_proof(
    proof: &AuxPowProof,
    coinbase: Vec<u8>,
    root: [u8; 32],
    header: ParentHeader,
) -> VectorResult<AuxPowProof> {
    AuxPowProof::new(
        coinbase,
        proof.parent_merkle_branch().to_vec(),
        0,
        proof.auth_data_merkle_branch().to_vec(),
        0,
        root,
        proof.auxiliary_merkle_branch().to_vec(),
        proof.auxiliary_index(),
        header,
    )
    .map_err(|error| error.to_string())
}

/// Verifies stored bytes only. No proof-of-work search is reachable from here.
fn verify(proof_bytes: &[u8], metadata: &Metadata, case: Case) -> VectorResult<()> {
    require(
        metadata.version == 1 && metadata.name == case.name,
        "vector identity/version mismatch",
    )?;
    require(
        metadata.qualification == QUALIFICATION,
        "missing synthetic qualification",
    )?;
    require(
        metadata.parent_coinbase_version == case.version
            && metadata.parent_branch_id == wcash_zec_parent::NU7_BRANCH_ID,
        "transaction version/domain mismatch",
    )?;
    require(
        metadata.attempted_nonces > 0 && metadata.attempted_nonces <= MAX_NONCES_PER_CASE,
        "unbounded generation attempt count",
    )?;
    require(
        metadata.child_bits == CHILD_BITS
            && metadata.parent_bits == case.parent_bits
            && metadata.dual_target_winner == case.dual,
        "target classification metadata mismatch",
    )?;
    require(
        hex::encode(Sha256::digest(proof_bytes)) == metadata.proof_sha256,
        "proof digest mismatch",
    )?;
    require(
        hex::encode(Sha256::digest(
            hex::decode(SOURCE_CHILD.trim()).map_err(|error| error.to_string())?,
        )) == metadata.source_child_sha256,
        "source-child provenance mismatch",
    )?;
    let proof = AuxPowProof::decode(proof_bytes).map_err(|error| error.to_string())?;
    require(
        proof.encode().map_err(|error| error.to_string())? == proof_bytes,
        "non-canonical proof",
    )?;
    let child_bytes = hex::decode(&metadata.child_block_hex).map_err(|error| error.to_string())?;
    let child: Block = ChildDeserialize::zcash_deserialize_into(child_bytes.as_slice())
        .map_err(|error| error.to_string())?;
    require(
        child
            .zcash_serialize_to_vec()
            .map_err(|error| error.to_string())?
            == child_bytes,
        "non-canonical child block",
    )?;
    let baseline = synthetic_child()?;
    require(
        child.hash() == baseline.hash() && child.transactions == baseline.transactions,
        "synthetic child identity/transactions changed",
    )?;
    require(
        child
            .header
            .solution
            .as_wcash()
            .map(|witness| witness.as_bytes())
            == Some(proof_bytes),
        "child does not carry the exact proof",
    )?;
    require(
        hex::encode(child.hash().0) == metadata.child_hash_le,
        "child hash mismatch",
    )?;
    let child_target = target(CHILD_BITS)?;
    let parent_target = target(case.parent_bits)?;
    require(
        hex::encode(child_target.to_le_bytes()) == metadata.child_target_le
            && hex::encode(parent_target.to_le_bytes()) == metadata.parent_target_le,
        "target byte-order mismatch",
    )?;
    let validated = proof
        .validate_with_parent_rules(
            child.hash().0,
            child_target,
            ParentCoinbaseRules::LegacyAndNu7,
        )
        .map_err(|error| error.to_string())?;
    require(
        hex::encode(validated.parent_work().block_hash().into_le_bytes())
            == metadata.parent_hash_le,
        "parent hash mismatch",
    )?;
    require(
        matches!(
            proof.validate(child.hash().0, child_target),
            Err(AuxPowError::InvalidParentCoinbase)
                | Err(AuxPowError::UnsupportedParentCoinbaseBranch(
                    wcash_zec_parent::NU7_BRANCH_ID
                ))
        ),
        "Legacy did not reject the NU7 domain at the parser/policy boundary",
    )?;
    let coinbase = wcash_zec_parent::canonical_nu7_coinbase(proof.coinbase_bytes())
        .map_err(|error| error.to_string())?;
    require(
        coinbase.version() == case.version,
        "stored parent coinbase version mismatch",
    )?;
    require(
        proof.parent_header().advertised_n_bits() == case.parent_bits,
        "parent header bits mismatch",
    )?;
    require(
        parent_target.is_met_by_le_hash(proof.parent_header().block_hash().into_le_bytes())
            == case.dual,
        "independent parent target classification mismatch",
    )?;
    require(
        proof.parent_header().validate_work(parent_target).is_ok() == case.dual,
        "independent real parent-work check disagrees",
    )?;
    let parent_bytes =
        hex::decode(&metadata.parent_block_hex).map_err(|error| error.to_string())?;
    let mut exact_parent_bytes = proof.parent_header().as_bytes().to_vec();
    exact_parent_bytes.push(1);
    exact_parent_bytes.extend_from_slice(proof.coinbase_bytes());
    require(
        parent_bytes == exact_parent_bytes,
        "parent block bytes differ from the exact proof",
    )?;
    let parent: ParentBlock = ParentDeserialize::zcash_deserialize_into(parent_bytes.as_slice())
        .map_err(|error| error.to_string())?;
    require(
        ParentSerialize::zcash_serialize_to_vec(&parent).map_err(|error| error.to_string())?
            == parent_bytes,
        "non-canonical parent block",
    )?;
    require(
        &parent_bytes[..1487] == proof.parent_header().as_bytes()
            && parent.transactions.len() == 1
            && parent.transactions[0].hash() == coinbase.hash(),
        "parent block does not contain the exact header/coinbase",
    )?;
    parent
        .header
        .solution
        .check(&parent.header)
        .map_err(|error| error.to_string())?;
    let mut wrong_child = child.hash().0;
    wrong_child[0] ^= 1;
    require(
        proof
            .validate_with_parent_rules(
                wrong_child,
                child_target,
                ParentCoinbaseRules::LegacyAndNu7,
            )
            .is_err(),
        "changed child binding accepted",
    )?;
    let mut bad_carrier = proof.coinbase_bytes().to_vec();
    let marker = bad_carrier
        .windows(4)
        .position(|window| window == [0xfa, 0xbe, 0x6d, 0x6d])
        .ok_or("missing carrier marker")?;
    bad_carrier[marker + 4] ^= 1;
    let changed = altered_proof(
        &proof,
        bad_carrier,
        proof.chain_history_root(),
        proof.parent_header().clone(),
    )?;
    require(
        changed
            .validate_with_parent_rules(
                child.hash().0,
                child_target,
                ParentCoinbaseRules::LegacyAndNu7,
            )
            .is_err(),
        "changed carrier accepted",
    )?;
    let mut wrong_root = proof.chain_history_root();
    wrong_root[0] ^= 1;
    let changed = altered_proof(
        &proof,
        proof.coinbase_bytes().to_vec(),
        wrong_root,
        proof.parent_header().clone(),
    )?;
    require(
        changed
            .validate_with_parent_rules(
                child.hash().0,
                child_target,
                ParentCoinbaseRules::LegacyAndNu7,
            )
            .is_err(),
        "changed authenticated root accepted",
    )?;
    let mut wrong_header = *proof.parent_header().as_bytes();
    wrong_header[4] ^= 1;
    let changed_header = ParentHeader::decode(&wrong_header).map_err(|error| error.to_string())?;
    require(
        changed_header.validate_work(Target::MAX).is_err(),
        "changed header retained a valid Equihash solution",
    )?;
    Ok(())
}

fn write_new(path: &Path, bytes: &[u8]) -> VectorResult<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(|error| {
            format!(
                "refusing to overwrite or create {}: {error}",
                path.display()
            )
        })?;
    file.write_all(bytes).map_err(|error| error.to_string())?;
    file.sync_all().map_err(|error| error.to_string())
}

/// The manual generator has a hard watchdog because cancellation cannot
/// interrupt a single memory-hard Tromp run. Run this one ignored test alone.
struct Deadline {
    started: Instant,
    active_case: Arc<Mutex<&'static str>>,
    stop: Option<mpsc::Sender<()>>,
    watcher: Option<thread::JoinHandle<()>>,
}

impl Deadline {
    fn new(directory: PathBuf) -> Self {
        let started = Instant::now();
        let active_case = Arc::new(Mutex::new("initialization"));
        let watched_case = active_case.clone();
        let (stop, stopped) = mpsc::channel();
        let watcher = thread::spawn(move || {
            if matches!(
                stopped.recv_timeout(GLOBAL_LIMIT),
                Err(mpsc::RecvTimeoutError::Timeout)
            ) {
                let case = watched_case.lock().map(|case| *case).unwrap_or("unknown");
                let failure = serde_json::json!({"case":case,"failure":"global 120-second wall-clock limit","qualification":QUALIFICATION});
                let _ = write_new(
                    &directory.join("generator-failure.json"),
                    failure.to_string().as_bytes(),
                );
                std::process::exit(124);
            }
        });
        Self {
            started,
            active_case,
            stop: Some(stop),
            watcher: Some(watcher),
        }
    }
}

impl Drop for Deadline {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(watcher) = self.watcher.take() {
            let _ = watcher.join();
        }
    }
}

#[test]
#[ignore = "manual offline Tromp generation; requires WCASH_NU7_VECTOR_OUTPUT_DIR; run alone"]
fn generate_nu7_cryptographic_vectors_offline() -> VectorResult<()> {
    let directory = std::env::var_os(OUTPUT_DIRECTORY_ENV)
        .map(PathBuf::from)
        .ok_or("WCASH_NU7_VECTOR_OUTPUT_DIR must name a fresh task-owned directory")?;
    require(directory.is_absolute(), "output directory must be absolute")?;
    let info = fs::symlink_metadata(&directory).map_err(|error| error.to_string())?;
    require(
        info.is_dir() && !info.file_type().is_symlink(),
        "output must be an existing real directory",
    )?;
    require(
        fs::read_dir(&directory)
            .map_err(|error| error.to_string())?
            .next()
            .is_none(),
        "output directory must be empty; existing evidence is never overwritten",
    )?;
    let deadline = Deadline::new(directory.clone());
    let child = synthetic_child()?;
    for case in CASES {
        *deadline
            .active_case
            .lock()
            .map_err(|_| "generator case lock poisoned")? = case.name;
        let result = (|| -> VectorResult<()> {
            let job = prepared(case, &child)?;
            let mut winner = None;
            let mut attempts = 0;
            for nonce in 0..MAX_NONCES_PER_CASE {
                require(
                    deadline.started.elapsed() < GLOBAL_LIMIT,
                    "global generation deadline reached",
                )?;
                attempts += 1;
                match job.solve_with_cancel(nonce, 1, || deadline.started.elapsed() >= GLOBAL_LIMIT)
                {
                    Ok(solved)
                        if target(case.parent_bits)?
                            .is_met_by_le_hash(solved.parent_block_hash_le())
                            == case.dual =>
                    {
                        winner = Some(solved);
                        break;
                    }
                    Ok(_) | Err(MinerError::SolverExhausted { .. }) => {}
                    Err(error) => {
                        return Err(format!(
                            "case {} failed after {attempts} nonce(s): {error}",
                            case.name
                        ))
                    }
                }
            }
            let solved = winner.ok_or_else(|| {
                format!("case {} exhausted {attempts} bounded nonce(s)", case.name)
            })?;
            require(
                deadline.started.elapsed() < GLOBAL_LIMIT,
                "global generation deadline reached",
            )?;
            let mut completed = child.clone();
            completed.header = Arc::new(
                job.attach_to_header(child.header.as_ref().clone(), &solved)
                    .map_err(|error| error.to_string())?,
            );
            let mut parent_bytes = solved.proof().parent_header().as_bytes().to_vec();
            parent_bytes.push(1);
            parent_bytes.extend_from_slice(solved.proof().coinbase_bytes());
            let metadata = Metadata {
                version: 1,
                name: case.name.to_string(),
                qualification: QUALIFICATION.to_string(),
                parent_coinbase_version: case.version,
                parent_branch_id: wcash_zec_parent::NU7_BRANCH_ID,
                dual_target_winner: case.dual,
                attempted_nonces: attempts,
                child_bits: CHILD_BITS,
                parent_bits: case.parent_bits,
                child_hash_le: hex::encode(child.hash().0),
                parent_hash_le: hex::encode(solved.parent_block_hash_le()),
                child_target_le: hex::encode(target(CHILD_BITS)?.to_le_bytes()),
                parent_target_le: hex::encode(target(case.parent_bits)?.to_le_bytes()),
                proof_sha256: hex::encode(Sha256::digest(solved.encoded_proof())),
                source_child_sha256: hex::encode(Sha256::digest(
                    hex::decode(SOURCE_CHILD.trim()).map_err(|error| error.to_string())?,
                )),
                child_block_hex: hex::encode(
                    completed
                        .zcash_serialize_to_vec()
                        .map_err(|error| error.to_string())?,
                ),
                parent_block_hex: hex::encode(parent_bytes),
            };
            verify(solved.encoded_proof(), &metadata, case)?;
            write_new(
                &directory.join(format!("{}.proof.hex", case.name)),
                format!("{}\n", hex::encode(solved.encoded_proof())).as_bytes(),
            )?;
            write_new(
                &directory.join(format!("{}.json", case.name)),
                &serde_json::to_vec_pretty(&metadata).map_err(|error| error.to_string())?,
            )
        })();
        if let Err(error) = result {
            let failure =
                serde_json::json!({"case":case.name,"failure":error,"qualification":QUALIFICATION});
            write_new(
                &directory.join("generator-failure.json"),
                &serde_json::to_vec_pretty(&failure).map_err(|error| error.to_string())?,
            )?;
            return Err(error);
        }
    }
    write_new(&directory.join("manifest.json"), &serde_json::to_vec_pretty(&serde_json::json!({"complete":true,"cases":CASES.map(|case| case.name),"max_nonces_per_case":MAX_NONCES_PER_CASE,"global_wall_clock_seconds":120,"qualification":QUALIFICATION})).map_err(|error| error.to_string())?)
}

#[test]
fn stored_nu7_cryptographic_vectors_verify_without_solving() -> VectorResult<()> {
    let directory =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../wcash-zcash-aux/test-vectors/nu7");
    let manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(directory.join("manifest.json")).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    require(
        manifest["complete"] == true,
        "stored vector set is incomplete",
    )?;
    require(
        manifest["cases"] == serde_json::json!(CASES.map(|case| case.name)),
        "stored vector cases changed",
    )?;
    for case in CASES {
        let encoded = fs::read_to_string(directory.join(format!("{}.proof.hex", case.name)))
            .map_err(|error| error.to_string())?;
        let proof = hex::decode(encoded.trim()).map_err(|error| error.to_string())?;
        let metadata: Metadata = serde_json::from_slice(
            &fs::read(directory.join(format!("{}.json", case.name)))
                .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
        verify(&proof, &metadata, case)?;
    }
    Ok(())
}
