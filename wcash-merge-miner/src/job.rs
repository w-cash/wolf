//! Parent job preparation, real Equihash solving, and strict share finalization.

use equihash::tromp::solve_200_9;
use sha2::{Digest, Sha256};
use wcash_zcash_aux::{block_commitments_hash, AuxPowProof, ParentHeader, Target};
use zebra_chain::{
    block::Header,
    work::{
        difficulty::U256,
        equihash::{Solution, WCASH_BLOCK_WIRE_VERSION},
    },
};

use crate::{build_parent_coinbase, coinbase::canonical_parent_coinbase, MinerError, ParentOutput};

const HEADER_INPUT_BYTES: usize = 108;
const HEADER_NONCE_BYTES: usize = 32;
const SOLUTION_COMPACT_SIZE: [u8; 3] = [0xfd, 0x40, 0x05];

/// Exact compressed byte length of a Zcash Equihash `(200, 9)` solution.
pub const EQUIHASH_SOLUTION_BYTES: usize = 1_344;

/// Non-consensus fields used to construct one local Zcash parent template.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobConfig {
    /// Zcash parent height encoded into the coinbase input.
    pub parent_height: u32,
    /// Parent header version. Current Zcash-compatible headers use version 4.
    pub parent_version: u32,
    /// Raw wire-order parent previous-block hash.
    pub previous_block_hash: [u8; 32],
    /// Raw wire-order parent chain-history root used by ZIP-244 commitments.
    pub chain_history_root: [u8; 32],
    /// Parent header timestamp.
    pub timestamp: u32,
    /// Parent header `nBits`, retained as diagnostic parent data only.
    pub advertised_n_bits: u32,
    /// Nonce committed into the auxiliary-tree carrier.
    pub auxiliary_nonce: u32,
    /// Extra data appended to the encoded parent height in the coinbase input.
    pub coinbase_extra_data: Vec<u8>,
    /// Optional synthetic parent payout outputs placed before the commitment.
    pub parent_outputs: Vec<ParentOutput>,
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            parent_height: 1,
            parent_version: 4,
            previous_block_hash: [0; 32],
            chain_history_root: [0; 32],
            timestamp: 0,
            // This field is diagnostic in Wcash validation. The local default is
            // the familiar regtest-style compact target, not a target authority.
            advertised_n_bits: 0x207f_ffff,
            auxiliary_nonce: 0,
            coinbase_extra_data: b"Wcash/ZcashAuxPoW/local/v1".to_vec(),
            parent_outputs: Vec::new(),
        }
    }
}

/// A frozen local parent job that can be sent to a solver or share submitter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedJob {
    child_block_hash: [u8; 32],
    required_target: Target,
    job_id_bytes: [u8; 32],
    job_id: String,
    coinbase_bytes: Vec<u8>,
    coinbase_transaction_id: [u8; 32],
    parent_header_input: [u8; HEADER_INPUT_BYTES],
    parent_merkle_branch: Vec<[u8; 32]>,
    auth_data_merkle_branch: Vec<[u8; 32]>,
    chain_history_root: [u8; 32],
    auxiliary_nonce: u32,
}

impl PreparedJob {
    /// Constructs a local Zcash parent job directly from a Wcash header.
    ///
    /// The child ID is Zebra's proof-independent Wcash header hash. The target
    /// is expanded from the child's authenticated compact difficulty and then
    /// converted to the little-endian numeric byte order required by the
    /// AuxPoW verifier.
    pub fn from_wcash_header(header: &Header, config: JobConfig) -> Result<Self, MinerError> {
        if header.version != WCASH_BLOCK_WIRE_VERSION || header.solution.as_wcash().is_none() {
            return Err(MinerError::NotWcashHeader {
                version: header.version,
            });
        }

        let expanded_target = header
            .difficulty_threshold
            .to_expanded()
            .ok_or(MinerError::InvalidChildDifficulty)?;
        let expanded_target: U256 = expanded_target.into();
        let required_target = Target::from_le_bytes(expanded_target.to_little_endian())?;

        Self::new(header.hash().0, required_target, config)
    }

    /// Constructs a deterministic one-transaction parent block job.
    pub fn new(
        child_block_hash: [u8; 32],
        required_target: Target,
        config: JobConfig,
    ) -> Result<Self, MinerError> {
        if config.parent_version < 4 || config.parent_version >> 31 != 0 {
            return Err(MinerError::InvalidParentVersion(config.parent_version));
        }

        let coinbase_bytes = build_parent_coinbase(
            config.parent_height,
            &config.coinbase_extra_data,
            &config.parent_outputs,
            child_block_hash,
            config.auxiliary_nonce,
        )?;
        let coinbase = canonical_parent_coinbase(&coinbase_bytes)?;

        let mut parent_header_input = [0; HEADER_INPUT_BYTES];
        parent_header_input[..4].copy_from_slice(&config.parent_version.to_le_bytes());
        parent_header_input[4..36].copy_from_slice(&config.previous_block_hash);
        // A one-transaction Merkle tree has the coinbase txid as its root.
        parent_header_input[36..68].copy_from_slice(&coinbase.transaction_id);
        let block_commitments =
            block_commitments_hash(config.chain_history_root, coinbase.authorizing_data_digest);
        parent_header_input[68..100].copy_from_slice(&block_commitments);
        parent_header_input[100..104].copy_from_slice(&config.timestamp.to_le_bytes());
        parent_header_input[104..108].copy_from_slice(&config.advertised_n_bits.to_le_bytes());

        let job_id_bytes = job_id(
            child_block_hash,
            required_target,
            &parent_header_input,
            &coinbase_bytes,
            &[],
            &[],
            config.chain_history_root,
        );
        let job_id = hex::encode(job_id_bytes);

        Ok(Self {
            child_block_hash,
            required_target,
            job_id_bytes,
            job_id,
            coinbase_bytes,
            coinbase_transaction_id: coinbase.transaction_id,
            parent_header_input,
            parent_merkle_branch: Vec::new(),
            auth_data_merkle_branch: Vec::new(),
            chain_history_root: config.chain_history_root,
            auxiliary_nonce: config.auxiliary_nonce,
        })
    }

    /// Constructs a frozen job from a proposal-validated native Zcash template.
    ///
    /// This constructor is deliberately crate-private: callers must use
    /// [`crate::native::NativeZcashProvider`], which checks the complete parent
    /// block and all roots before providing these components.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_live_template(
        child_block_hash: [u8; 32],
        required_target: Target,
        coinbase_bytes: Vec<u8>,
        parent_header_input: [u8; HEADER_INPUT_BYTES],
        parent_merkle_branch: Vec<[u8; 32]>,
        auth_data_merkle_branch: Vec<[u8; 32]>,
        chain_history_root: [u8; 32],
        auxiliary_nonce: u32,
    ) -> Result<Self, MinerError> {
        if parent_merkle_branch.len() != auth_data_merkle_branch.len() {
            return Err(MinerError::InvalidParentTemplate(
                "transaction and auth-data branches have different depths".to_string(),
            ));
        }
        let coinbase = canonical_parent_coinbase(&coinbase_bytes)?;
        let job_id_bytes = job_id(
            child_block_hash,
            required_target,
            &parent_header_input,
            &coinbase_bytes,
            &parent_merkle_branch,
            &auth_data_merkle_branch,
            chain_history_root,
        );
        let job_id = hex::encode(job_id_bytes);

        Ok(Self {
            child_block_hash,
            required_target,
            job_id_bytes,
            job_id,
            coinbase_bytes,
            coinbase_transaction_id: coinbase.transaction_id,
            parent_header_input,
            parent_merkle_branch,
            auth_data_merkle_branch,
            chain_history_root,
            auxiliary_nonce,
        })
    }

    /// Returns the stable identifier for this exact local job.
    pub fn job_id(&self) -> &str {
        &self.job_id
    }

    /// Returns the stable identifier as the exact 32 bytes hashed for this job.
    pub const fn job_id_bytes(&self) -> [u8; 32] {
        self.job_id_bytes
    }

    /// Returns the exact Wcash child block ID bytes committed by this job.
    pub const fn child_block_hash(&self) -> [u8; 32] {
        self.child_block_hash
    }

    /// Returns the authenticated Wcash target for the parent work.
    pub const fn required_target(&self) -> Target {
        self.required_target
    }

    /// Returns the exact canonical parent coinbase bytes.
    pub fn coinbase_bytes(&self) -> &[u8] {
        &self.coinbase_bytes
    }

    /// Returns the raw-byte-order parent coinbase transaction ID and Merkle root.
    pub const fn coinbase_transaction_id(&self) -> [u8; 32] {
        self.coinbase_transaction_id
    }

    /// Returns the 108 parent-header bytes hashed before the separate nonce.
    pub const fn parent_header_input(&self) -> &[u8; HEADER_INPUT_BYTES] {
        &self.parent_header_input
    }

    /// Returns the nonce in the AuxPoW commitment carrier.
    pub const fn auxiliary_nonce(&self) -> u32 {
        self.auxiliary_nonce
    }

    /// Builds and structurally checks the exact parent header submitted by a solver.
    pub fn parent_header(&self, nonce: &[u8], solution: &[u8]) -> Result<ParentHeader, MinerError> {
        if nonce.len() != HEADER_NONCE_BYTES {
            return Err(MinerError::InvalidNonceLength(nonce.len()));
        }
        if solution.len() != EQUIHASH_SOLUTION_BYTES {
            return Err(MinerError::InvalidSolutionLength(solution.len()));
        }

        let mut header_bytes = Vec::with_capacity(
            HEADER_INPUT_BYTES
                + HEADER_NONCE_BYTES
                + SOLUTION_COMPACT_SIZE.len()
                + EQUIHASH_SOLUTION_BYTES,
        );
        header_bytes.extend_from_slice(&self.parent_header_input);
        header_bytes.extend_from_slice(nonce);
        header_bytes.extend_from_slice(&SOLUTION_COMPACT_SIZE);
        header_bytes.extend_from_slice(solution);
        Ok(ParentHeader::decode(&header_bytes)?)
    }

    /// Validates submitted `(200,9)` work and constructs the canonical proof.
    ///
    /// Validation uses `AuxPowProof::validate`, including Zebra coinbase parsing,
    /// both Merkle bindings, the authenticated Wcash target, and real Equihash.
    pub fn finalize(&self, nonce: &[u8], solution: &[u8]) -> Result<SolvedAuxPow, MinerError> {
        let parent_header = self.parent_header(nonce, solution)?;

        let proof = AuxPowProof::new(
            self.coinbase_bytes.clone(),
            self.parent_merkle_branch.clone(),
            0,
            self.auth_data_merkle_branch.clone(),
            0,
            self.chain_history_root,
            Vec::<[u8; 32]>::new(),
            0,
            parent_header,
        )?;
        let validated = proof.validate(self.child_block_hash, self.required_target)?;
        let parent_block_hash_le = validated.parent_work().block_hash().into_le_bytes();
        let encoded_proof = proof.encode()?;

        let mut nonce_bytes = [0; HEADER_NONCE_BYTES];
        nonce_bytes.copy_from_slice(nonce);
        let mut solution_bytes = Box::new([0; EQUIHASH_SOLUTION_BYTES]);
        solution_bytes.copy_from_slice(solution);

        Ok(SolvedAuxPow {
            proof,
            encoded_proof,
            parent_block_hash_le,
            nonce: nonce_bytes,
            solution: solution_bytes,
        })
    }

    /// Attaches a fully verified proof to the exact Wcash header used to make
    /// this job.
    ///
    /// The proof is validated again at this boundary. This prevents a caller
    /// from mixing a solved parent from another job into the child template.
    pub fn attach_to_header(
        &self,
        mut header: Header,
        solved: &SolvedAuxPow,
    ) -> Result<Header, MinerError> {
        if header.version != WCASH_BLOCK_WIRE_VERSION || header.solution.as_wcash().is_none() {
            return Err(MinerError::NotWcashHeader {
                version: header.version,
            });
        }
        if header.hash().0 != self.child_block_hash {
            return Err(MinerError::ChildHeaderMismatch);
        }

        solved
            .proof
            .validate(self.child_block_hash, self.required_target)?;
        header.solution = Solution::for_wcash(solved.encoded_proof.clone())
            .map_err(MinerError::WcashWitnessEncoding)?;

        Ok(header)
    }

    /// Searches a bounded sequential nonce range with the real Tromp `(200,9)` solver.
    ///
    /// One run is memory- and CPU-intensive by design. `max_nonce_runs` bounds
    /// the work and makes exhaustion explicit; no verification rule is bypassed.
    pub fn solve(&self, start_nonce: u64, max_nonce_runs: u64) -> Result<SolvedAuxPow, MinerError> {
        self.solve_with_cancel(start_nonce, max_nonce_runs, || false)
    }

    /// Searches a bounded sequential nonce range and polls `is_cancelled`
    /// before every memory-hard Equihash run.
    ///
    /// Cancellation cannot interrupt a single Tromp solver run, but it is
    /// observed before another nonce consumes CPU and memory.
    pub fn solve_with_cancel<F>(
        &self,
        start_nonce: u64,
        max_nonce_runs: u64,
        mut is_cancelled: F,
    ) -> Result<SolvedAuxPow, MinerError>
    where
        F: FnMut() -> bool,
    {
        let end_nonce = start_nonce
            .checked_add(max_nonce_runs)
            .ok_or(MinerError::NonceRangeOverflow)?;
        let mut nonce_values = start_nonce..end_nonce;
        let mut attempted = 0u64;
        let mut cancelled = false;

        while attempted < max_nonce_runs {
            let mut solution_nonce = None;
            let solutions = solve_200_9(&self.parent_header_input, || {
                if is_cancelled() {
                    cancelled = true;
                    return None;
                }
                let value = nonce_values.next()?;
                let nonce = nonce_from_u64(value);
                solution_nonce = Some(nonce);
                attempted = attempted.saturating_add(1);
                Some(nonce)
            });

            if cancelled || is_cancelled() {
                return Err(MinerError::SolverCancelled { attempted });
            }
            if solutions.is_empty() {
                break;
            }
            let nonce = solution_nonce.expect("the solver returns solutions only after a nonce");
            for solution in solutions {
                match self.finalize(&nonce, &solution) {
                    Ok(solved) => return Ok(solved),
                    Err(MinerError::AuxPow(
                        wcash_zcash_aux::AuxPowError::InsufficientParentWork { .. },
                    )) => continue,
                    Err(error) => return Err(error),
                }
            }
        }

        Err(MinerError::SolverExhausted { attempted })
    }
}

/// A fully verified local AuxPoW proof and its winning parent work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SolvedAuxPow {
    proof: AuxPowProof,
    encoded_proof: Vec<u8>,
    parent_block_hash_le: [u8; 32],
    nonce: [u8; HEADER_NONCE_BYTES],
    solution: Box<[u8; EQUIHASH_SOLUTION_BYTES]>,
}

impl SolvedAuxPow {
    /// Returns the decoded proof object.
    pub const fn proof(&self) -> &AuxPowProof {
        &self.proof
    }

    /// Returns the canonical bytes attached to the Wcash header.
    pub fn encoded_proof(&self) -> &[u8] {
        &self.encoded_proof
    }

    /// Returns the winning raw little-endian parent block hash.
    pub const fn parent_block_hash_le(&self) -> [u8; 32] {
        self.parent_block_hash_le
    }

    /// Returns the winning 32-byte nonce.
    pub const fn nonce(&self) -> [u8; HEADER_NONCE_BYTES] {
        self.nonce
    }

    /// Returns the winning compressed `(200,9)` solution.
    pub fn solution(&self) -> &[u8; EQUIHASH_SOLUTION_BYTES] {
        &self.solution
    }
}

fn nonce_from_u64(value: u64) -> [u8; HEADER_NONCE_BYTES] {
    let mut nonce = [0; HEADER_NONCE_BYTES];
    nonce[..8].copy_from_slice(&value.to_le_bytes());
    nonce
}

fn job_id(
    child_block_hash: [u8; 32],
    target: Target,
    header_input: &[u8; HEADER_INPUT_BYTES],
    coinbase: &[u8],
    parent_merkle_branch: &[[u8; 32]],
    auth_data_merkle_branch: &[[u8; 32]],
    chain_history_root: [u8; 32],
) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"Wcash/ZcashAuxPoW/job/v2\0");
    hash.update(child_block_hash);
    hash.update(target.to_le_bytes());
    hash.update(header_input);
    hash.update(coinbase);
    for node in parent_merkle_branch {
        hash.update(node);
    }
    for node in auth_data_merkle_branch {
        hash.update(node);
    }
    hash.update(chain_history_root);
    hash.finalize().into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(child: [u8; 32]) -> PreparedJob {
        PreparedJob::new(child, Target::MAX, JobConfig::default()).expect("valid fixture job")
    }

    #[test]
    fn one_transaction_template_and_job_id_are_stable_and_bound() {
        let first = job([1; 32]);
        let same = job([1; 32]);
        let different = job([2; 32]);

        assert_eq!(first, same);
        assert_ne!(first.job_id(), different.job_id());
        assert_eq!(hex::encode(first.job_id_bytes()), first.job_id());
        assert_eq!(
            &first.parent_header_input()[36..68],
            &first.coinbase_transaction_id()
        );
    }

    #[test]
    fn malformed_and_fake_work_never_create_a_proof() {
        let job = job([3; 32]);
        assert!(matches!(
            job.finalize(&[0; 31], &[0; EQUIHASH_SOLUTION_BYTES]),
            Err(MinerError::InvalidNonceLength(31))
        ));
        assert!(matches!(
            job.finalize(&[0; HEADER_NONCE_BYTES], &[0; 1]),
            Err(MinerError::InvalidSolutionLength(1))
        ));
        assert!(matches!(
            job.finalize(&[0; HEADER_NONCE_BYTES], &[0; EQUIHASH_SOLUTION_BYTES]),
            Err(MinerError::AuxPow(
                wcash_zcash_aux::AuxPowError::InvalidEquihash
            ))
        ));
    }

    #[test]
    fn zero_and_overflowing_solver_ranges_are_bounded() {
        let job = job([4; 32]);
        assert!(matches!(
            job.solve(0, 0),
            Err(MinerError::SolverExhausted { attempted: 0 })
        ));
        assert!(matches!(
            job.solve(u64::MAX, 1),
            Err(MinerError::NonceRangeOverflow)
        ));
        assert!(matches!(
            job.solve_with_cancel(0, 1, || true),
            Err(MinerError::SolverCancelled { attempted: 0 })
        ));
    }

    #[test]
    fn wcash_header_job_binds_the_raw_child_id_and_expanded_target() {
        let mut header = zebra_chain::block::genesis::wcash_regtest_genesis_block()
            .header
            .as_ref()
            .clone();
        let target_integer = U256::from_big_endian(&[0x0f; 32]);
        header.difficulty_threshold =
            zebra_chain::work::difficulty::ExpandedDifficulty::from(target_integer).to_compact();

        let job = PreparedJob::from_wcash_header(&header, JobConfig::default())
            .expect("valid Wcash header creates a parent job");
        let canonical_target: U256 = header
            .difficulty_threshold
            .to_expanded()
            .expect("fixture compact target is valid")
            .into();
        assert_eq!(job.child_block_hash(), header.hash().0);
        assert_eq!(
            job.required_target().to_le_bytes(),
            canonical_target.to_little_endian()
        );

        header.version = 4;
        assert!(matches!(
            PreparedJob::from_wcash_header(&header, JobConfig::default()),
            Err(MinerError::NotWcashHeader { version: 4 })
        ));
    }

    #[test]
    #[ignore = "runs the memory-hard production Equihash (200,9) solver"]
    fn real_solver_produces_a_strictly_valid_round_trip() {
        let job = job([0x5a; 32]);
        let solved = job
            .solve(0, 16)
            .expect("16 runs have an overwhelmingly likely Equihash solution");
        let decoded = AuxPowProof::decode(solved.encoded_proof()).expect("proof encoding is exact");
        decoded
            .validate(job.child_block_hash(), job.required_target())
            .expect("generated proof passes the production verifier");
    }

    #[test]
    #[ignore = "runs the memory-hard production Equihash (200,9) solver"]
    fn real_solver_attaches_a_valid_proof_to_a_wcash_header() {
        let mut header = zebra_chain::block::genesis::wcash_regtest_genesis_block()
            .header
            .as_ref()
            .clone();
        let target_integer = U256::from_big_endian(&[0x0f; 32]);
        header.difficulty_threshold =
            zebra_chain::work::difficulty::ExpandedDifficulty::from(target_integer).to_compact();
        let original_id = header.hash();
        let job = PreparedJob::from_wcash_header(&header, JobConfig::default())
            .expect("valid Wcash header creates a parent job");
        let solved = job
            .solve(0, 256)
            .expect("the local target is expected to be met in 256 nonce runs");
        let solved_header = job
            .attach_to_header(header, &solved)
            .expect("verified proof attaches to its exact child header");

        assert_eq!(solved_header.hash(), original_id);
        let witness = solved_header
            .solution
            .as_wcash()
            .expect("solved header keeps the Wcash carrier");
        let proof = AuxPowProof::decode(witness.as_bytes()).expect("attached proof is canonical");
        proof
            .validate(solved_header.hash().0, job.required_target())
            .expect("attached proof passes the production verifier");
    }
}
