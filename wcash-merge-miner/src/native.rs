//! Native Zcash template preparation, proposal validation, and dual submission.
//!
//! This backend talks only to operator-controlled Zebra nodes. It does not
//! proxy or reinterpret jobs from a third-party pool. Every job is assembled
//! from a genuine Zcash `getblocktemplate`, checked locally, and submitted in
//! proposal mode to every configured validation node before miners can see it.

use std::{collections::HashSet, fmt, sync::Arc, thread, time::Duration};

use hex::FromHex;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use wcash_zcash_aux::{
    parent_payout_address_commitment, sha256d_merkle_root, validate_miner_data_commitment,
    ParentBlockHash, Target, MAX_COINBASE_BYTES,
};
use zcash_address::{unified::Receiver, ZcashAddress};
use zebra_chain::{
    block::{
        self,
        merkle::{AuthDataRoot, AUTH_DIGEST_PLACEHOLDER},
        Block, ChainHistoryBlockTxAuthCommitmentHash, ChainHistoryMmrRootHash, Header,
    },
    parameters::Network,
    serialization::{BytesInDisplayOrder, DateTime32, ZcashDeserializeInto, ZcashSerialize},
    transaction::{AuthDigest, Hash as TransactionHash, Transaction},
    transparent,
    work::{
        difficulty::{CompactDifficulty, ExpandedDifficulty},
        equihash::Solution,
    },
};

use crate::{
    rpc::{RpcEndpoint, ZebraRpcClient, DEFAULT_RPC_TIMEOUT},
    MinerError, PreparedJob, SolvedAuxPow, EQUIHASH_SOLUTION_BYTES,
};

const HEADER_INPUT_BYTES: usize = 108;
const PARENT_BLOCK_LIMIT: usize = 2_000_000;

/// Strict local projection of the parent fields used to build an exact job.
///
/// Keeping this wire DTO local prevents the standalone pool from sharing Rust
/// implementation types with the node. Unknown standard GBT extension fields
/// remain forward-compatible, while every security-relevant field is required
/// and independently decoded below.
#[derive(Debug, serde::Deserialize)]
struct BlockTemplateResponse {
    version: u32,
    #[serde(rename = "previousblockhash")]
    previous_block_hash: String,
    #[serde(rename = "blockcommitmentshash")]
    block_commitments_hash: String,
    #[serde(rename = "defaultroots")]
    default_roots: TemplateRoots,
    transactions: Vec<TransactionTemplate>,
    #[serde(rename = "coinbasetxn")]
    coinbase_txn: TransactionTemplate,
    target: String,
    #[serde(rename = "curtime")]
    cur_time: u32,
    bits: String,
    height: u32,
    #[serde(rename = "wcashparentpayoutcommitment")]
    parent_payout_commitment: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct TemplateRoots {
    #[serde(rename = "merkleroot")]
    merkle_root: String,
    #[serde(rename = "chainhistoryroot")]
    chain_history_root: String,
    #[serde(rename = "authdataroot")]
    auth_data_root: String,
    #[serde(rename = "blockcommitmentshash")]
    block_commitments_hash: String,
}

#[derive(Debug, serde::Deserialize)]
struct TransactionTemplate {
    data: String,
    hash: String,
    #[serde(rename = "authdigest")]
    auth_digest: String,
}

/// Minimal standard `getblockheader(hash, true)` response used to bind
/// confirmation depth to the exact parent winner rather than sampling a
/// potentially different block at the same height.
#[derive(Debug, serde::Deserialize)]
struct ParentBlockHeaderStatus {
    hash: String,
    confirmations: i64,
    height: u32,
}

/// Configuration for the independent native-Zcash work source.
#[derive(Clone)]
pub struct NativeZcashConfig {
    /// Node that creates commitment-aware Zcash block templates.
    template_node: RpcEndpoint,
    /// Independent nodes that must all accept the exact proposal.
    proposal_validators: Vec<RpcEndpoint>,
    /// Expected parent genesis hash in conventional RPC display order.
    expected_genesis_hash: String,
    /// Domain-separated commitment to the expected parent payout address.
    expected_parent_payout_commitment: [u8; 32],
    /// Parsed expected parent payout address, omitted from diagnostics.
    expected_parent_payout_address: ZcashAddress,
    /// Deadline applied independently to each RPC request.
    rpc_timeout: Duration,
}

impl fmt::Debug for NativeZcashConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeZcashConfig")
            .field("template_node", &self.template_node)
            .field("proposal_validators", &self.proposal_validators)
            .field("expected_genesis_hash", &self.expected_genesis_hash)
            .field("expected_parent_payout_commitment", &"[REDACTED]")
            .field("expected_parent_payout_address", &"[REDACTED]")
            .field("rpc_timeout", &self.rpc_timeout)
            .finish()
    }
}

impl NativeZcashConfig {
    /// Creates a configuration that fails closed without a proposal validator.
    pub fn new(
        template_node: RpcEndpoint,
        proposal_validators: Vec<RpcEndpoint>,
        expected_genesis_hash: String,
        expected_parent_payout_address: ZcashAddress,
    ) -> Result<Self, MinerError> {
        let expected_parent_payout_commitment =
            parent_payout_address_commitment(&expected_parent_payout_address.to_string());
        let config = Self {
            template_node,
            proposal_validators,
            expected_genesis_hash,
            expected_parent_payout_commitment,
            expected_parent_payout_address,
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), MinerError> {
        if !self.template_node.is_loopback() {
            return Err(MinerError::RpcConfiguration(
                "the Zcash template node must be loopback: it chooses the parent coinbase recipient"
                    .to_string(),
            ));
        }
        if self.proposal_validators.is_empty() {
            return Err(MinerError::RpcConfiguration(
                "at least one proposal-validation node is required".to_string(),
            ));
        }
        let mut endpoints = HashSet::with_capacity(self.proposal_validators.len() + 1);
        endpoints.insert(self.template_node.label().to_string());
        for validator in &self.proposal_validators {
            if !endpoints.insert(validator.label().to_string()) {
                return Err(MinerError::RpcConfiguration(format!(
                    "template and proposal-validation endpoints must be unique; duplicate {}",
                    validator.label()
                )));
            }
        }
        validate_display_hash(&self.expected_genesis_hash, "expected Zcash genesis hash")
    }

    /// Returns the operator-pinned parent genesis hash.
    pub(crate) fn expected_genesis_hash(&self) -> &str {
        &self.expected_genesis_hash
    }
}

/// Native pool backend backed by a template node and an explicit validation quorum.
#[derive(Clone, Debug)]
pub struct NativeZcashProvider {
    template_node: ZebraRpcClient,
    proposal_validators: Vec<ZebraRpcClient>,
    expected_parent_payout_commitment: [u8; 32],
    expected_parent_payout_address: ZcashAddress,
}

impl NativeZcashProvider {
    /// Connects the configured native parent-node set.
    pub fn new(config: NativeZcashConfig) -> Result<Self, MinerError> {
        let expected_genesis_hash = config.expected_genesis_hash.clone();
        let provider = Self::connect(config)?;
        provider.require_network_identity(&expected_genesis_hash)?;
        Ok(provider)
    }

    /// Builds RPC clients without issuing network requests. This is used by
    /// durable outbox recovery so one chain can be replayed even when the
    /// other chain is temporarily unavailable.
    pub(crate) fn connect(config: NativeZcashConfig) -> Result<Self, MinerError> {
        config.validate()?;
        let expected_parent_payout_commitment = config.expected_parent_payout_commitment;
        let expected_parent_payout_address = config.expected_parent_payout_address;
        let template_node = ZebraRpcClient::new(config.template_node, config.rpc_timeout)?;
        let proposal_validators = config
            .proposal_validators
            .into_iter()
            .map(|endpoint| ZebraRpcClient::new(endpoint, config.rpc_timeout))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            template_node,
            proposal_validators,
            expected_parent_payout_commitment,
            expected_parent_payout_address,
        })
    }

    /// Builds and independently proposal-validates one exact parent job.
    pub fn prepare_job(
        &self,
        child_block_hash: [u8; 32],
        child_target: Target,
        auxiliary_nonce: u32,
    ) -> Result<NativePreparedJob, MinerError> {
        let child_display = display_hex(child_block_hash);
        let template: BlockTemplateResponse = self.template_node.call(
            "getblocktemplate",
            json!([{
                "mode": "template",
                "capabilities": ["coinbasetxn", "proposal"],
                "wcashaux": {
                    "blockhash": child_display,
                    "nonce": auxiliary_nonce,
                }
            }]),
        )?;

        let actual_parent_payout_commitment: [u8; 32] = parse_template_hex(
            template
                .parent_payout_commitment
                .as_deref()
                .ok_or_else(|| {
                    MinerError::InvalidParentTemplate(
                        "wcashaux template omitted its parent payout commitment".to_string(),
                    )
                })?,
            "wcashparentpayoutcommitment",
        )?;
        if actual_parent_payout_commitment != self.expected_parent_payout_commitment {
            return Err(MinerError::InvalidParentTemplate(
                "parent template payout commitment differs from ZCASH_PAYOUT_ADDRESS".to_string(),
            ));
        }

        let prepared = NativePreparedJob::from_template(
            child_block_hash,
            child_target,
            auxiliary_nonce,
            template,
            &self.expected_parent_payout_address,
        )?;

        // Check the same predecessor on every node before proposal validation.
        self.require_tip(&self.template_node, &prepared)?;
        for validator in &self.proposal_validators {
            self.require_tip(validator, &prepared)?;
            let payout_template: BlockTemplateResponse = validator.call(
                "getblocktemplate",
                json!([{
                    "mode": "template",
                    "capabilities": ["coinbasetxn"],
                }]),
            )?;
            validate_independent_parent_payout_template(
                &prepared,
                &payout_template,
                &self.expected_parent_payout_address,
                validator.label(),
            )?;
            let result = validator.call_value(
                "getblocktemplate",
                json!([{
                    "mode": "proposal",
                    "data": hex::encode(prepared.proposal_bytes()),
                }]),
            )?;
            match result {
                Value::Null => {}
                Value::String(reason) => {
                    return Err(MinerError::ParentProposalRejected {
                        endpoint: validator.label().to_string(),
                        reason,
                    })
                }
                other => {
                    return Err(MinerError::RpcProtocol(format!(
                        "{} returned non-null, non-string proposal result {other}",
                        validator.label()
                    )))
                }
            }
        }

        // Close the race where a tip changed while the proposal checks ran.
        self.require_tip(&self.template_node, &prepared)?;
        for validator in &self.proposal_validators {
            self.require_tip(validator, &prepared)?;
        }

        Ok(prepared)
    }

    /// Submits a parent-target winner byte-for-byte to every configured node.
    ///
    /// Success requires at least one node to both accept the submission and
    /// report the exact block at the expected height. Every endpoint is tried,
    /// so one unavailable node does not suppress a valid parent winner.
    pub fn submit_parent(
        &self,
        job: &NativePreparedJob,
        share: &ValidatedNativeShare,
    ) -> Result<ParentSubmissionReport, MinerError> {
        let block_bytes = share.parent_block().ok_or_else(|| {
            MinerError::InvalidParentTemplate(
                "share does not meet the Zcash parent target".to_string(),
            )
        })?;
        let expected_hash = display_hex(share.parent_block_hash().into_le_bytes());
        self.submit_parent_bytes(block_bytes, job.parent_height(), &expected_hash)
    }

    /// Replays exact parent block bytes from the durable winner outbox.
    pub(crate) fn submit_parent_bytes(
        &self,
        block_bytes: &[u8],
        height: u32,
        expected_hash: &str,
    ) -> Result<ParentSubmissionReport, MinerError> {
        if block_bytes.len() > PARENT_BLOCK_LIMIT {
            return Err(MinerError::InvalidParentTemplate(
                "solved parent block exceeds the consensus size limit".to_string(),
            ));
        }
        let expected: block::Hash = parse_template_hex(expected_hash, "winner parent hash")?;
        let persisted: Block = block_bytes.zcash_deserialize_into().map_err(|error| {
            MinerError::InvalidParentTemplate(format!(
                "durable parent winner is not a canonical Zcash block: {error}"
            ))
        })?;
        if persisted.hash() != expected {
            return Err(MinerError::InvalidParentTemplate(
                "durable parent winner bytes do not match their recorded block hash".to_string(),
            ));
        }
        if persisted.coinbase_height().map(u32::from) != Some(height) {
            return Err(MinerError::InvalidParentTemplate(
                "durable parent winner height does not match its coinbase".to_string(),
            ));
        }

        let mut nodes = Vec::with_capacity(self.proposal_validators.len() + 1);
        nodes.push(&self.template_node);
        nodes.extend(self.proposal_validators.iter());

        let mut seen = HashSet::new();
        let nodes = nodes
            .into_iter()
            .filter(|node| seen.insert(node.label().to_string()))
            .collect::<Vec<_>>();
        let outcomes = thread::scope(|scope| {
            nodes
                .into_iter()
                .map(|node| {
                    scope.spawn(move || {
                        submit_parent_to_node(node, block_bytes, height, expected_hash)
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|handle| match handle.join() {
                    Ok(outcome) => outcome,
                    Err(_) => ParentNodeOutcome::Unavailable {
                        endpoint: "parent submission worker".to_string(),
                        reason: "submission worker panicked".to_string(),
                    },
                })
                .collect::<Vec<_>>()
        });

        let report = ParentSubmissionReport { outcomes };
        if !report.is_confirmed() {
            return Err(MinerError::InvalidParentTemplate(
                "no parent node confirmed the submitted block on its best chain".to_string(),
            ));
        }
        Ok(report)
    }

    /// Returns the conservative best-chain confirmation depth reported for an
    /// exact parent block. An empty result means at least one pinned node
    /// answered authoritatively that the block is not on its best chain.
    pub(crate) fn parent_confirmation_depth(
        &self,
        height: u32,
        expected_hash: &str,
    ) -> Result<Option<u32>, MinerError> {
        let mut nodes = Vec::with_capacity(self.proposal_validators.len() + 1);
        nodes.push(&self.template_node);
        nodes.extend(self.proposal_validators.iter());
        let checks = thread::scope(|scope| {
            nodes
                .into_iter()
                .map(|node| {
                    scope.spawn(move || {
                        parent_confirmation_depth_on_node(node, height, expected_hash)
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|worker| {
                    worker.join().unwrap_or_else(|_| {
                        Err(MinerError::RpcProtocol(
                            "parent confirmation worker panicked".to_string(),
                        ))
                    })
                })
                .collect::<Vec<_>>()
        });
        conservative_parent_confirmation_depth(checks)
    }

    /// Requires every configured parent node to remain on the exact job tip.
    pub fn assert_current(&self, job: &NativePreparedJob) -> Result<(), MinerError> {
        let mut nodes = Vec::with_capacity(self.proposal_validators.len() + 1);
        nodes.push(&self.template_node);
        nodes.extend(self.proposal_validators.iter());
        let checks = thread::scope(|scope| {
            nodes
                .into_iter()
                .map(|node| scope.spawn(move || self.require_tip(node, job)))
                .collect::<Vec<_>>()
                .into_iter()
                .map(|handle| {
                    handle.join().unwrap_or_else(|_| {
                        Err(MinerError::InvalidParentTemplate(
                            "parent tip-check worker panicked".to_string(),
                        ))
                    })
                })
                .collect::<Vec<_>>()
        });
        checks.into_iter().collect()
    }

    fn require_tip(
        &self,
        node: &ZebraRpcClient,
        job: &NativePreparedJob,
    ) -> Result<(), MinerError> {
        let tip_height = job.parent_height().checked_sub(1).ok_or_else(|| {
            MinerError::InvalidParentTemplate("parent height must be positive".to_string())
        })?;
        let actual_height: u32 = node.call("getblockcount", json!([]))?;
        let actual: String = node.call("getbestblockhash", json!([]))?;
        if actual_height != tip_height || !actual.eq_ignore_ascii_case(job.parent_tip_display()) {
            return Err(MinerError::ParentTipMismatch {
                expected: format!("{} at height {tip_height}", job.parent_tip_display()),
                endpoint: node.label().to_string(),
                actual: format!("{actual} at height {actual_height}"),
            });
        }
        Ok(())
    }

    pub(crate) fn require_network_identity(
        &self,
        expected_genesis_hash: &str,
    ) -> Result<(), MinerError> {
        self.require_node_genesis(&self.template_node, expected_genesis_hash)?;
        for validator in &self.proposal_validators {
            self.require_node_genesis(validator, expected_genesis_hash)?;
        }
        Ok(())
    }

    fn require_node_genesis(
        &self,
        node: &ZebraRpcClient,
        expected_genesis_hash: &str,
    ) -> Result<(), MinerError> {
        let actual: String = node.call("getblockhash", json!([0]))?;
        if !actual.eq_ignore_ascii_case(expected_genesis_hash) {
            return Err(MinerError::NetworkIdentityMismatch {
                expected: expected_genesis_hash.to_string(),
                endpoint: node.label().to_string(),
                actual,
            });
        }
        Ok(())
    }
}

/// One frozen native Zcash template that passed local and proposal checks.
#[derive(Clone, Debug)]
pub struct NativePreparedJob {
    job: PreparedJob,
    parent_proposal: Block,
    proposal_bytes: Vec<u8>,
    parent_target: Target,
    parent_tip_display: String,
    parent_height: u32,
}

impl NativePreparedJob {
    fn from_template(
        child_block_hash: [u8; 32],
        child_target: Target,
        auxiliary_nonce: u32,
        template: BlockTemplateResponse,
        expected_parent_payout_address: &ZcashAddress,
    ) -> Result<Self, MinerError> {
        if template.version != 4 {
            return Err(MinerError::InvalidParentTemplate(format!(
                "unsupported parent header version {}",
                template.version
            )));
        }
        if template.height == 0 {
            return Err(MinerError::InvalidParentTemplate(
                "parent template height is zero".to_string(),
            ));
        }

        let previous_block_hash: block::Hash =
            parse_template_hex(&template.previous_block_hash, "previousblockhash")?;
        let declared_block_commitments: ChainHistoryBlockTxAuthCommitmentHash =
            parse_template_hex(&template.block_commitments_hash, "blockcommitmentshash")?;
        let declared_merkle: block::merkle::Root = parse_template_hex(
            &template.default_roots.merkle_root,
            "defaultroots.merkleroot",
        )?;
        let chain_history_root: ChainHistoryMmrRootHash = parse_template_hex(
            &template.default_roots.chain_history_root,
            "defaultroots.chainhistoryroot",
        )?;
        let declared_auth: AuthDataRoot = parse_template_hex(
            &template.default_roots.auth_data_root,
            "defaultroots.authdataroot",
        )?;
        let declared_default_commitments: ChainHistoryBlockTxAuthCommitmentHash =
            parse_template_hex(
                &template.default_roots.block_commitments_hash,
                "defaultroots.blockcommitmentshash",
            )?;
        let bits: CompactDifficulty = parse_template_hex(&template.bits, "bits")?;
        let expanded_target: ExpandedDifficulty = parse_template_hex(&template.target, "target")?;
        if bits.to_expanded() != Some(expanded_target) {
            return Err(MinerError::InvalidParentTemplate(
                "compact bits and expanded target describe different difficulties".to_string(),
            ));
        }

        let mut transactions = Vec::with_capacity(template.transactions.len() + 1);
        let coinbase = decode_template_coinbase(&template.coinbase_txn)?;
        let coinbase_bytes = coinbase.zcash_serialize_to_vec()?;
        let recovered_parent_payout =
            zebra_chain::primitives::zcash_note_encryption::publicly_recoverable_coinbase_value_to(
                &coinbase,
                expected_parent_payout_address,
            )
            .ok_or_else(|| {
                MinerError::InvalidParentTemplate(
                    "parent coinbase payout outputs do not match the configured address"
                        .to_string(),
                )
            })?;
        if recovered_parent_payout == 0 {
            return Err(MinerError::InvalidParentTemplate(
                "parent coinbase does not pay the configured ZCASH_PAYOUT_ADDRESS".to_string(),
            ));
        }
        let coinbase_inputs = coinbase.inputs();
        let miner_data = coinbase_inputs
            .first()
            .and_then(|input| input.miner_data())
            .ok_or_else(|| {
                MinerError::InvalidParentTemplate("coinbase miner data is missing".to_string())
            })?;
        let commitment = validate_miner_data_commitment(miner_data, child_block_hash, &[], 0)?;
        if commitment.nonce() != auxiliary_nonce {
            return Err(MinerError::InvalidParentTemplate(format!(
                "coinbase commits auxiliary nonce {}, requested {auxiliary_nonce}",
                commitment.nonce()
            )));
        }
        transactions.push(Arc::new(coinbase));

        for (index, tx_template) in template.transactions.iter().enumerate() {
            let transaction_bytes = decode_template_bytes(
                &tx_template.data,
                "transactions[].data",
                PARENT_BLOCK_LIMIT,
            )?;
            let transaction: Transaction = transaction_bytes
                .as_slice()
                .zcash_deserialize_into()
                .map_err(|error| {
                    MinerError::InvalidParentTemplate(format!(
                        "invalid transaction {index}: {error}"
                    ))
                })?;
            let declared_hash: TransactionHash =
                parse_template_hex(&tx_template.hash, "transactions[].hash")?;
            if transaction.hash() != declared_hash {
                return Err(MinerError::InvalidParentTemplate(format!(
                    "transaction {index} txid mismatch"
                )));
            }
            let declared_auth: AuthDigest =
                parse_template_hex(&tx_template.auth_digest, "transactions[].authdigest")?;
            if transaction.auth_digest().unwrap_or(AUTH_DIGEST_PLACEHOLDER) != declared_auth {
                return Err(MinerError::InvalidParentTemplate(format!(
                    "transaction {index} auth digest mismatch"
                )));
            }
            transactions.push(Arc::new(transaction));
        }

        let computed_merkle: block::merkle::Root = transactions.iter().collect();
        if computed_merkle != declared_merkle {
            return Err(MinerError::InvalidParentTemplate(
                "computed transaction Merkle root differs from defaultroots".to_string(),
            ));
        }
        let computed_auth: AuthDataRoot = transactions.iter().collect();
        if computed_auth != declared_auth {
            return Err(MinerError::InvalidParentTemplate(
                "computed authorization-data root differs from defaultroots".to_string(),
            ));
        }
        let computed_commitments = ChainHistoryBlockTxAuthCommitmentHash::from_commitments(
            &chain_history_root,
            &computed_auth,
        );
        if computed_commitments != declared_default_commitments
            || computed_commitments != declared_block_commitments
        {
            return Err(MinerError::InvalidParentTemplate(
                "computed hashBlockCommitments differs from the template header".to_string(),
            ));
        }

        let header = Header {
            version: template.version,
            previous_block_hash,
            merkle_root: computed_merkle,
            commitment_bytes: <[u8; 32]>::from(computed_commitments).into(),
            time: DateTime32::from(template.cur_time).into(),
            difficulty_threshold: bits,
            nonce: [0; 32].into(),
            solution: Solution::Common([0; EQUIHASH_SOLUTION_BYTES]),
        };
        let parent_proposal = Block {
            header: Arc::new(header),
            transactions,
        };
        let proposal_bytes = parent_proposal.zcash_serialize_to_vec()?;
        if proposal_bytes.len() > PARENT_BLOCK_LIMIT {
            return Err(MinerError::InvalidParentTemplate(format!(
                "proposal is {} bytes, maximum is {PARENT_BLOCK_LIMIT}",
                proposal_bytes.len()
            )));
        }

        let header_bytes = parent_proposal.header.zcash_serialize_to_vec()?;
        let parent_header_input: [u8; HEADER_INPUT_BYTES] = header_bytes
            .get(..HEADER_INPUT_BYTES)
            .ok_or_else(|| {
                MinerError::InvalidParentTemplate("serialized header is truncated".to_string())
            })?
            .try_into()
            .map_err(|_| {
                MinerError::InvalidParentTemplate("serialized header is truncated".to_string())
            })?;

        let txids = parent_proposal
            .transactions
            .iter()
            .map(|transaction| transaction.hash().0)
            .collect::<Vec<_>>();
        let auth_digests = parent_proposal
            .transactions
            .iter()
            .map(|transaction| {
                transaction
                    .auth_digest()
                    .unwrap_or(AUTH_DIGEST_PLACEHOLDER)
                    .0
            })
            .collect::<Vec<_>>();
        let parent_merkle_branch = sha256d_branch(&txids)?;
        if sha256d_merkle_root(txids[0], &parent_merkle_branch, 0)? != computed_merkle.0 {
            return Err(MinerError::InvalidParentTemplate(
                "generated coinbase transaction branch is inconsistent".to_string(),
            ));
        }
        let auth_data_branch = auth_data_branch(&auth_digests)?;

        let chain_history_root: [u8; 32] = chain_history_root.into();
        let job = PreparedJob::from_live_template(
            child_block_hash,
            child_target,
            coinbase_bytes,
            parent_header_input,
            parent_merkle_branch,
            auth_data_branch,
            chain_history_root,
            auxiliary_nonce,
        )?;

        let mut parent_target = expanded_target.bytes_in_display_order();
        parent_target.reverse();
        let parent_target = Target::from_le_bytes(parent_target)?;
        let parent_tip_display = template.previous_block_hash;
        let parent_height = template.height;

        Ok(Self {
            job,
            parent_proposal,
            proposal_bytes,
            parent_target,
            parent_tip_display,
            parent_height,
        })
    }

    /// Returns the solver-facing job shared by the local harness and ZIP-301 frontend.
    pub const fn job(&self) -> &PreparedJob {
        &self.job
    }

    /// Returns the exact proposal bytes accepted before this job was issued.
    pub fn proposal_bytes(&self) -> &[u8] {
        &self.proposal_bytes
    }

    /// Returns the Zcash network target authenticated by the parent template.
    pub const fn parent_target(&self) -> Target {
        self.parent_target
    }

    /// Returns the parent predecessor hash in conventional display order.
    pub fn parent_tip_display(&self) -> &str {
        &self.parent_tip_display
    }

    /// Returns the candidate parent height.
    pub const fn parent_height(&self) -> u32 {
        self.parent_height
    }

    /// Validates one share and independently classifies Wcash and Zcash winners.
    pub fn validate_share(
        &self,
        nonce: &[u8],
        solution: &[u8],
        share_target: Target,
    ) -> Result<ValidatedNativeShare, MinerError> {
        let header = self.job.parent_header(nonce, solution)?;
        let work = header.validate_work(share_target)?;
        let hash = work.block_hash();
        let hash_bytes = hash.into_le_bytes();

        let wcash_candidate = self
            .job
            .required_target()
            .is_met_by_le_hash(hash_bytes)
            .then(|| self.job.finalize(nonce, solution))
            .transpose()?;
        let parent_block = self
            .parent_target
            .is_met_by_le_hash(hash_bytes)
            .then(|| self.solved_parent_block(nonce, solution))
            .transpose()?;

        Ok(ValidatedNativeShare {
            parent_block_hash: hash,
            accepted_target: share_target,
            wcash_candidate,
            parent_block,
        })
    }

    fn solved_parent_block(&self, nonce: &[u8], solution: &[u8]) -> Result<Vec<u8>, MinerError> {
        let nonce: [u8; 32] = nonce
            .try_into()
            .map_err(|_| MinerError::InvalidNonceLength(nonce.len()))?;
        let solution: [u8; EQUIHASH_SOLUTION_BYTES] = solution
            .try_into()
            .map_err(|_| MinerError::InvalidSolutionLength(solution.len()))?;
        let mut block = self.parent_proposal.clone();
        let mut header = block.header.as_ref().clone();
        header.nonce = nonce.into();
        header.solution = Solution::Common(solution);
        block.header = Arc::new(header);
        let bytes = block.zcash_serialize_to_vec()?;
        if bytes.len() != self.proposal_bytes.len() {
            return Err(MinerError::InvalidParentTemplate(
                "solved parent serialization changed the proposal size".to_string(),
            ));
        }
        Ok(bytes)
    }
}

/// A valid pool share, classified against both independent network targets.
#[derive(Clone, Debug)]
pub struct ValidatedNativeShare {
    parent_block_hash: ParentBlockHash,
    accepted_target: Target,
    wcash_candidate: Option<SolvedAuxPow>,
    parent_block: Option<Vec<u8>>,
}

impl ValidatedNativeShare {
    /// Returns the verified parent header hash.
    pub const fn parent_block_hash(&self) -> ParentBlockHash {
        self.parent_block_hash
    }

    /// Returns the exact fixed share target used to accept this work.
    pub const fn accepted_target(&self) -> Target {
        self.accepted_target
    }

    /// Returns a Wcash AuxPoW candidate only when the child target was met.
    pub const fn wcash_candidate(&self) -> Option<&SolvedAuxPow> {
        self.wcash_candidate.as_ref()
    }

    /// Returns exact parent block bytes only when the Zcash target was met.
    pub fn parent_block(&self) -> Option<&[u8]> {
        self.parent_block.as_deref()
    }
}

/// Result of broadcasting one Zcash parent winner.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParentSubmissionReport {
    outcomes: Vec<ParentNodeOutcome>,
}

impl ParentSubmissionReport {
    /// Returns every unique node outcome.
    pub fn outcomes(&self) -> &[ParentNodeOutcome] {
        &self.outcomes
    }

    /// Returns true when at least one node confirmed the exact best-chain block.
    pub fn is_confirmed(&self) -> bool {
        self.outcomes
            .iter()
            .any(|outcome| matches!(outcome, ParentNodeOutcome::Accepted { .. }))
    }
}

/// Per-node outcome for a parent winner broadcast.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParentNodeOutcome {
    /// Node accepted and confirmed the exact block at the expected height.
    Accepted {
        /// Credential-free endpoint label.
        endpoint: String,
    },
    /// Node accepted the RPC call but had not confirmed the block yet.
    Unconfirmed {
        /// Credential-free endpoint label.
        endpoint: String,
    },
    /// Node explicitly rejected the block.
    Rejected {
        /// Credential-free endpoint label.
        endpoint: String,
        /// Node-provided rejection reason.
        reason: String,
    },
    /// Node could not be reached or returned invalid RPC framing.
    Unavailable {
        /// Credential-free endpoint label.
        endpoint: String,
        /// Sanitized failure reason.
        reason: String,
    },
}

fn sha256d_branch(leaves: &[[u8; 32]]) -> Result<Vec<[u8; 32]>, MinerError> {
    if leaves.is_empty() {
        return Err(MinerError::InvalidParentTemplate(
            "parent block has no transactions".to_string(),
        ));
    }
    let mut level = leaves.to_vec();
    let mut branch = Vec::new();
    let mut index = 0usize;
    while level.len() > 1 {
        if level.len() % 2 == 1 {
            let last = *level.last().expect("nonempty level");
            level.push(last);
        }
        branch.push(level[index ^ 1]);
        level = level
            .chunks_exact(2)
            .map(|pair| sha256d_pair(&pair[0], &pair[1]))
            .collect();
        index /= 2;
    }
    Ok(branch)
}

fn auth_data_branch(leaves: &[[u8; 32]]) -> Result<Vec<[u8; 32]>, MinerError> {
    if leaves.is_empty() {
        return Err(MinerError::InvalidParentTemplate(
            "parent block has no authorization digests".to_string(),
        ));
    }
    let mut level = leaves.to_vec();
    level.resize(level.len().next_power_of_two(), [0; 32]);
    let mut branch = Vec::new();
    let mut index = 0usize;
    while level.len() > 1 {
        branch.push(level[index ^ 1]);
        level = level
            .chunks_exact(2)
            .map(|pair| auth_data_hash(&pair[0], &pair[1]))
            .collect();
        index /= 2;
    }
    Ok(branch)
}

fn sha256d_pair(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut first = Sha256::new();
    first.update(left);
    first.update(right);
    Sha256::digest(first.finalize()).into()
}

fn auth_data_hash(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    blake2b_simd::Params::new()
        .hash_length(32)
        .personal(b"ZcashAuthDatHash")
        .to_state()
        .update(left)
        .update(right)
        .finalize()
        .as_bytes()
        .try_into()
        .expect("BLAKE2b output length is fixed at 32 bytes")
}

fn display_hex(mut raw: [u8; 32]) -> String {
    raw.reverse();
    hex::encode(raw)
}

fn parent_is_confirmed(node: &ZebraRpcClient, height: u32, expected_hash: &str) -> bool {
    parent_confirmation_depth_on_node(node, height, expected_hash)
        .is_ok_and(|depth| depth.is_some())
}

fn parent_confirmation_depth_on_node(
    node: &ZebraRpcClient,
    height: u32,
    expected_hash: &str,
) -> Result<Option<u32>, MinerError> {
    let response = node.call("getblockheader", json!([expected_hash, true]));
    classify_parent_header_response(response, height, expected_hash)
}

fn classify_parent_header_response(
    response: Result<ParentBlockHeaderStatus, MinerError>,
    expected_height: u32,
    expected_hash: &str,
) -> Result<Option<u32>, MinerError> {
    match response {
        // Zcash uses -5 (InvalidAddressOrKey) when an exact block hash is not
        // available on the queried chain. This is an authoritative maturity
        // veto, not a transport failure that another node can override.
        Err(MinerError::RpcError { code: Some(-5), .. }) => Ok(None),
        Err(error) => Err(error),
        Ok(status) => validate_parent_header_status(status, expected_height, expected_hash),
    }
}

fn validate_parent_header_status(
    status: ParentBlockHeaderStatus,
    expected_height: u32,
    expected_hash: &str,
) -> Result<Option<u32>, MinerError> {
    if !status.hash.eq_ignore_ascii_case(expected_hash) {
        return Err(MinerError::RpcProtocol(
            "getblockheader returned a different parent block hash".to_string(),
        ));
    }
    if status.height != expected_height {
        return Err(MinerError::RpcProtocol(format!(
            "getblockheader returned parent height {}, expected {expected_height}",
            status.height
        )));
    }
    if status.confirmations <= 0 {
        return Ok(None);
    }
    u32::try_from(status.confirmations)
        .map(Some)
        .map_err(|_| MinerError::RpcProtocol("parent confirmation depth exceeds u32".to_string()))
}

fn conservative_parent_confirmation_depth(
    checks: impl IntoIterator<Item = Result<Option<u32>, MinerError>>,
) -> Result<Option<u32>, MinerError> {
    let mut conservative_depth = None;
    let mut unavailable = None;
    for check in checks {
        match check {
            Ok(None) => return Ok(None),
            Ok(Some(depth)) => {
                conservative_depth =
                    Some(conservative_depth.map_or(depth, |current: u32| current.min(depth)));
            }
            Err(error) if is_transient_parent_rpc_failure(&error) => unavailable = Some(error),
            Err(error) => return Err(error),
        }
    }

    conservative_depth.map(Some).ok_or_else(|| {
        unavailable.unwrap_or_else(|| {
            MinerError::RpcProtocol(
                "no pinned parent node could report winner confirmation depth".to_string(),
            )
        })
    })
}

fn is_transient_parent_rpc_failure(error: &MinerError) -> bool {
    matches!(error, MinerError::RpcTransport(_) | MinerError::Io(_))
}

fn submit_parent_to_node(
    node: &ZebraRpcClient,
    block_bytes: &[u8],
    height: u32,
    expected_hash: &str,
) -> ParentNodeOutcome {
    let submission = node.call_value("submitblock", json!([hex::encode(block_bytes)]));
    if parent_is_confirmed(node, height, expected_hash) {
        return ParentNodeOutcome::Accepted {
            endpoint: node.label().to_string(),
        };
    }
    match submission {
        Ok(Value::Null) => ParentNodeOutcome::Unconfirmed {
            endpoint: node.label().to_string(),
        },
        Ok(Value::String(reason)) => ParentNodeOutcome::Rejected {
            endpoint: node.label().to_string(),
            reason,
        },
        Ok(other) => ParentNodeOutcome::Rejected {
            endpoint: node.label().to_string(),
            reason: format!("unexpected submitblock result {other}"),
        },
        Err(error) => ParentNodeOutcome::Unavailable {
            endpoint: node.label().to_string(),
            reason: error.to_string(),
        },
    }
}

fn decode_template_coinbase(template: &TransactionTemplate) -> Result<Transaction, MinerError> {
    let coinbase_bytes =
        decode_template_bytes(&template.data, "coinbasetxn.data", MAX_COINBASE_BYTES)?;
    let coinbase: Transaction =
        coinbase_bytes
            .as_slice()
            .zcash_deserialize_into()
            .map_err(|error| {
                MinerError::InvalidParentTemplate(format!("invalid coinbase bytes: {error}"))
            })?;
    if coinbase.zcash_serialize_to_vec()? != coinbase_bytes {
        return Err(MinerError::InvalidParentTemplate(
            "coinbasetxn is not canonically serialized".to_string(),
        ));
    }
    if !coinbase.is_coinbase() {
        return Err(MinerError::InvalidParentTemplate(
            "coinbasetxn is not a coinbase".to_string(),
        ));
    }
    let declared_hash: TransactionHash = parse_template_hex(&template.hash, "coinbasetxn.hash")?;
    if coinbase.hash() != declared_hash {
        return Err(MinerError::InvalidParentTemplate(
            "coinbase txid does not match coinbasetxn.hash".to_string(),
        ));
    }
    let auth_digest = coinbase.auth_digest().ok_or_else(|| {
        MinerError::InvalidParentTemplate(
            "parent coinbase must be a v5 or v6 transaction".to_string(),
        )
    })?;
    let declared_auth: AuthDigest =
        parse_template_hex(&template.auth_digest, "coinbasetxn.authdigest")?;
    if auth_digest != declared_auth {
        return Err(MinerError::InvalidParentTemplate(
            "coinbase auth digest does not match coinbasetxn.authdigest".to_string(),
        ));
    }
    Ok(coinbase)
}

fn validate_independent_parent_payout_template(
    prepared: &NativePreparedJob,
    validator_template: &BlockTemplateResponse,
    expected_address: &ZcashAddress,
    endpoint: &str,
) -> Result<(), MinerError> {
    if validator_template.version != 4 || validator_template.height != prepared.parent_height {
        return Err(MinerError::InvalidParentTemplate(format!(
            "independent payout template from {endpoint} has a different version or height"
        )));
    }
    let validator_previous: block::Hash = parse_template_hex(
        &validator_template.previous_block_hash,
        "independent payout template previousblockhash",
    )?;
    if validator_previous != prepared.parent_proposal.header.previous_block_hash {
        return Err(MinerError::InvalidParentTemplate(format!(
            "independent payout template from {endpoint} has a different predecessor"
        )));
    }

    let validator_coinbase = decode_template_coinbase(&validator_template.coinbase_txn)?;
    let validator_payout =
        zebra_chain::primitives::zcash_note_encryption::publicly_recoverable_coinbase_value_to(
            &validator_coinbase,
            expected_address,
        )
        .ok_or_else(|| {
            MinerError::InvalidParentTemplate(format!(
                "independent payout template from {endpoint} has outputs that do not match the configured payout address"
            ))
        })?;
    if validator_payout == 0 {
        return Err(MinerError::InvalidParentTemplate(format!(
            "independent payout template from {endpoint} has no positive configured payout"
        )));
    }

    let prepared_coinbase = prepared
        .parent_proposal
        .transactions
        .first()
        .ok_or_else(|| {
            MinerError::InvalidParentTemplate(
                "prepared parent proposal has no coinbase transaction".to_string(),
            )
        })?;
    validate_matching_non_payout_transparent_outputs(
        &prepared_coinbase.outputs(),
        &validator_coinbase.outputs(),
        expected_address,
        endpoint,
    )?;

    Ok(())
}

/// Checks every transparent output other than payments to the configured miner.
///
/// Two honest nodes can select different mempool transactions on the same tip, so
/// their transparent miner outputs can legitimately differ by the selected fees.
/// Funding-stream and lockbox outputs are independent of those fees and must still
/// match exactly. The exact prepared block is subsequently checked in proposal
/// mode, which enforces its consensus subsidy and fee total.
fn validate_matching_non_payout_transparent_outputs(
    prepared_outputs: &[transparent::Output],
    validator_outputs: &[transparent::Output],
    expected_address: &ZcashAddress,
    endpoint: &str,
) -> Result<(), MinerError> {
    let prepared_non_payout = non_payout_transparent_outputs(prepared_outputs, expected_address)?;
    let validator_non_payout = non_payout_transparent_outputs(validator_outputs, expected_address)?;

    if prepared_non_payout != validator_non_payout {
        return Err(MinerError::InvalidParentTemplate(format!(
            "parent template non-payout transparent coinbase outputs differ from independent payout template at {endpoint}"
        )));
    }

    Ok(())
}

fn non_payout_transparent_outputs<'a>(
    outputs: &'a [transparent::Output],
    expected_address: &ZcashAddress,
) -> Result<Vec<&'a transparent::Output>, MinerError> {
    outputs
        .iter()
        .filter_map(|output| match transparent_output_receiver(output) {
            Ok(receiver) if expected_address.matches_receiver(&receiver) => None,
            Ok(_) => Some(Ok(output)),
            Err(error) => Some(Err(error)),
        })
        .collect()
}

fn transparent_output_receiver(output: &transparent::Output) -> Result<Receiver, MinerError> {
    // The selected network only changes textual address encoding; the receiver
    // payload recovered from a P2PKH or P2SH script is network-independent.
    match output.address(&Network::Mainnet).ok_or_else(|| {
        MinerError::InvalidParentTemplate(
            "parent coinbase contains a non-standard transparent output".to_string(),
        )
    })? {
        transparent::Address::PayToPublicKeyHash { pub_key_hash, .. } => {
            Ok(Receiver::P2pkh(pub_key_hash))
        }
        transparent::Address::PayToScriptHash { script_hash, .. } => {
            Ok(Receiver::P2sh(script_hash))
        }
        // A TEX address uses the same P2PKH output script and is therefore
        // indistinguishable on chain from its P2PKH receiver.
        transparent::Address::Tex {
            validating_key_hash,
            ..
        } => Ok(Receiver::P2pkh(validating_key_hash)),
    }
}

fn parse_template_hex<T>(encoded: &str, field: &str) -> Result<T, MinerError>
where
    T: FromHex,
    T::Error: std::fmt::Display,
{
    T::from_hex(encoded)
        .map_err(|error| MinerError::InvalidParentTemplate(format!("invalid {field}: {error}")))
}

fn validate_display_hash(encoded: &str, field: &'static str) -> Result<(), MinerError> {
    if encoded.len() != 64 {
        return Err(MinerError::InvalidHexField {
            field,
            reason: format!("expected 64 hexadecimal characters, got {}", encoded.len()),
        });
    }
    hex::decode(encoded)
        .map(|_| ())
        .map_err(|error| MinerError::InvalidHexField {
            field,
            reason: error.to_string(),
        })
}

fn decode_template_bytes(
    encoded: &str,
    field: &str,
    maximum_bytes: usize,
) -> Result<Vec<u8>, MinerError> {
    let maximum_hex_chars = maximum_bytes.checked_mul(2).ok_or_else(|| {
        MinerError::InvalidParentTemplate(format!("invalid byte limit for {field}"))
    })?;
    if encoded.len() > maximum_hex_chars {
        return Err(MinerError::InvalidParentTemplate(format!(
            "{field} encodes more than {maximum_bytes} bytes"
        )));
    }
    if !encoded.len().is_multiple_of(2) {
        return Err(MinerError::InvalidParentTemplate(format!(
            "{field} has an odd number of hexadecimal characters"
        )));
    }
    hex::decode(encoded)
        .map_err(|error| MinerError::InvalidParentTemplate(format!("invalid {field}: {error}")))
}

#[cfg(test)]
mod tests {
    use wcash_zcash_aux::{auth_data_merkle_root, PROOF_VERSION};
    use zcash_address::ToAddress;
    use zcash_protocol::consensus::NetworkType;
    use zebra_chain::{
        amount::{Amount, NonNegative},
        parameters::NetworkKind,
        transaction::AuthDigest,
    };

    use super::*;

    #[test]
    fn branches_match_consensus_root_functions() {
        let txids = [[1; 32], [2; 32], [3; 32], [4; 32], [5; 32]];
        let tx_branch = sha256d_branch(&txids).expect("nonempty transaction list");
        let expected_tx_root: block::merkle::Root = txids
            .iter()
            .copied()
            .map(zebra_chain::transaction::Hash)
            .collect();
        assert_eq!(
            sha256d_merkle_root(txids[0], &tx_branch, 0).expect("valid branch"),
            expected_tx_root.0
        );

        let auth = [[6; 32], [7; 32], [8; 32], [9; 32], [10; 32]];
        let auth_branch = auth_data_branch(&auth).expect("nonempty auth list");
        let expected_auth: AuthDataRoot = auth.iter().copied().map(AuthDigest).collect();
        assert_eq!(
            auth_data_merkle_root(auth[0], &auth_branch, 0).expect("valid auth branch"),
            <[u8; 32]>::from(expected_auth)
        );
        assert_eq!(PROOF_VERSION, 2, "native jobs require AuxPoW v2");
    }

    #[test]
    fn native_config_requires_an_independent_proposal_gate() {
        let endpoint =
            RpcEndpoint::new("http://127.0.0.1:8232", None, None).expect("loopback endpoint");
        let genesis = "00".repeat(32);
        let payout_address: ZcashAddress = "tmJymvcUCn1ctbghvTJpXBwHiMEB8P6wxNV"
            .parse()
            .expect("valid Zcash testnet address");
        assert!(NativeZcashConfig::new(
            endpoint.clone(),
            Vec::new(),
            genesis.clone(),
            payout_address.clone(),
        )
        .is_err());
        assert!(NativeZcashConfig::new(
            endpoint.clone(),
            vec![endpoint],
            genesis.clone(),
            payout_address.clone(),
        )
        .is_err());
        let validator = RpcEndpoint::new("http://127.0.0.1:8233", None, None)
            .expect("second loopback endpoint");
        assert!(NativeZcashConfig::new(
            RpcEndpoint::new("http://127.0.0.1:8232", None, None).expect("template endpoint"),
            vec![validator],
            genesis,
            payout_address.clone(),
        )
        .is_ok());
        let remote_template =
            RpcEndpoint::new("https://template.example:8232", None, None).expect("remote HTTPS");
        let remote_validator =
            RpcEndpoint::new("https://validator.example:8232", None, None).expect("remote HTTPS");
        assert!(NativeZcashConfig::new(
            remote_template,
            vec![remote_validator],
            "00".repeat(32),
            payout_address.clone(),
        )
        .is_err());

        let bypass_attempt = NativeZcashConfig {
            template_node: RpcEndpoint::new("http://127.0.0.1:8232", None, None)
                .expect("loopback endpoint"),
            proposal_validators: Vec::new(),
            expected_genesis_hash: "00".repeat(32),
            expected_parent_payout_commitment: parent_payout_address_commitment(
                &payout_address.to_string(),
            ),
            expected_parent_payout_address: payout_address,
            rpc_timeout: DEFAULT_RPC_TIMEOUT,
        };
        assert!(
            NativeZcashProvider::connect(bypass_attempt).is_err(),
            "the provider must revalidate configs even when an in-crate caller bypasses new()"
        );
    }

    #[test]
    fn template_hex_decoder_is_bounded_and_strict() {
        assert_eq!(
            decode_template_bytes("00ff", "field", 2).expect("bounded hex"),
            [0, 255]
        );
        assert!(decode_template_bytes("000000", "field", 2).is_err());
        assert!(decode_template_bytes("0", "field", 2).is_err());
        assert!(decode_template_bytes("zz", "field", 2).is_err());
        assert!(parse_template_hex::<block::Hash>("00", "hash").is_err());
    }

    #[test]
    fn local_dto_distinguishes_standard_and_attested_gbt() {
        let fixture =
            include_str!("../../zebra-rpc/tests/vectors/getblocktemplate_response_template.json");
        let ordinary: BlockTemplateResponse =
            serde_json::from_str(fixture).expect("ordinary GBT fixture decodes");
        assert_eq!(ordinary.parent_payout_commitment, None);
        let mut value: Value = serde_json::from_str(fixture).expect("valid standard fixture");
        value
            .as_object_mut()
            .expect("GBT fixture is an object")
            .insert(
                "wcashparentpayoutcommitment".to_string(),
                Value::String("55".repeat(32)),
            );
        let template: BlockTemplateResponse =
            serde_json::from_value(value).expect("private GBT fixture decodes");
        assert_eq!(template.version, 4);
        assert_eq!(template.height, 2_931_867);
        assert_eq!(template.previous_block_hash.len(), 64);
        assert!(template.transactions.is_empty());
        assert_eq!(template.parent_payout_commitment, Some("55".repeat(32)));
    }

    fn transparent_output(receiver: Receiver, value: i64) -> transparent::Output {
        let address = match receiver {
            Receiver::P2pkh(hash) => {
                transparent::Address::from_pub_key_hash(NetworkKind::Testnet, hash)
            }
            Receiver::P2sh(hash) => {
                transparent::Address::from_script_hash(NetworkKind::Testnet, hash)
            }
            _ => panic!("transparent output fixture requires a transparent receiver"),
        };
        transparent::Output::new(
            Amount::<NonNegative>::try_from(value).expect("fixture value is non-negative"),
            address.script(),
        )
    }

    #[test]
    fn independent_transparent_payout_allows_honest_fee_divergence() {
        let payout_hash = [0x11; 20];
        let funding_output = transparent_output(Receiver::P2sh([0x22; 20]), 25_000);
        let expected_address = ZcashAddress::from_transparent_p2pkh(NetworkType::Test, payout_hash);

        // Both nodes extend the same tip and have the same mandatory output, but
        // their selected mempool fees produce different legitimate miner values.
        let prepared_outputs = vec![
            transparent_output(Receiver::P2pkh(payout_hash), 625_010_000),
            funding_output.clone(),
        ];
        let validator_outputs = vec![
            transparent_output(Receiver::P2pkh(payout_hash), 625_030_000),
            funding_output,
        ];

        validate_matching_non_payout_transparent_outputs(
            &prepared_outputs,
            &validator_outputs,
            &expected_address,
            "validator",
        )
        .expect("fee-dependent miner output values may differ");
    }

    #[test]
    fn independent_transparent_payout_rejects_extra_non_miner_output() {
        let payout_hash = [0x11; 20];
        let funding_output = transparent_output(Receiver::P2sh([0x22; 20]), 25_000);
        let expected_address = ZcashAddress::from_transparent_p2pkh(NetworkType::Test, payout_hash);
        let prepared_outputs = vec![
            transparent_output(Receiver::P2pkh(payout_hash), 625_010_000),
            funding_output.clone(),
            transparent_output(Receiver::P2pkh([0x33; 20]), 10_000),
        ];
        let validator_outputs = vec![
            transparent_output(Receiver::P2pkh(payout_hash), 625_030_000),
            funding_output,
        ];

        assert!(validate_matching_non_payout_transparent_outputs(
            &prepared_outputs,
            &validator_outputs,
            &expected_address,
            "validator",
        )
        .is_err());
    }

    #[test]
    fn parent_confirmation_is_bound_to_the_exact_hash_and_height() {
        let hash = "ab".repeat(32);
        let status = ParentBlockHeaderStatus {
            hash: hash.clone(),
            confirmations: 101,
            height: 42,
        };
        assert_eq!(
            validate_parent_header_status(status, 42, &hash).expect("exact header status"),
            Some(101)
        );

        for confirmations in [-1, 0] {
            let status = ParentBlockHeaderStatus {
                hash: hash.clone(),
                confirmations,
                height: 42,
            };
            assert_eq!(
                validate_parent_header_status(status, 42, &hash)
                    .expect("non-best-chain status is authoritative"),
                None
            );
        }

        let wrong_hash = ParentBlockHeaderStatus {
            hash: "cd".repeat(32),
            confirmations: 1,
            height: 42,
        };
        assert!(validate_parent_header_status(wrong_hash, 42, &hash).is_err());
        let wrong_height = ParentBlockHeaderStatus {
            hash: hash.clone(),
            confirmations: 1,
            height: 43,
        };
        assert!(validate_parent_header_status(wrong_height, 42, &hash).is_err());
        let overflow = ParentBlockHeaderStatus {
            hash: hash.clone(),
            confirmations: i64::from(u32::MAX) + 1,
            height: 42,
        };
        assert!(validate_parent_header_status(overflow, 42, &hash).is_err());
    }

    #[test]
    fn parent_confirmation_aggregation_is_conservative_and_error_aware() {
        let hash = "ab".repeat(32);
        let unknown = classify_parent_header_response(
            Err(MinerError::RpcError {
                endpoint: "node-b".to_string(),
                code: Some(-5),
                message: "Block not found".to_string(),
            }),
            42,
            &hash,
        );
        assert_eq!(
            conservative_parent_confirmation_depth([Ok(Some(100)), unknown])
                .expect("an authoritative unknown is a maturity veto"),
            None
        );

        assert_eq!(
            conservative_parent_confirmation_depth([Ok(Some(101)), Ok(Some(7))])
                .expect("agreeing nodes report the lowest depth"),
            Some(7)
        );

        let wrong_height = validate_parent_header_status(
            ParentBlockHeaderStatus {
                hash: hash.clone(),
                confirmations: 100,
                height: 43,
            },
            42,
            &hash,
        );
        assert!(
            conservative_parent_confirmation_depth([Ok(Some(100)), wrong_height]).is_err(),
            "a malformed response must prevent maturity"
        );

        assert_eq!(
            conservative_parent_confirmation_depth([
                Ok(Some(12)),
                Err(MinerError::Io(std::io::Error::from(
                    std::io::ErrorKind::ConnectionReset,
                ))),
            ])
            .expect("one temporarily unavailable node does not erase an exact observation"),
            Some(12)
        );
        assert!(
            conservative_parent_confirmation_depth([Err(MinerError::Io(std::io::Error::from(
                std::io::ErrorKind::ConnectionReset
            ),))])
            .is_err(),
            "transport-only results cannot mature a winner"
        );
        assert!(
            conservative_parent_confirmation_depth([
                Ok(Some(100)),
                Err(MinerError::RpcHttpStatus(
                    reqwest::StatusCode::SERVICE_UNAVAILABLE,
                )),
            ])
            .is_err(),
            "a reachable HTTP failure must prevent maturity"
        );
    }
}
