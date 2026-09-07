//! Transaction-related types.

use std::sync::Arc;

use crate::methods::arrayhex;
use chrono::{DateTime, Utc};
use derive_getters::Getters;
use derive_new::new;
use hex::ToHex;
use rand::rngs::OsRng;
use zcash_script::script::Asm;

use zcash_keys::address::Address;
use zcash_primitives::transaction::{
    builder::{BuildConfig, Builder},
    fees::fixed::FeeRule,
};
use zcash_proofs::prover::LocalTxProver;
use zcash_protocol::{consensus::BlockHeight, memo::MemoBytes, value::Zatoshis};
use zcash_transparent::coinbase::MAX_COINBASE_SCRIPT_LEN;
use zebra_chain::{
    amount::{
        self, Amount, NegativeAllowed, NegativeOrZero, NonNegative, MAX_WCASH_COINBASE_VALUE,
    },
    block::{self, merkle::AUTH_DIGEST_PLACEHOLDER, Height},
    orchard,
    parameters::{
        subsidy::{block_subsidy, funding_stream_values, miner_subsidy, SubsidyError},
        Network, NetworkUpgrade,
    },
    primitives::ed25519,
    sapling::ValueCommitment,
    serialization::{ZcashDeserializeInto, ZcashSerialize},
    transaction::{self, SerializedTransaction, Transaction, VerifiedUnminedTx},
    transparent::Script,
};
use zebra_consensus::{error::TransactionError, funding_stream_address};
use zebra_script::Sigops;
use zebra_state::IntoDisk;

use super::zec::Zec;
use super::{super::opthex, get_block_template::MinerParams};

/// Transaction data and fields needed to generate blocks using the `getblocktemplate` RPC.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
#[serde(bound = "FeeConstraint: amount::Constraint + Clone")]
pub struct TransactionTemplate<FeeConstraint>
where
    FeeConstraint: amount::Constraint + Clone + Copy,
{
    /// The hex-encoded serialized data for this transaction.
    #[serde(with = "hex")]
    pub(crate) data: SerializedTransaction,

    /// The transaction ID of this transaction.
    #[serde(with = "hex")]
    #[getter(copy)]
    pub(crate) hash: transaction::Hash,

    /// The authorizing data digest of a v5 transaction, or a placeholder for older versions.
    #[serde(rename = "authdigest")]
    #[serde(with = "hex")]
    #[getter(copy)]
    pub(crate) auth_digest: transaction::AuthDigest,

    /// The transactions in this block template that this transaction depends upon.
    /// These are 1-based indexes in the `transactions` list.
    ///
    /// Zebra's mempool does not support transaction dependencies, so this list is always empty.
    ///
    /// We use `u16` because 2 MB blocks are limited to around 39,000 transactions.
    pub(crate) depends: Vec<u16>,

    /// The fee for this transaction.
    ///
    /// Non-coinbase transactions must be `NonNegative`.
    /// The Coinbase transaction `fee` is the negative sum of the fees of the transactions in
    /// the block, so their fee must be `NegativeOrZero`.
    #[getter(copy)]
    pub(crate) fee: Amount<FeeConstraint>,

    /// The number of transparent signature operations in this transaction.
    pub(crate) sigops: u32,

    /// Is this transaction required in the block?
    ///
    /// Coinbase transactions are required, all other transactions are not.
    pub(crate) required: bool,
}

// Convert from a mempool transaction to a non-coinbase transaction template.
impl From<&VerifiedUnminedTx> for TransactionTemplate<NonNegative> {
    fn from(tx: &VerifiedUnminedTx) -> Self {
        assert!(
            !tx.transaction.transaction.is_coinbase(),
            "unexpected coinbase transaction in mempool"
        );

        Self {
            data: tx.transaction.transaction.as_ref().into(),
            hash: tx.transaction.id.mined_id(),
            auth_digest: tx
                .transaction
                .id
                .auth_digest()
                .unwrap_or(AUTH_DIGEST_PLACEHOLDER),

            // Always empty, not supported by Zebra's mempool.
            depends: Vec::new(),

            fee: tx.miner_fee,

            // Report the full block-level sigop count (legacy + P2SH) so the template `sigops`
            // field matches what the block verifier charges against `MAX_BLOCK_SIGOPS`.
            sigops: tx.block_sigop_count(),

            // Zebra does not require any transactions except the coinbase transaction.
            required: false,
        }
    }
}

impl From<VerifiedUnminedTx> for TransactionTemplate<NonNegative> {
    fn from(tx: VerifiedUnminedTx) -> Self {
        Self::from(&tx)
    }
}

impl TransactionTemplate<NegativeOrZero> {
    /// Constructs a transaction template for a coinbase transaction.
    pub fn new_coinbase(
        net: &Network,
        height: Height,
        miner_params: &MinerParams,
        txs_fee: Amount<NonNegative>,
    ) -> Result<Self, TransactionError> {
        let block_subsidy = block_subsidy(height, net)?;
        let miner_subsidy = miner_subsidy(height, net, block_subsidy)?;
        if net.uses_wcash_consensus() {
            // Check the inherited per-transaction limit before constrained
            // Amount addition. In a standard Zcash build the Amount ceiling is
            // also 21 million coins, so adding a positive subsidy to a
            // limit-sized fee would otherwise surface as a generic overflow.
            let raw_miner_reward = miner_subsidy
                .zatoshis()
                .checked_add(txs_fee.zatoshis())
                .ok_or(SubsidyError::Overflow)?;
            if raw_miner_reward > MAX_WCASH_COINBASE_VALUE {
                return Err(SubsidyError::WcashCoinbaseValueTooLarge.into());
            }
        }
        let miner_reward = (miner_subsidy + txs_fee)?;
        let miner_reward = Zatoshis::try_from(miner_reward)?;

        let mut builder = Builder::new(
            net,
            BlockHeight::from(height),
            BuildConfig::Coinbase {
                miner_data: miner_params.data().clone(),
            },
        );

        let default_memo = MemoBytes::empty();
        let memo = miner_params.memo().unwrap_or(&default_memo);

        // ZIP-233 was dropped from the v6 transaction format, so no burn amount is set here. If the
        // Network Sustainability Mechanism re-introduces a burn, it will be plumbed back through
        // explicitly at that point.

        macro_rules! trace_err {
            ($res:expr, $type:expr) => {
                $res.map_err(|err| tracing::error!("Failed to add {} output: {err}", $type))
                    .ok()
            };
        }

        // On NU6.3 onward the coinbase MUST have an empty Orchard component, and newly shielded
        // coinbase value is routed to the Ironwood pool instead (see the Ironwood pool spec and
        // `coinbase_orchard_component_empty` in zebra-consensus). Ironwood outputs use the same
        // Orchard-shaped `orchard::Address` as their recipient, so a unified miner address with an
        // Orchard receiver just gets routed to the Ironwood output builder from NU6.3 onward.
        let use_ironwood = net.uses_wcash_consensus()
            || NetworkUpgrade::current(net, height) >= NetworkUpgrade::Nu6_3;

        let add_shielded_reward = |builder: &mut Builder<_, _>, addr: &_| {
            // Standard Zcash coinbase outputs are publicly recoverable with the all-zero outgoing
            // viewing key. Wcash intentionally uses ordinary private note encryption instead.
            let ovk = if net.uses_wcash_consensus() {
                None
            } else {
                Some(::orchard::keys::OutgoingViewingKey::from([0u8; 32]))
            };
            if use_ironwood {
                trace_err!(
                    builder.add_ironwood_output::<String>(ovk, *addr, miner_reward, memo.clone()),
                    "Ironwood"
                )
            } else {
                trace_err!(
                    builder.add_orchard_output::<String>(ovk, *addr, miner_reward, memo.clone()),
                    "Orchard"
                )
            }
        };

        let add_sapling_reward = |builder: &mut Builder<_, _>, addr: &_| {
            trace_err!(
                builder.add_sapling_output::<String>(
                    Some(sapling_crypto::keys::OutgoingViewingKey([0u8; 32])),
                    *addr,
                    miner_reward,
                    memo.clone(),
                ),
                "Sapling"
            )
        };

        let add_transparent_reward = |builder: &mut Builder<_, _>, addr| {
            trace_err!(
                builder.add_transparent_output(addr, miner_reward),
                "transparent"
            )
        };

        if net.uses_wcash_consensus() {
            match miner_params.addr() {
                Address::Unified(addr) => addr
                    .orchard()
                    .and_then(|addr| add_shielded_reward(&mut builder, addr)),
                _ => Err(TransactionError::CoinbaseConstruction(
                    "Wcash miner rewards require a Unified address with an Orchard receiver"
                        .to_string(),
                ))?,
            }
        } else {
            match miner_params.addr() {
                Address::Unified(addr) => addr
                    .orchard()
                    .and_then(|addr| add_shielded_reward(&mut builder, addr))
                    .or_else(|| {
                        addr.sapling()
                            .and_then(|addr| add_sapling_reward(&mut builder, addr))
                    })
                    .or_else(|| {
                        addr.transparent()
                            .and_then(|addr| add_transparent_reward(&mut builder, addr))
                    }),

                Address::Sapling(addr) => add_sapling_reward(&mut builder, addr),

                Address::Transparent(addr) => add_transparent_reward(&mut builder, addr),

                _ => Err(TransactionError::CoinbaseConstruction(
                    "Address not supported for miner rewards".to_string(),
                ))?,
            }
        }
        .ok_or(TransactionError::CoinbaseConstruction(
            "Could not construct output with miner reward".to_string(),
        ))?;

        let mut funding_streams = funding_stream_values(height, net, block_subsidy)?
            .into_iter()
            .filter_map(|(receiver, amount)| {
                Some((*funding_stream_address(height, net, receiver)?, amount))
            })
            .chain(net.lockbox_disbursements(height))
            .filter_map(|(addr, amount)| {
                Some((Zatoshis::try_from(amount).ok()?, addr.try_into().ok()?))
            })
            .collect::<Vec<_>>();

        funding_streams.sort();

        for (fs_amount, fs_addr) in funding_streams {
            builder.add_transparent_output(&fs_addr, fs_amount)?;
        }

        let sapling_prover = LocalTxProver::bundled();
        let build_result = builder.build(
            &Default::default(),
            Default::default(),
            Default::default(),
            OsRng,
            &sapling_prover,
            &sapling_prover,
            &FeeRule::non_standard(Zatoshis::ZERO),
        )?;

        let tx = build_result.transaction();
        let mut data = vec![];
        tx.write(&mut data)?;

        Ok(Self {
            data: data.into(),
            hash: tx.txid().as_ref().into(),
            auth_digest: tx.auth_commitment().as_ref().try_into()?,
            depends: Vec::new(),
            fee: (-txs_fee).constrain()?,
            sigops: tx.sigops()?,
            required: true,
        })
    }

    /// Appends the canonical Wcash AuxPoW commitment to a completed v5/v6
    /// Zcash coinbase transaction.
    ///
    /// ZIP-244 excludes the coinbase input script from the transaction ID but
    /// commits it through the authorizing-data digest. This lets a parent node
    /// preserve an already-generated shielded coinbase (including its proofs
    /// and binding authorization), while producing the exact auth-data root
    /// required by the returned block template.
    ///
    /// This method deliberately fails closed on every transaction format other
    /// than the two formats whose transparent prefix is defined identically by
    /// ZIP-244. The wire splice is round-trip checked before any updated
    /// template fields are returned.
    pub(crate) fn with_wcash_aux(
        mut self,
        auxiliary_block_hash: [u8; 32],
        nonce: u32,
    ) -> Result<Self, TransactionError> {
        const V5_V6_TRANSPARENT_PREFIX_BYTES: usize = 20;
        const NULL_PREVOUT_BYTES: usize = 36;

        fn construction_error(message: impl Into<String>) -> TransactionError {
            TransactionError::CoinbaseConstruction(message.into())
        }

        let commitment =
            wcash_zcash_aux::miner_data_commitment(auxiliary_block_hash, &[], 0, nonce).map_err(
                |error| construction_error(format!("invalid Wcash AuxPoW request: {error}")),
            )?;

        let original_bytes = self.data.as_ref();
        let original_transaction: Transaction = original_bytes
            .zcash_deserialize_into()
            .map_err(|error| construction_error(format!("could not parse coinbase: {error}")))?;

        if !matches!(
            &original_transaction,
            Transaction::V5 { .. } | Transaction::V6 { .. }
        ) {
            return Err(construction_error(
                "Wcash AuxPoW commitments require a v5 or v6 Zcash coinbase",
            ));
        }
        if !original_transaction.is_coinbase() {
            return Err(construction_error(
                "Wcash AuxPoW commitment target is not a coinbase transaction",
            ));
        }

        // Reject trailing bytes and any future alternative encoding before
        // relying on the fixed transparent prefix shared by transaction v5 and
        // v6.
        let canonical_original = original_transaction.zcash_serialize_to_vec()?;
        if canonical_original.as_slice() != original_bytes {
            return Err(construction_error(
                "coinbase transaction is not canonically encoded",
            ));
        }
        if original_transaction.hash() != self.hash
            || original_transaction.auth_digest() != Some(self.auth_digest)
        {
            return Err(construction_error(
                "coinbase template identifiers do not match its serialized transaction",
            ));
        }

        let input = original_transaction
            .inputs()
            .first()
            .ok_or_else(|| construction_error("coinbase input is missing"))?;
        let miner_data = input
            .miner_data()
            .ok_or_else(|| construction_error("coinbase miner data is missing"))?;
        let coinbase_script = input
            .coinbase_script()
            .ok_or_else(|| construction_error("coinbase script is invalid"))?;
        let extended_script_len = coinbase_script
            .len()
            .checked_add(commitment.len())
            .ok_or_else(|| construction_error("coinbase script length overflow"))?;
        if extended_script_len > MAX_COINBASE_SCRIPT_LEN {
            return Err(construction_error(format!(
                "Wcash AuxPoW commitment would make the coinbase script {extended_script_len} bytes; maximum is {MAX_COINBASE_SCRIPT_LEN}"
            )));
        }

        let mut extended_miner_data = miner_data.clone();
        extended_miner_data.extend_from_slice(&commitment);
        wcash_zcash_aux::validate_miner_data_commitment(
            &extended_miner_data,
            auxiliary_block_hash,
            &[],
            0,
        )
        .map_err(|error| {
            construction_error(format!(
                "coinbase miner data cannot carry a canonical Wcash AuxPoW commitment: {error}"
            ))
        })?;

        // v5 and v6 both encode the five u32 prefix fields before their
        // transparent input vector. A canonical coinbase then has a one-byte
        // input count, a null prevout, and a one-byte (2..=100) script length.
        // Consensus bounds guarantee both the old and new script lengths stay
        // below the CompactSize discriminator, so changing the length never
        // changes the width of this field.
        let input_count_offset = V5_V6_TRANSPARENT_PREFIX_BYTES;
        let script_length_offset = input_count_offset + 1 + NULL_PREVOUT_BYTES;
        let script_start = script_length_offset + 1;
        if original_bytes.get(input_count_offset) != Some(&1) {
            return Err(construction_error(
                "coinbase input count is not canonically encoded as one",
            ));
        }
        if original_bytes.get(input_count_offset + 1..input_count_offset + 1 + 32)
            != Some(&[0; 32][..])
            || original_bytes.get(input_count_offset + 1 + 32..script_length_offset)
                != Some(&u32::MAX.to_le_bytes()[..])
        {
            return Err(construction_error(
                "coinbase transaction does not contain the expected null prevout",
            ));
        }
        if original_bytes.get(script_length_offset).copied()
            != u8::try_from(coinbase_script.len()).ok()
        {
            return Err(construction_error(
                "coinbase script length does not match the parsed transaction",
            ));
        }
        let script_end = script_start
            .checked_add(coinbase_script.len())
            .ok_or_else(|| construction_error("coinbase script offset overflow"))?;
        if original_bytes.get(script_start..script_end) != Some(coinbase_script.as_slice()) {
            return Err(construction_error(
                "coinbase script bytes do not match the parsed transaction",
            ));
        }

        let mut extended_bytes = Vec::with_capacity(original_bytes.len() + commitment.len());
        extended_bytes.extend_from_slice(&original_bytes[..script_length_offset]);
        extended_bytes.push(
            u8::try_from(extended_script_len)
                .map_err(|_| construction_error("coinbase script length is not CompactSize"))?,
        );
        extended_bytes.extend_from_slice(&original_bytes[script_start..script_end]);
        extended_bytes.extend_from_slice(&commitment);
        extended_bytes.extend_from_slice(&original_bytes[script_end..]);

        let extended_transaction: Transaction = extended_bytes
            .as_slice()
            .zcash_deserialize_into()
            .map_err(|error| {
                construction_error(format!("extended coinbase could not be parsed: {error}"))
            })?;
        if extended_transaction.zcash_serialize_to_vec()? != extended_bytes {
            return Err(construction_error(
                "extended coinbase failed canonical round-trip validation",
            ));
        }
        if extended_transaction.hash() != self.hash
            || extended_transaction.hash() != original_transaction.hash()
        {
            return Err(construction_error(
                "Wcash AuxPoW coinbase mutation unexpectedly changed the transaction ID",
            ));
        }

        let extended_input = extended_transaction
            .inputs()
            .first()
            .ok_or_else(|| construction_error("extended coinbase input is missing"))?;
        wcash_zcash_aux::validate_miner_data_commitment(
            extended_input
                .miner_data()
                .ok_or_else(|| construction_error("extended coinbase miner data is missing"))?,
            auxiliary_block_hash,
            &[],
            0,
        )
        .map_err(|error| {
            construction_error(format!(
                "extended coinbase commitment failed validation: {error}"
            ))
        })?;

        self.data = extended_bytes.into();
        self.hash = extended_transaction.hash();
        self.auth_digest = extended_transaction.auth_digest().ok_or_else(|| {
            construction_error("extended coinbase does not have a ZIP-244 auth digest")
        })?;
        self.sigops = extended_transaction.sigops()?;

        Ok(self)
    }
}

/// A Transaction object as returned by `getrawtransaction` and `getblock` RPC
/// requests.
#[allow(clippy::too_many_arguments)]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct TransactionObject {
    /// Whether specified block is in the active chain or not (only present with
    /// explicit "blockhash" argument)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[getter(copy)]
    pub(crate) in_active_chain: Option<bool>,
    /// The raw transaction, encoded as hex bytes.
    #[serde(with = "hex")]
    pub(crate) hex: SerializedTransaction,
    /// The height of the block in the best chain that contains the tx, -1 if
    /// it's in a side chain block, or `None` if the tx is in the mempool.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[getter(copy)]
    pub(crate) height: Option<i32>,
    /// The height diff between the block containing the tx and the best chain
    /// tip + 1, 0 if it's in a side chain, or `None` if the tx is in the
    /// mempool.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[getter(copy)]
    pub(crate) confirmations: Option<i64>,

    /// Transparent inputs of the transaction.
    #[serde(rename = "vin")]
    pub(crate) inputs: Vec<Input>,

    /// Transparent outputs of the transaction.
    #[serde(rename = "vout")]
    pub(crate) outputs: Vec<Output>,

    /// Sapling spends of the transaction.
    #[serde(rename = "vShieldedSpend")]
    pub(crate) shielded_spends: Vec<ShieldedSpend>,

    /// Sapling outputs of the transaction.
    #[serde(rename = "vShieldedOutput")]
    pub(crate) shielded_outputs: Vec<ShieldedOutput>,

    /// Transparent outputs of the transaction.
    #[serde(rename = "vjoinsplit")]
    pub(crate) joinsplits: Vec<JoinSplit>,

    /// Sapling binding signature of the transaction.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "opthex",
        default,
        rename = "bindingSig"
    )]
    #[getter(copy)]
    pub(crate) binding_sig: Option<[u8; 64]>,

    /// JoinSplit public key of the transaction.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "opthex",
        default,
        rename = "joinSplitPubKey"
    )]
    #[getter(copy)]
    pub(crate) joinsplit_pub_key: Option<[u8; 32]>,

    /// JoinSplit signature of the transaction.
    #[serde(
        skip_serializing_if = "Option::is_none",
        with = "opthex",
        default,
        rename = "joinSplitSig"
    )]
    #[getter(copy)]
    pub(crate) joinsplit_sig: Option<[u8; ed25519::Signature::BYTE_SIZE]>,

    /// Orchard actions of the transaction.
    #[serde(rename = "orchard", skip_serializing_if = "Option::is_none")]
    pub(crate) orchard: Option<Orchard>,

    /// Ironwood actions of the transaction (v6 transactions from NU6.3 onward).
    ///
    /// The Ironwood pool reuses the Orchard-shaped bundle, so this uses the same [`Orchard`] object.
    #[serde(rename = "ironwood", skip_serializing_if = "Option::is_none")]
    pub(crate) ironwood: Option<Orchard>,

    /// The net value of Sapling Spends minus Outputs in ZEC
    #[serde(rename = "valueBalance", skip_serializing_if = "Option::is_none")]
    #[getter(copy)]
    pub(crate) value_balance: Option<f64>,

    /// The net value of Sapling Spends minus Outputs in zatoshis
    #[serde(rename = "valueBalanceZat", skip_serializing_if = "Option::is_none")]
    #[getter(copy)]
    pub(crate) value_balance_zat: Option<i64>,

    /// The size of the transaction in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[getter(copy)]
    pub(crate) size: Option<i64>,

    /// The time the transaction was included in a block.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[getter(copy)]
    pub(crate) time: Option<i64>,

    /// The transaction identifier, encoded as hex bytes.
    #[serde(with = "hex")]
    #[getter(copy)]
    pub txid: transaction::Hash,

    /// The transaction's auth digest. For pre-v5 transactions this will be
    /// ffff..ffff
    #[serde(
        rename = "authdigest",
        with = "opthex",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[getter(copy)]
    pub(crate) auth_digest: Option<transaction::AuthDigest>,

    /// Whether the overwintered flag is set
    pub(crate) overwintered: bool,

    /// The version of the transaction.
    pub(crate) version: u32,

    /// The version group ID.
    #[serde(
        rename = "versiongroupid",
        with = "opthex",
        skip_serializing_if = "Option::is_none",
        default
    )]
    pub(crate) version_group_id: Option<Vec<u8>>,

    /// The lock time
    #[serde(rename = "locktime")]
    pub(crate) lock_time: u32,

    /// The block height after which the transaction expires.
    /// Included for Overwinter+ transactions (matching zcashd), omitted for V1/V2.
    /// See: <https://github.com/zcash/zcash/blob/v6.11.0/src/rpc/rawtransaction.cpp#L224-L226>
    #[serde(rename = "expiryheight", skip_serializing_if = "Option::is_none")]
    #[getter(copy)]
    pub(crate) expiry_height: Option<Height>,

    /// The block hash
    #[serde(
        rename = "blockhash",
        with = "opthex",
        skip_serializing_if = "Option::is_none",
        default
    )]
    #[getter(copy)]
    pub(crate) block_hash: Option<block::Hash>,

    /// The block height after which the transaction expires
    #[serde(rename = "blocktime", skip_serializing_if = "Option::is_none")]
    #[getter(copy)]
    pub(crate) block_time: Option<i64>,
}

/// The transparent input of a transaction.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Input {
    /// A coinbase input.
    Coinbase {
        /// The coinbase scriptSig as hex.
        #[serde(with = "hex")]
        coinbase: Vec<u8>,
        /// The script sequence number.
        sequence: u32,
    },
    /// A non-coinbase input.
    NonCoinbase {
        /// The transaction id.
        txid: String,
        /// The vout index.
        vout: u32,
        /// The script.
        #[serde(rename = "scriptSig")]
        script_sig: ScriptSig,
        /// The script sequence number.
        sequence: u32,
        /// The value of the output being spent in ZEC.
        #[serde(skip_serializing_if = "Option::is_none")]
        value: Option<f64>,
        /// The value of the output being spent, in zats, named to match zcashd.
        #[serde(rename = "valueSat", skip_serializing_if = "Option::is_none")]
        value_zat: Option<i64>,
        /// The address of the output being spent.
        #[serde(skip_serializing_if = "Option::is_none")]
        address: Option<String>,
    },
}

/// The transparent output of a transaction.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct Output {
    /// The value in ZEC.
    value: f64,
    /// The value in zats.
    #[serde(rename = "valueZat")]
    value_zat: i64,
    /// index.
    n: u32,
    /// The scriptPubKey.
    #[serde(rename = "scriptPubKey")]
    script_pub_key: ScriptPubKey,
}

/// The output object returned by `gettxout` RPC requests.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct OutputObject {
    #[serde(rename = "bestblock")]
    best_block: String,
    confirmations: u32,
    value: f64,
    #[serde(rename = "scriptPubKey")]
    script_pub_key: ScriptPubKey,
    version: u32,
    coinbase: bool,
}
impl OutputObject {
    pub fn from_output(
        output: &zebra_chain::transparent::Output,
        best_block: String,
        confirmations: u32,
        version: u32,
        coinbase: bool,
        network: &Network,
    ) -> Self {
        let lock_script = &output.lock_script;
        let addresses = output
            .address(network)
            .map(|address| vec![encode_transparent_address(&address, network)]);
        let req_sigs = addresses.as_ref().map(|a| a.len() as u32);

        let script_pub_key = ScriptPubKey::new(
            zcash_script::script::Code(lock_script.as_raw_bytes().to_vec()).to_asm(false),
            lock_script.clone(),
            req_sigs,
            zcash_script::script::Code(lock_script.as_raw_bytes().to_vec())
                .to_component()
                .ok()
                .and_then(|c| c.refine().ok())
                .and_then(|component| zcash_script::solver::standard(&component))
                .map(|kind| match kind {
                    zcash_script::solver::ScriptKind::PubKeyHash { .. } => "pubkeyhash",
                    zcash_script::solver::ScriptKind::ScriptHash { .. } => "scripthash",
                    zcash_script::solver::ScriptKind::MultiSig { .. } => "multisig",
                    zcash_script::solver::ScriptKind::NullData { .. } => "nulldata",
                    zcash_script::solver::ScriptKind::PubKey { .. } => "pubkey",
                })
                .unwrap_or("nonstandard")
                .to_string(),
            addresses,
        );

        Self {
            best_block,
            confirmations,
            value: crate::methods::types::zec::Zec::from(output.value()).lossy_zec(),
            script_pub_key,
            version,
            coinbase,
        }
    }
}

/// The scriptPubKey of a transaction output.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct ScriptPubKey {
    /// the asm.
    asm: String,
    /// the hex.
    #[serde(with = "hex")]
    hex: Script,
    /// The required sigs.
    #[serde(rename = "reqSigs")]
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[getter(copy)]
    req_sigs: Option<u32>,
    /// The type, eg 'pubkeyhash'.
    r#type: String,
    /// The addresses.
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    addresses: Option<Vec<String>>,
}

/// The scriptSig of a transaction input.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct ScriptSig {
    /// The asm.
    asm: String,
    /// The hex.
    hex: Script,
}

/// A Sprout JoinSplit of a transaction.
#[allow(clippy::too_many_arguments)]
#[serde_with::serde_as]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct JoinSplit {
    /// Public input value in ZEC.
    #[serde(rename = "vpub_old")]
    old_public_value: f64,
    /// Public input value in zatoshis.
    #[serde(rename = "vpub_oldZat")]
    old_public_value_zat: i64,
    /// Public input value in ZEC.
    #[serde(rename = "vpub_new")]
    new_public_value: f64,
    /// Public input value in zatoshis.
    #[serde(rename = "vpub_newZat")]
    new_public_value_zat: i64,
    /// Merkle root of the Sprout note commitment tree.
    #[serde(with = "hex")]
    #[getter(copy)]
    anchor: [u8; 32],
    /// The nullifier of the input notes.
    #[serde_as(as = "Vec<serde_with::hex::Hex>")]
    nullifiers: Vec<[u8; 32]>,
    /// The commitments of the output notes.
    #[serde_as(as = "Vec<serde_with::hex::Hex>")]
    commitments: Vec<[u8; 32]>,
    /// The onetime public key used to encrypt the ciphertexts
    #[serde(rename = "onetimePubKey")]
    #[serde(with = "hex")]
    #[getter(copy)]
    one_time_pubkey: [u8; 32],
    /// The random seed
    #[serde(rename = "randomSeed")]
    #[serde(with = "hex")]
    #[getter(copy)]
    random_seed: [u8; 32],
    /// The input notes MACs.
    #[serde_as(as = "Vec<serde_with::hex::Hex>")]
    macs: Vec<[u8; 32]>,
    /// A zero-knowledge proof using the Sprout circuit.
    #[serde(with = "hex")]
    proof: Vec<u8>,
    /// The output notes ciphertexts.
    #[serde_as(as = "Vec<serde_with::hex::Hex>")]
    ciphertexts: Vec<Vec<u8>>,
}

/// A Sapling spend of a transaction.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct ShieldedSpend {
    /// Value commitment to the input note.
    #[serde(with = "hex")]
    #[getter(skip)]
    cv: ValueCommitment,
    /// Merkle root of the Sapling note commitment tree.
    #[serde(with = "hex")]
    #[getter(copy)]
    anchor: [u8; 32],
    /// The nullifier of the input note.
    #[serde(with = "hex")]
    #[getter(copy)]
    nullifier: [u8; 32],
    /// The randomized public key for spendAuthSig.
    #[serde(with = "hex")]
    #[getter(copy)]
    rk: [u8; 32],
    /// A zero-knowledge proof using the Sapling Spend circuit.
    #[serde(with = "hex")]
    #[getter(copy)]
    proof: [u8; 192],
    /// A signature authorizing this Spend.
    #[serde(rename = "spendAuthSig", with = "hex")]
    #[getter(copy)]
    spend_auth_sig: [u8; 64],
}

// We can't use `#[getter(copy)]` as upstream `sapling_crypto::note::ValueCommitment` is not `Copy`.
impl ShieldedSpend {
    /// The value commitment to the input note.
    pub fn cv(&self) -> ValueCommitment {
        self.cv.clone()
    }
}

/// A Sapling output of a transaction.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct ShieldedOutput {
    /// Value commitment to the input note.
    #[serde(with = "hex")]
    #[getter(skip)]
    cv: ValueCommitment,
    /// The u-coordinate of the note commitment for the output note.
    #[serde(rename = "cmu", with = "hex")]
    cm_u: [u8; 32],
    /// A Jubjub public key.
    #[serde(rename = "ephemeralKey", with = "hex")]
    ephemeral_key: [u8; 32],
    /// The output note encrypted to the recipient.
    #[serde(rename = "encCiphertext", with = "arrayhex")]
    enc_ciphertext: [u8; 580],
    /// A ciphertext enabling the sender to recover the output note.
    #[serde(rename = "outCiphertext", with = "hex")]
    out_ciphertext: [u8; 80],
    /// A zero-knowledge proof using the Sapling Output circuit.
    #[serde(with = "hex")]
    proof: [u8; 192],
}

// We can't use `#[getter(copy)]` as upstream `sapling_crypto::note::ValueCommitment` is not `Copy`.
impl ShieldedOutput {
    /// The value commitment to the output note.
    pub fn cv(&self) -> ValueCommitment {
        self.cv.clone()
    }
}

/// Object with Orchard-specific information.
#[serde_with::serde_as]
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct Orchard {
    /// Array of Orchard actions.
    actions: Vec<OrchardAction>,
    /// The net value of Orchard Actions in ZEC.
    #[serde(rename = "valueBalance")]
    value_balance: f64,
    /// The net value of Orchard Actions in zatoshis.
    #[serde(rename = "valueBalanceZat")]
    value_balance_zat: i64,
    /// The flags.
    #[serde(skip_serializing_if = "Option::is_none")]
    flags: Option<OrchardFlags>,
    /// A root of the Orchard note commitment tree at some block height in the past
    #[serde_as(as = "Option<serde_with::hex::Hex>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[getter(copy)]
    anchor: Option<[u8; 32]>,
    /// Encoding of aggregated zk-SNARK proofs for Orchard Actions
    #[serde_as(as = "Option<serde_with::hex::Hex>")]
    #[serde(skip_serializing_if = "Option::is_none")]
    proof: Option<Vec<u8>>,
    /// An Orchard binding signature on the SIGHASH transaction hash
    #[serde(rename = "bindingSig")]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<serde_with::hex::Hex>")]
    #[getter(copy)]
    binding_sig: Option<[u8; 64]>,
}

/// Object with Orchard-specific information.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct OrchardFlags {
    /// Whether Orchard outputs are enabled.
    #[serde(rename = "enableOutputs")]
    enable_outputs: bool,
    /// Whether Orchard spends are enabled.
    #[serde(rename = "enableSpends")]
    enable_spends: bool,
}

/// The Orchard action of a transaction.
#[allow(clippy::too_many_arguments)]
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Getters, new)]
pub struct OrchardAction {
    /// A value commitment to the net value of the input note minus the output note.
    #[serde(with = "hex")]
    cv: [u8; 32],
    /// The nullifier of the input note.
    #[serde(with = "hex")]
    nullifier: [u8; 32],
    /// The randomized validating key for spendAuthSig.
    #[serde(with = "hex")]
    rk: [u8; 32],
    /// The x-coordinate of the note commitment for the output note.
    #[serde(rename = "cmx", with = "hex")]
    cm_x: [u8; 32],
    /// An encoding of an ephemeral Pallas public key.
    #[serde(rename = "ephemeralKey", with = "hex")]
    ephemeral_key: [u8; 32],
    /// The output note encrypted to the recipient.
    #[serde(rename = "encCiphertext", with = "arrayhex")]
    enc_ciphertext: [u8; 580],
    /// A ciphertext enabling the sender to recover the output note.
    #[serde(rename = "spendAuthSig", with = "hex")]
    spend_auth_sig: [u8; 64],
    /// A signature authorizing the spend in this Action.
    #[serde(rename = "outCiphertext", with = "hex")]
    out_ciphertext: [u8; 80],
}

/// Builds the RPC object for an Orchard-shaped shielded pool (Orchard or Ironwood) from its
/// shielded data and net value balance.
///
/// The Ironwood pool reuses the Orchard bundle shape, so both pools serialize through the same
/// [`Orchard`] object; the caller selects which pool's shielded data and value balance to pass.
fn orchard_shaped_object(
    shielded_data: Option<&orchard::ShieldedData>,
    value_balance: Amount<NegativeAllowed>,
) -> Orchard {
    let actions = shielded_data
        .into_iter()
        .flat_map(|data| data.actions.iter())
        .map(|authorized_action| {
            let action = &authorized_action.action;
            OrchardAction {
                cv: action.cv.into(),
                nullifier: action.nullifier.into(),
                rk: action.rk.into(),
                cm_x: action.cm_x.into(),
                ephemeral_key: action.ephemeral_key.into(),
                enc_ciphertext: action.enc_ciphertext.into(),
                spend_auth_sig: authorized_action.spend_auth_sig.into(),
                out_ciphertext: action.out_ciphertext.into(),
            }
        })
        .collect();

    Orchard {
        actions,
        value_balance: Zec::from(value_balance).lossy_zec(),
        value_balance_zat: value_balance.zatoshis(),
        flags: shielded_data.map(|data| {
            OrchardFlags::new(
                data.flags.contains(orchard::Flags::ENABLE_OUTPUTS),
                data.flags.contains(orchard::Flags::ENABLE_SPENDS),
            )
        }),
        anchor: shielded_data.map(|data| data.shared_anchor.bytes_in_display_order()),
        proof: shielded_data.map(|data| data.proof.bytes_in_display_order()),
        binding_sig: shielded_data.map(|data| data.binding_sig.into()),
    }
}

impl Default for TransactionObject {
    fn default() -> Self {
        Self {
            hex: SerializedTransaction::from(
                [0u8; zebra_chain::transaction::MIN_TRANSPARENT_TX_SIZE as usize].to_vec(),
            ),
            height: Option::default(),
            confirmations: Option::default(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            shielded_spends: Vec::new(),
            shielded_outputs: Vec::new(),
            joinsplits: Vec::new(),
            orchard: None,
            ironwood: None,
            binding_sig: None,
            joinsplit_pub_key: None,
            joinsplit_sig: None,
            value_balance: None,
            value_balance_zat: None,
            size: None,
            time: None,
            txid: transaction::Hash::from([0u8; 32]),
            in_active_chain: None,
            auth_digest: None,
            overwintered: false,
            version: 4,
            version_group_id: None,
            lock_time: 0,
            expiry_height: None,
            block_hash: None,
            block_time: None,
        }
    }
}

impl TransactionObject {
    /// Converts `tx` and `height` into a new `GetRawTransaction` in the `verbose` format.
    #[allow(clippy::unwrap_in_result)]
    #[allow(clippy::too_many_arguments)]
    pub fn from_transaction(
        tx: Arc<Transaction>,
        height: Option<block::Height>,
        confirmations: Option<i64>,
        network: &Network,
        block_time: Option<DateTime<Utc>>,
        block_hash: Option<block::Hash>,
        in_active_chain: Option<bool>,
        txid: transaction::Hash,
    ) -> Self {
        let block_time = block_time.map(|bt| bt.timestamp());
        Self {
            hex: tx.clone().into(),
            height: if in_active_chain.unwrap_or_default() {
                height.map(|height| height.0 as i32)
            } else if block_hash.is_some() {
                // Side chain
                Some(-1)
            } else {
                // Mempool
                None
            },
            confirmations: if in_active_chain.unwrap_or_default() {
                confirmations
            } else if block_hash.is_some() {
                // Side chain
                Some(0)
            } else {
                // Mempool
                None
            },
            inputs: tx
                .inputs()
                .iter()
                .map(|input| match input {
                    zebra_chain::transparent::Input::Coinbase { sequence, .. } => Input::Coinbase {
                        coinbase: input
                            .coinbase_script()
                            .expect("we know it is a valid coinbase script"),
                        sequence: *sequence,
                    },
                    zebra_chain::transparent::Input::PrevOut {
                        sequence,
                        unlock_script,
                        outpoint,
                    } => Input::NonCoinbase {
                        txid: outpoint.hash.encode_hex(),
                        vout: outpoint.index,
                        script_sig: ScriptSig {
                            // https://github.com/zcash/zcash/blob/v6.11.0/src/rpc/rawtransaction.cpp#L240
                            asm: zcash_script::script::Code(unlock_script.as_raw_bytes().to_vec())
                                .to_asm(true),
                            hex: unlock_script.clone(),
                        },
                        sequence: *sequence,
                        value: None,
                        value_zat: None,
                        address: None,
                    },
                })
                .collect(),
            outputs: tx
                .outputs()
                .iter()
                .enumerate()
                .map(|output| {
                    // Parse the scriptPubKey to find destination addresses.
                    let (addresses, req_sigs) = output
                        .1
                        .address(network)
                        .map(|address| (vec![encode_transparent_address(&address, network)], 1))
                        .unzip();

                    Output {
                        value: Zec::from(output.1.value).lossy_zec(),
                        value_zat: output.1.value.zatoshis(),
                        n: output.0 as u32,
                        script_pub_key: ScriptPubKey {
                            // https://github.com/zcash/zcash/blob/v6.11.0/src/rpc/rawtransaction.cpp#L271
                            // https://github.com/zcash/zcash/blob/v6.11.0/src/rpc/rawtransaction.cpp#L45
                            asm: zcash_script::script::Code(
                                output.1.lock_script.as_raw_bytes().to_vec(),
                            )
                            .to_asm(false),
                            hex: output.1.lock_script.clone(),
                            req_sigs,
                            r#type: zcash_script::script::Code(
                                output.1.lock_script.as_raw_bytes().to_vec(),
                            )
                            .to_component()
                            .ok()
                            .and_then(|c| c.refine().ok())
                            .and_then(|component| zcash_script::solver::standard(&component))
                            .map(|kind| match kind {
                                zcash_script::solver::ScriptKind::PubKeyHash { .. } => "pubkeyhash",
                                zcash_script::solver::ScriptKind::ScriptHash { .. } => "scripthash",
                                zcash_script::solver::ScriptKind::MultiSig { .. } => "multisig",
                                zcash_script::solver::ScriptKind::NullData { .. } => "nulldata",
                                zcash_script::solver::ScriptKind::PubKey { .. } => "pubkey",
                            })
                            .unwrap_or("nonstandard")
                            .to_string(),
                            addresses,
                        },
                    }
                })
                .collect(),
            shielded_spends: tx
                .sapling_spends_per_anchor()
                .map(|spend| {
                    let mut anchor = spend.per_spend_anchor.as_bytes();
                    anchor.reverse();

                    let mut nullifier = spend.nullifier.as_bytes();
                    nullifier.reverse();

                    let mut rk: [u8; 32] = spend.clone().rk.into();
                    rk.reverse();

                    let spend_auth_sig: [u8; 64] = spend.spend_auth_sig.into();

                    ShieldedSpend {
                        cv: spend.cv.clone(),
                        anchor,
                        nullifier,
                        rk,
                        proof: spend.zkproof.0,
                        spend_auth_sig,
                    }
                })
                .collect(),
            shielded_outputs: tx
                .sapling_outputs()
                .map(|output| {
                    let mut cm_u: [u8; 32] = output.cm_u.to_bytes();
                    cm_u.reverse();
                    let mut ephemeral_key: [u8; 32] = output.ephemeral_key.into();
                    ephemeral_key.reverse();
                    let enc_ciphertext: [u8; 580] = output.enc_ciphertext.into();
                    let out_ciphertext: [u8; 80] = output.out_ciphertext.into();

                    ShieldedOutput {
                        cv: output.cv.clone(),
                        cm_u,
                        ephemeral_key,
                        enc_ciphertext,
                        out_ciphertext,
                        proof: output.zkproof.0,
                    }
                })
                .collect(),
            joinsplits: tx
                .sprout_joinsplits()
                .map(|joinsplit| {
                    let mut ephemeral_key_bytes: [u8; 32] = joinsplit.ephemeral_key.to_bytes();
                    ephemeral_key_bytes.reverse();

                    JoinSplit {
                        old_public_value: Zec::from(joinsplit.vpub_old).lossy_zec(),
                        old_public_value_zat: joinsplit.vpub_old.zatoshis(),
                        new_public_value: Zec::from(joinsplit.vpub_new).lossy_zec(),
                        new_public_value_zat: joinsplit.vpub_new.zatoshis(),
                        anchor: joinsplit.anchor.bytes_in_display_order(),
                        nullifiers: joinsplit
                            .nullifiers
                            .iter()
                            .map(|n| n.bytes_in_display_order())
                            .collect(),
                        commitments: joinsplit
                            .commitments
                            .iter()
                            .map(|c| c.bytes_in_display_order())
                            .collect(),
                        one_time_pubkey: ephemeral_key_bytes,
                        random_seed: joinsplit.random_seed.bytes_in_display_order(),
                        macs: joinsplit
                            .vmacs
                            .iter()
                            .map(|m| m.bytes_in_display_order())
                            .collect(),
                        proof: joinsplit.zkproof.unwrap_or_default(),
                        ciphertexts: joinsplit
                            .enc_ciphertexts
                            .iter()
                            .map(|c| c.zcash_serialize_to_vec().unwrap_or_default())
                            .collect(),
                    }
                })
                .collect(),
            value_balance: Some(Zec::from(tx.sapling_value_balance().sapling_amount()).lossy_zec()),
            value_balance_zat: Some(tx.sapling_value_balance().sapling_amount().zatoshis()),
            orchard: Some(orchard_shaped_object(
                tx.orchard_shielded_data(),
                tx.orchard_value_balance().orchard_amount(),
            )),
            ironwood: tx.ironwood_shielded_data().map(|data| {
                orchard_shaped_object(Some(data), tx.ironwood_value_balance().ironwood_amount())
            }),
            binding_sig: tx.sapling_binding_sig().map(|raw_sig| raw_sig.into()),
            joinsplit_pub_key: tx.joinsplit_pub_key().map(|raw_key| {
                // Display order is reversed in the RPC output.
                let mut key: [u8; 32] = raw_key.into();
                key.reverse();
                key
            }),
            joinsplit_sig: tx.joinsplit_sig().map(|raw_sig| raw_sig.into()),
            size: tx.as_bytes().len().try_into().ok(),
            time: block_time,
            txid,
            in_active_chain,
            auth_digest: tx.auth_digest(),
            overwintered: tx.is_overwintered(),
            version: tx.version(),
            version_group_id: tx.version_group_id().map(|id| id.to_be_bytes().to_vec()),
            lock_time: tx.raw_lock_time(),
            // zcashd includes expiryheight only for Overwinter+ transactions.
            // For those, expiry_height of 0 means "no expiry" per ZIP-203.
            expiry_height: if tx.is_overwintered() {
                Some(tx.expiry_height().unwrap_or(Height(0)))
            } else {
                None
            },
            block_hash,
            block_time,
        }
    }
}

/// Encodes a transparent address using the active chain's public address namespace.
fn encode_transparent_address(
    address: &zebra_chain::transparent::Address,
    network: &Network,
) -> String {
    if network.uses_wcash_consensus() {
        address
            .encode_wcash(network)
            .expect("transaction output addresses use the supplied network kind")
    } else {
        address.to_string()
    }
}

#[cfg(test)]
mod tests {
    use zebra_chain::{parameters::NetworkKind, transparent::Address as TransparentAddress};

    use super::*;

    #[test]
    fn transaction_outputs_use_the_active_chain_namespace() {
        let wcash_network = Network::new_wcash_regtest();
        let wcash_address = TransparentAddress::from_pub_key_hash(NetworkKind::Regtest, [0; 20]);
        assert_eq!(
            encode_transparent_address(&wcash_address, &wcash_network),
            "WR64VqQpZRujxYnAJmqGK4d4fbqQZRZHazG"
        );

        let zcash_network = Network::new_regtest(Default::default());
        let zcash_address = TransparentAddress::from_pub_key_hash(NetworkKind::Testnet, [0; 20]);
        assert_eq!(
            encode_transparent_address(&zcash_address, &zcash_network),
            zcash_address.to_string()
        );
    }
}
