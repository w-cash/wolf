# Wcash documentation

Start with the guide for your task. Wolf contains Wcash-specific code and
inherited Zebra components; a shared crate name does not mean a shared network,
consensus profile or release.

## Guides by task

| Task | Guide |
| --- | --- |
| Understand the project and find services | [Project README](../README.md) |
| Build, configure and verify a node | [Node operator guide](wcash-node.md) |
| Integrate an agent or read-only client | [Agent and integration guide](wcash-agents.md) |
| Change this repository with a coding agent | [Agent contribution instructions](../AGENTS.md) |
| Verify network identities and emission | [Consensus reference](wcash-consensus.md) |
| Distinguish current and retired testing networks | [Testnet reference](wcash-testnet.md) |
| Inspect genesis anchors and encoding | [Genesis crate guide](../wcash-genesis/README.md) |
| Integrate child and parent mining RPC | [Merged-mining contract](wcash-merged-mining.md) |
| Check pool/backend compatibility | [Pool compatibility](wcash-pool-compatibility.md) |
| Run isolated mining and wallet exercises | [Local Regtest guide](wcash-local.md) |
| Understand inspection, signing and payout boundaries | [Wallet payout guide](wcash-wallet-payout.md) |
| Decode AuxPoW and use fixed vectors | [AuxPoW crate guide](../wcash-zcash-aux/README.md) |
| Contribute or privately report a vulnerability | [Contributing](../CONTRIBUTING.md) and [security](../SECURITY.md) |

The inherited [Zebra book](../book/src/SUMMARY.md) describes shared internals and
development practices. Its Zcash public-network settings, downloads and
upstream governance do not define Wcash's corresponding behavior.

## Which source answers which question

| Question | Implementation reference |
| --- | --- |
| Which node feature enables Wcash? | [Node features](../zebrad/Cargo.toml) |
| What genesis must my endpoint return? | [Frozen genesis blocks](../zebra-chain/src/block/genesis.rs) |
| What are the network anchors and P2P identities? | [Network identity crate](../wcash-genesis/src/lib.rs) |
| How does Mainnet issuance work? | [Mainnet recurrence and vectors](../zebra-chain/src/parameters/network/subsidy/wcash_mainnet.rs) |
| How do testing-network subsidies differ? | [Subsidy dispatch and testing rules](../zebra-chain/src/parameters/network/subsidy.rs) |
| Which local RPC methods and types are implemented? | [RPC implementation](../zebra-rpc/src) |
| What does the wallet actually support? | [Wallet implementation](../wcash-wallet/src) and [wallet guide](wcash-wallet-payout.md) |
| Which verification jobs are defined? | [Wcash release workflow](../.github/workflows/wcash-release-gate.yml) |

A source file proves what that revision implements, not which binary a service
runs. For deployment claims, also record artifact digest, build features,
configuration revision, runtime network identity and dated test results. A CI
workflow definition is not evidence of a successful run.

## Keep ecosystem documentation aligned

This is a publishing checklist for maintainers, not a claim that this change
updates the external websites. Link to the applicable committed Wolf reference
rather than maintaining independent copies of consensus constants.

| Surface | Documentation responsibility |
| --- | --- |
| [w.cash](https://w.cash/) | Explain WEC versus ZEC; link node, mining and agent guides; label illustrative terminal snippets; align the whitepaper's revision and Mainnet emission facts |
| [wcashwallet.com](https://wcashwallet.com/) | Explain platform installability/maturity, backup/recovery, privacy and support; distinguish the browser demo, application releases and Wolf's experimental wallet |
| [wcashexplorer.com](https://wcashexplorer.com/) | Publish its own REST contract, correct network/supply facts, indexing versus chain freshness, AuxPoW meanings, and project/wallet/help links |
| [pool.zecwec.com](https://pool.zecwec.com/) | Maintain miner setup and separate WEC/ZEC custody, fees, payout modes, status and support; distinguish public Stratum from private backend RPC |
| [equihash.com](https://equihash.com/) | Link protocol/mining guides and wallet; document data sources, units, measurement windows and ownership relationships |
| [rust.dev/wcash](https://rust.dev/wcash) | Date engineering notes, link exact commits/upgrades, distinguish historical designs from current rules, and link canonical docs |

Before publishing changes to identity, emission, RPC or supported software,
check the relevant rows above. A documentation edit cannot activate consensus
changes. Preserve historical references with a supersession notice instead of
making an old release appear to have different rules.

## Documentation verification

- Use relative repository links so references follow the same branch or commit.
- Label each command's network, feature profile, authentication and side effects.
- Keep Mainnet, current Testnet and Regtest distinct. Never publish real seeds,
  RPC cookies, private keys or unredacted wallet databases as examples.
- Check Markdown, spelling, links and copied constants. Record whether runnable
  examples were executed or only checked against source.
- State untested boundaries: documentation changes do not certify consensus,
  deployments, wallet safety or a release.
