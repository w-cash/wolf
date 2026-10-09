# Wcash documentation

Start with the guide for your task. Wolf contains Wcash-specific code and
inherited Zebra components; a shared crate name does not mean a shared network,
consensus profile or release.

## Guides by task

| Task | Guide |
| --- | --- |
| Understand the project and find services | [Project README](../README.md) |
| Understand the maintainer-approved protocol direction | [Protocol direction](wcash-direction.md) |
| Check release artifacts and deployed-source evidence | [Release and deployment status](wcash-release-status.md) |
| Build, configure and verify a node | [Node operator guide](wcash-node.md) |
| Deploy and verify the public wallet transport | [Wallet-service TLS guide](../deploy/wcash-wallet-service/README.md) |
| Integrate an agent or read-only client | [Agent and integration guide](wcash-agents.md) |
| Change this repository with a coding agent | [Agent contribution instructions](../AGENTS.md) |
| Verify network identities and emission | [Consensus reference](wcash-consensus.md) |
| Distinguish current and retired testing networks | [Testnet reference](wcash-testnet.md) |
| Inspect genesis anchors and encoding | [Genesis crate guide](../wcash-genesis/README.md) |
| Integrate child and parent mining RPC | [Merged-mining contract](wcash-merged-mining.md) |
| Check pool/backend compatibility | [Pool compatibility](wcash-pool-compatibility.md) |
| Run isolated mining and wallet exercises | [Local Regtest guide](wcash-local.md) |
| Use the experimental local wallet | [Wallet guide](../wcash-wallet/README.md) |
| Understand pool signing and payout boundaries | [Wallet payout guide](wcash-wallet-payout.md) |
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
| What does the wallet actually support? | [Wallet implementation](../wcash-wallet/src), [wallet guide](../wcash-wallet/README.md), and [payout boundary](wcash-wallet-payout.md) |
| Which verification jobs are defined? | [Wcash release workflow](../.github/workflows/wcash-release-gate.yml) |
| Which source, artifacts and deployment are officially recorded? | [Release manifest](../wcash-release-manifest.json) and [status guide](wcash-release-status.md) |

A source file proves what that revision implements, not which binary a service
runs. For deployment claims, also record artifact digest, build features,
configuration revision, runtime network identity and dated test results. A CI
workflow definition is not evidence of a successful run.

## Documentation verification

- Use relative repository links so references follow the same branch or commit.
- Label each command's network, feature profile, authentication and side effects.
- Keep Mainnet, current Testnet and Regtest distinct. Never publish real seeds,
  RPC cookies, private keys or unredacted wallet databases as examples.
- Check Markdown, spelling, links and copied constants. Record whether runnable
  examples were executed or only checked against source.
- State untested boundaries: documentation changes do not certify consensus,
  deployments, wallet safety or a release.
