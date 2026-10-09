# Wcash security policy

This policy covers the Wcash fork in `w-cash/wolf`, including its node, AuxPoW
tooling and experimental wallet. The source implements Mainnet, Testnet and
Regtest; the old pre-testnet description no longer describes its capabilities.
Implementation or public operation is not proof of an independent security or
consensus audit. Evaluate each release using its source, build profile, review
scope and published verification evidence. Testnet and Regtest coins have no
monetary value.

## Reporting a vulnerability

For vulnerabilities in this Wcash fork, use GitHub's private
[Report a vulnerability](https://github.com/w-cash/wolf/security/advisories/new)
flow. Include the exact commit, platform, impact, reproduction steps, and the
smallest practical proof of concept.

Do not open a public issue for a suspected consensus split, counterfeiting bug,
remote compromise, denial-of-service vector, private-key or viewing-key leak,
or another issue that could put users or a network at risk. For non-sensitive
bugs and documentation changes, follow [CONTRIBUTING.md](CONTRIBUTING.md).

No email or encrypted-message security channel is currently published for
Wcash. GitHub Security Advisories are the canonical private reporting channel
until the project documents an independently verifiable alternative.

Do not include seed phrases, spending keys, RPC cookies, access tokens or a
wallet database containing secrets in a public report. Use disposable local
reproductions and redact logs. If private GitHub reporting is unavailable, do
not fall back to publishing vulnerability details in a public issue or pull
request; ask a Wcash maintainer for a private reporting path.

## Upstream issues

Wcash inherits substantial code from Zebra and librustzcash. If a report also
affects an unmodified upstream release, coordinate disclosure with Wcash first
and use the applicable upstream project's security process as well. Do not make
an upstream-impacting issue public before the affected maintainers have had a
reasonable opportunity to investigate and coordinate a fix.

The historical Zcash Foundation disclosure policy in the upstream repository
does not make the Zcash Foundation responsible for this fork or its releases.
