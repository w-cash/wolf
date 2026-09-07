# Wcash security policy

Wcash is pre-testnet software. It has not completed an independent security or
consensus audit and must not be used with funds of real value.

## Reporting a vulnerability

For vulnerabilities in this Wcash fork, use GitHub's private
[Report a vulnerability](https://github.com/w-cash/wolf/security/advisories/new)
flow. Include the exact commit, platform, impact, reproduction steps, and the
smallest practical proof of concept.

Do not open a public issue for a suspected consensus split, counterfeiting bug,
remote compromise, denial-of-service vector, private-key or viewing-key leak,
or another issue that could put users or a future network at risk. Public,
non-sensitive bugs can be filed in the
[Wcash issue tracker](https://github.com/w-cash/wolf/issues).

No email or encrypted-message security channel is currently published for
Wcash. GitHub Security Advisories are the canonical private reporting channel
until the project documents an independently verifiable alternative.

## Upstream issues

Wcash inherits substantial code from Zebra and librustzcash. If a report also
affects an unmodified upstream release, coordinate disclosure with Wcash first
and use the applicable upstream project's security process as well. Do not make
an upstream-impacting issue public before the affected maintainers have had a
reasonable opportunity to investigate and coordinate a fix.

The historical Zcash Foundation disclosure policy in the upstream repository
does not make the Zcash Foundation responsible for this fork or its releases.
