# Wcash wallet-service TLS deployment

This directory provides the minimum public transport boundary for Wolf's
lightwalletd-compatible `CompactTxStreamer` service. It does not deploy Wolf,
publish a wallet release, or identify an existing production binary.

The canonical machine-readable status is
[`wcash-wallet-service-manifest.json`](../../wcash-wallet-service-manifest.json).
Its initial state deliberately records no canonical deployment. It keeps a
dated observation of the legacy plaintext endpoint separate from a supported
service claim.

Wolf's `lightwalletd_listen_addr` is plaintext HTTP/2 without authentication.
It must not be exposed directly to the Internet. Public wallets need a dedicated
TLS hostname whose proxy reaches Wolf over loopback or an operator-controlled
private network.

## Required evidence before deployment

Record these values in the operator's deployment record before advertising an
endpoint:

- exact Wolf source commit and clean-tree status;
- `zebrad` SHA-256 digest, target and Cargo features, including
  `wcash-consensus`;
- immutable configuration revision;
- Mainnet genesis and branch ID;
- host or deployment identity and deployment time; and
- the test result from the verifier in this repository.

Do not infer a source commit from `GetLightdInfo`. Its version, vendor and
subversion identify the protocol-facing build, not the Git tree or artifact.
Do not put private keys, RPC cookies, certificate private material, internal IP
addresses or wallet data in a public record.

## Rollout without breaking existing clients

1. Confirm the current process, artifact digest, source commit, feature set and
   configuration. If any of these are unknown, keep the deployment status
   `not deployed` and resolve them on the host.
2. **Migration phase:** keep the existing
   `http://mainnet.zecwec.com:48234` endpoint reachable while adding the new TLS
   hostname. Released h2c clients cannot follow an HTTP-to-HTTPS redirect. Do
   not bind Wolf to loopback or close port `48234` yet.
3. Create one dedicated DNS name for the wallet service. The included
   [`nginx.conf.example`](nginx.conf.example) uses
   `wallet-mainnet.wcashwallet.com`; the maintainer may choose a different
   reviewed name, but wallets and operations must share one canonical value.
4. Check which process already owns public port `443`. The current Mainnet host
   already uses nginx, so add a reviewed nginx server block there; do not start
   Caddy or another listener on the same address and port. Using the example on
   a separate host is also valid after its DNS and firewall are prepared.
5. Obtain a certificate using the operator's existing certificate process,
   update the example paths, and validate the complete active configuration
   with the deployed nginx version before reloading it. The server block
   terminates TLS and proxies gRPC to Wolf over loopback HTTP/2.
6. Permit inbound TCP `443`. Keep JSON-RPC, RPC cookies, metrics and
   administrative listeners outside this public virtual host.
7. Verify the TLS endpoint from a different network before changing a wallet.
   The verifier deliberately provides no option to skip certificate validation:

   ```sh
   python3 scripts/verify-wcash-wallet-service.py \
     wallet-mainnet.wcashwallet.com:443
   ```

   Install `grpcurl` from its reviewed distribution first and run the script
   from this repository checkout so it can use the committed protobuf files.
   The script checks the TLS handshake through `grpcurl`, exact Wcash Mainnet
   genesis, transaction branch, Wcash subversion, a one-block streaming request,
   related tip responses and a configurable tip-age limit. Its JSON output
   records the applied tip-age threshold and is observation evidence, not source
   or artifact provenance. The default 1,800-second limit tolerates a temporary
   mining gap while still rejecting a materially stale service; deployments may
   select and record a tighter reviewed limit.
8. Update the service manifest with the operator-controlled source, artifact,
   configuration and check evidence. Run both its JSON Schema and semantic
   validators. `ready` is rejected unless all required evidence exists.
9. Change desktop and mobile wallet endpoints to the verified TLS hostname.
   Require certificate and hostname validation and do not fall back to HTTP.
10. Test new-wallet sync, restore, transparent and Ironwood receive, shielded
   send, broadcast, history and restart behavior against the TLS endpoint. Then
   publish new candidate builds with source commits and artifact hashes.
11. **Final phase:** after replacement releases are available and the migration
   window ends, make Wolf proxy-only using this setting:

   ```toml
   [rpc]
   lightwalletd_listen_addr = "127.0.0.1:48234"
   ```

   Restart using the reviewed deployment procedure, close public port `48234`,
   and verify the TLS endpoint and firewall state again.

## Local backend check

An operator on the Wolf host can check the loopback backend before configuring
the proxy:

```sh
python3 scripts/verify-wcash-wallet-service.py \
  --plaintext-loopback 127.0.0.1:48234
```

Plaintext mode accepts only literal loopback IP addresses. The production
check must use the public TLS name without `--plaintext-loopback`.

## Monitoring boundary

Run the verifier after deployments and on a schedule appropriate for the
operator. Alert on verification failure, unexpected genesis or branch, and
stale tips. Monitor certificate expiry separately because the verifier performs
normal certificate validation but does not report remaining certificate life.
A successful check proves only what was observed at that time; it does not prove
wallet availability from every region, independent consensus agreement, release
provenance or future uptime.

The endpoint accepts transaction broadcast as part of the CompactTxStreamer
API. Apply connection and request limits at the edge, retain privacy-conscious
operational metrics, and never log request bodies. Treat client IP addresses and
access patterns as sensitive wallet metadata.
