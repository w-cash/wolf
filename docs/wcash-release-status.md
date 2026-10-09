# Wcash release and deployment status

Wolf separates source implementation, a built artifact, a published release and
a deployed service. Evidence for one state does not establish the next one.

The canonical machine-readable record is
[`wcash-release-manifest.json`](../wcash-release-manifest.json). Its
[JSON Schema](wcash-release-manifest.schema.json) rejects missing fields,
unexpected fields and a `released` claim without a source commit, artifact hash
and successful CI run.

## Current status

| Evidence | Current value |
| --- | --- |
| Mainnet implementation | Present in source; this is not a release claim |
| Official Wolf source release | `not released` |
| Official Wolf binary artifacts | `not released` |
| Qualifying Wcash CI run | `unknown` |
| Artifact signatures | `not provided`; signatures are optional |
| Production deployment identity | `unknown` |

The manifest deliberately uses `unknown`, `not released`, `not deployed` or
`not provided` when evidence has not been recorded. Do not replace these values
with a guess based on a website, container name, inherited Zebra version, local
build or reachable service.

## Lightweight release contract

A release update is one reviewed manifest change. It records:

- exact Wolf source commit and clean-tree status;
- executable, release profile, Cargo features, default-feature choice, Rust
  toolchain and build target;
- Wcash Mainnet genesis and transaction branch ID;
- every artifact's file name, target, byte size, SHA-256 digest and download URL;
- signature status and optional signature records;
- the qualifying Wcash CI workflow, commit and run URL;
- known limitations; and
- deployment evidence when a particular environment is actually identified.

Artifact signatures are optional in schema version 1. A release can be recorded
without signed commits or detached artifact signatures. In that case the
manifest must keep `signatures.status` as `not provided`, and the release notes
must explain that a hash detects changed bytes but does not independently prove
who published them. Adding signatures later does not require changing this
schema.

The contract does not publish binaries, deploy nodes, require signed Git
commits, rotate keys or enable an inherited Zebra release workflow. It only
prevents absent evidence from being presented as readiness.

## Updating a candidate or release

1. Select the reviewed source commit and confirm the worktree is clean.
2. Build `zebrad` with the exact feature set recorded in the manifest. A Wcash
   node must include `wcash-consensus`; do not relabel a default Zcash build.
3. Run the Wcash release gate for that same commit and record its result and URL.
4. Hash the final files, not an intermediate build directory, and add one
   `artifacts.items` entry per downloadable file.
5. Set the release state to `candidate` while review is incomplete. Use
   `released` only after the tag, download URLs and CI evidence exist.
6. Record limitations plainly. An empty limitations list is invalid.
7. Record a deployment only from operator evidence that ties its source,
   artifact digest and configuration revision together. Otherwise keep its
   status `unknown` or `not deployed`.
8. Validate the file and review the diff before publication.

Install the pinned validator and run the same command used by documentation CI:

```sh
python3 -m pip install check-jsonschema==0.38.2
check-jsonschema \
  --schemafile docs/wcash-release-manifest.schema.json \
  wcash-release-manifest.json
python3 .github/scripts/validate-wcash-release-manifest.py
python3 .github/scripts/test-validate-wcash-release-manifest.py
```

Schema validation checks the evidence shape and release-state requirements. The
semantic validator also checks that release and artifact states agree, CI refers
to the recorded source commit, signatures refer to recorded artifacts, and a
deployment refers to the recorded source and artifact. These checks do not
reproduce a build, inspect a remote deployment, audit consensus or make a wallet
safe.

## Deployment fields

Deployment state stays separate because a released artifact does not prove
which binary a public service runs. A `deployed` entry requires:

- environment name;
- exact source commit;
- artifact SHA-256 digest;
- configuration revision or immutable configuration identifier;
- deployment timestamp; and
- an evidence URL that operators can update without exposing credentials.

Schema version 1 records one deployment summary. If Wolf later needs multiple
public environments, add a versioned schema change rather than overloading
strings or placing secrets in this file.
