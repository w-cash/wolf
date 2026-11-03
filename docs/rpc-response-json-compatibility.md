# RPC response JSON compatibility

The client response types for `getblock`, `getblockheader`, `getrawtransaction`
and `getrawmempool` select their wire variants by JSON shape. The nested
`GetBlockTransaction` type uses the same string/object dispatch. Strings retain
the existing hex decoders; objects and arrays deserialize directly into their
existing concrete types.

For compatibility, string/object unions also accept positional arrays when the
existing concrete object deserializer accepts them. Invalid positional arrays
still fail that object's field checks. The mempool array variant remains a list
of transaction IDs. This describes the tested JSON forms, without claiming
compatibility with every Serde format.

This avoids Serde's untagged-enum numeric buffer, which cannot deserialize some
decimal fields when another workspace crate enables
`serde_json/arbitrary_precision`. Serialization, public variants, field types,
atomic-unit integers and monetary rules retain their existing behavior. This
change does not make existing floating-point display fields or the `Zec` wrapper
exact decimal monetary parsers.

The same-map `Input` coinbase/non-coinbase union remains unchanged. Its optional
non-coinbase decimal `value` field still has the inherited untagged-buffer
limitation under arbitrary precision. That separate case requires preserving
its variant fallback and ambiguous-object behavior; the response-shape dispatch
does not resolve it.

The regression target `json_enum_deserialization` covers verbose and raw
responses, nested transactions, numeric spellings, exact integer fields above
2^53, malformed fields and invalid shapes. Run it and the existing
`serialization_tests` target both with and without
`--features serde_json/arbitrary_precision`, then in the workspace feature
context. These focused checks do not replace the required complete workspace
checks or establish a deployed-node result.
