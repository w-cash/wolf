# Isolated Zcash parent adapter

This crate uses the complete canonical Zcash types from stable Zebra 7.0.0,
revision `e8db87eb69254e0d4eedd058eadd48bdfa09065a`. It does not select Wcash
transaction domains, emission, foundation policy, block spacing or storage.
The existing Wcash parser remains responsible for all legacy parent domains.

`canonical_nu7_coinbase` accepts only bounded, exact, canonical v5/v6 coinbases
with embedded branch `0x77190AD9`. Wcash AuxPoW invokes it only under an
explicit candidate-height policy. Every shipped Wcash network keeps that
activation disabled; a coordinated release must assign the height separately.

The checked-in transparent coinbase fixtures are constructed directly from the
wire grammar. `test-vectors/generate.py` independently calculates their ZIP-244
and ZIP-229 digest trees with Python's standard library. They contain zero-value
OP_TRUE outputs and no shielded bundles. They establish codec and commitment
behavior, not valid Zcash rewards, Equihash solutions, or successful submission.
The AuxPoW tests deliberately retain real Equihash rejection for these fixtures.
Full WEC-only and dual-winning proof qualification remains a release gate.
