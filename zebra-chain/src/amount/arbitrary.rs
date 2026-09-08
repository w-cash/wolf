//! Randomised test case generation for amounts.

use proptest::prelude::*;

use crate::amount::*;

impl<C> Arbitrary for Amount<C>
where
    C: Constraint + std::fmt::Debug,
{
    type Parameters = ();

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        // Transaction generators eventually convert amounts through
        // `zcash_protocol::value`, whose per-transaction range is bounded by
        // the shared 21-million-coin Zcash and Wcash monetary base.
        let valid_range = C::valid_range();
        let start = (*valid_range.start()).max(-MAX_SINGLE_TRANSACTION_VALUE);
        let end = (*valid_range.end()).min(MAX_SINGLE_TRANSACTION_VALUE);

        (start..=end).prop_map(|v| Self(v, PhantomData)).boxed()
    }

    type Strategy = BoxedStrategy<Self>;
}
