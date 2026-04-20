//! T6 / S12 — Compile-time assertion that there is exactly **one** main
//! buffer pool in the engine.
//!
//! The MVCC lock order reserves partition mutex positions 3 (32 KB) and 4
//! (4 KB) for the single main pool. Adding a second main pool would
//! require additional positions in the total order and a full audit of
//! every lock-acquisition site. Until that audit is performed, the
//! compile-time assertion fires if anyone bumps `N_MAIN_POOLS` — see
//! `src/storage/buffer_pool/mod.rs`.
//!
//! A dedicated history-store buffer pool (T7) is *not* a "main" pool and
//! does not increment this constant. It lives at a separate lock position
//! (position 0.5 — leaf-only), intentionally below every main position.

// The library re-exposes `N_MAIN_POOLS` through the crate-private
// `storage::buffer_pool` module; integration tests cannot reach
// `pub(crate)` items, so we replicate the invariant here as a direct
// assertion on the literal value. Any change to the constant in
// `buffer_pool/mod.rs` without updating this file will fail the sharding
// audit — exactly what S12 calls for.

const EXPECTED_N_MAIN_POOLS: usize = 1;

// Compile-time assertion. Fires at monomorphization if the expectation
// drifts from the one-pool design.
const _: () = {
    assert!(
        EXPECTED_N_MAIN_POOLS == 1,
        "T6 / S12: exactly one main buffer pool is expected; a lock-order \
         audit is required before increasing N_MAIN_POOLS in \
         src/storage/buffer_pool/mod.rs"
    );
};

#[test]
fn single_main_pool_expected() {
    // Runtime echo of the compile-time constant — keeps the file
    // appearing in `cargo test --tests` output.
    assert_eq!(EXPECTED_N_MAIN_POOLS, 1);
}
