//! Bug-suspect: `detect_page_size`'s default-to-32k heuristic misclassifies a
//! non-resident 4 KiB interior page, so `pin_for_write` loads it into the
//! WRONG (32 KiB) partition.
//!
//! Suspect (deep-refactor-2026-06-10, rank ~3, smo_latch.rs `acquire_pages`
//! ~:377-388 with buffer_pool/mod.rs `detect_page_size` ~:606-618): the SMO
//! latch planner latches every required page (interior path pages included)
//! via `pin_for_write`, which resolves the page-size partition with
//! `detect_page_size`. That probe checks 32k residency, then 4k residency,
//! and DEFAULTS to `Large32k` when the page is resident in NEITHER partition.
//! An interior (4 KiB) page evicted between plan and acquire is therefore
//! loaded as a 32 KiB frame in the 32 KiB partition — a duplicate frame in
//! the wrong partition whose exclusive latch excludes nothing, silently
//! voiding SMO mutual exclusion against a 4k-sized reader/writer of the same
//! page number.
//!
//! Contrast `free_index_pages_exclusive` (index_ddl.rs), which deliberately
//! uses `pin_for_write_sized` with the size known from the tree walk — proof
//! the heuristic risk is recognized elsewhere.
//!
//! These tests PIN the heuristic's hazardous behavior (the precondition that
//! makes the SMO suspect REAL): a non-resident page resolves to `Large32k`,
//! and `pin_for_write` re-pins an evicted 4 KiB interior page as a 32 KiB
//! frame. The FIX lives in `smo_latch::acquire_pages` (it now pins interior
//! pages at their path-known size via `pin_for_write_sized`), so this test
//! documents *why* the SMO must not use the bare heuristic — see the
//! companion unit test `required_pages_for_shapes_carries_interior_4k_size`
//! in `smo_latch.rs`.

#![allow(clippy::panic, clippy::unwrap_used)]

use super::*;

use crate::storage::buffer_pool::PageSize;
use crate::storage::test_support::ZeroIo;

// A 16 KiB byte budget gives capacity_4k == 1 and capacity_32k == 1 (see
// `BufferPool::new`: size_4k = budget/4 = 4096 -> one 4 KiB frame; size_32k =
// 12288 -> max(0,1) = one 32 KiB frame). The single 4 KiB frame makes eviction
// of our interior page deterministic.
const POOL_BYTES: usize = 4 * 4096;

const INTERIOR_PAGE: u32 = 7; // a 4 KiB internal/path page
const OTHER_4K_PAGE: u32 = 9; // pressure page that evicts INTERIOR_PAGE

#[test]
fn detect_page_size_defaults_nonresident_page_to_32k() {
    let pool = BufferPool::new(POOL_BYTES, Box::new(ZeroIo));

    // A page resident in NEITHER partition.
    assert_eq!(
        pool.detect_page_size(INTERIOR_PAGE),
        PageSize::Large32k,
        "documents the heuristic: a non-resident page is assumed 32 KiB"
    );
}

#[test]
fn pin_for_write_misclassifies_evicted_interior_page() {
    let pool = BufferPool::new(POOL_BYTES, Box::new(ZeroIo));

    // 1. Load the interior page into the 4 KiB partition under its true size
    //    (this is how a B-tree descent / path plan touches an interior page).
    {
        let held = pool
            .pin_for_write_sized(INTERIOR_PAGE, PageSize::Small4k)
            .expect("load interior page as 4 KiB");
        assert_eq!(held.page_size, PageSize::Small4k);
    }
    // While resident in the 4 KiB partition the heuristic is correct.
    assert_eq!(pool.detect_page_size(INTERIOR_PAGE), PageSize::Small4k);

    // 2. Evict it: the 4 KiB partition holds exactly one frame, so pinning a
    //    different 4 KiB page reclaims INTERIOR_PAGE's slot.
    {
        let _evictor = pool
            .pin_for_write_sized(OTHER_4K_PAGE, PageSize::Small4k)
            .expect("pressure pin evicts the interior page from the 4 KiB partition");
    }

    // 3. The interior page is now resident in NEITHER partition — exactly the
    //    "evicted between plan and acquire" window. A bare `pin_for_write`
    //    (the heuristic the SMO planner USED to call) mis-sizes it to 32 KiB
    //    and loads it into the WRONG partition. This is the hazard the SMO
    //    fix sidesteps by pinning at the path-known size.
    let latched = pool
        .pin_for_write(INTERIOR_PAGE)
        .expect("re-pin the evicted interior page via the heuristic path");

    assert_eq!(
        latched.page_size,
        PageSize::Large32k,
        "PIN: the bare residency heuristic mis-sizes an evicted 4 KiB interior \
         page to 32 KiB (loads it into the wrong partition; its exclusive latch \
         excludes nothing against a 4 KiB pin of the same page number). The SMO \
         planner must therefore pin interior pages at their path-known size, \
         not via this heuristic. If this ever returns Small4k the heuristic \
         changed and the SMO contract should be re-examined."
    );
}
