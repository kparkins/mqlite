#![allow(
    clippy::expect_used,
    clippy::panic,
    clippy::unwrap_used,
    reason = "US-015 lock-order tests use assertion-style panics and private probes"
)]

use super::*;

#[cfg(not(loom))]
use crate::storage::test_support::ZeroIo;

#[cfg(not(loom))]
const PAGE_A: u32 = 301;
#[cfg(not(loom))]
const PAGE_B: u32 = 302;
#[cfg(not(loom))]
const LARGE_PAGE_BYTES: usize = 32 * 1024;
#[cfg(not(loom))]
const TEST_CAPACITY: usize = 2;

#[cfg(not(loom))]
fn resident_partition() -> Partition {
    let io = ZeroIo;
    let mut partition = Partition::new(TEST_CAPACITY, LARGE_PAGE_BYTES);

    partition
        .pin_page(PAGE_A, &io, PageSize::Large32k, u64::MAX)
        .expect("PAGE_A must load into partition");
    partition
        .unpin_page(PAGE_A, false, None)
        .expect("PAGE_A must unpin");
    partition
        .pin_page(PAGE_B, &io, PageSize::Large32k, u64::MAX)
        .expect("PAGE_B must load into partition");
    partition
        .unpin_page(PAGE_B, false, None)
        .expect("PAGE_B must unpin");

    for frame in partition.frames.iter_mut().flatten() {
        frame.ref_bit = false;
    }
    partition.clock_hand = 0;
    partition
}

#[cfg(not(loom))]
#[test]
fn eviction_skips_exclusively_latched_frame() {
    let mut partition = resident_partition();
    let latched_idx = partition.page_map[&PAGE_A];
    let fallback_idx = partition.page_map[&PAGE_B];
    let latch = &partition.frames[latched_idx]
        .as_ref()
        .expect("latched frame must be resident")
        .latch as *const PageLatch;
    // SAFETY: `find_victim` only observes the frame and never evicts it.
    // The frame remains resident until `held` is dropped at the end of
    // this test, so the latch reference stays valid.
    let held = unsafe { &*latch }.lock_exclusive();

    let victim = partition
        .find_victim(u64::MAX)
        .expect("fallback frame must remain evictable");

    assert_eq!(
        victim, fallback_idx,
        "CLOCK eviction must skip the exclusively latched frame and choose \
         the unlatched fallback"
    );
    drop(held);
}

#[test]
fn eviction_exclusive_latch_probe_is_guardless_source_audit() {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let latch_path = manifest_dir
        .join("src")
        .join("storage")
        .join("buffer_pool")
        .join("page_latch.rs");
    let latch_body = std::fs::read_to_string(&latch_path).unwrap_or_else(|e| {
        panic!("cannot read {}: {e}", latch_path.display());
    });
    let probe_start = latch_body
        .find("pub(crate) fn is_exclusively_held(")
        .expect("PageLatch::is_exclusively_held must exist");
    let probe_tail = &latch_body[probe_start..];
    let probe_end = probe_tail
        .find("\n    }\n}")
        .expect("PageLatch::is_exclusively_held body must terminate inside impl");
    let probe_slice = &probe_tail[..probe_end];

    for forbidden in [
        ".read(",
        "try_read",
        "try_write",
        "lock_shared",
        "lock_exclusive",
    ] {
        assert!(
            !probe_slice.contains(forbidden),
            "PageLatch::is_exclusively_held must be guard-less and \
             non-acquiring; found forbidden token {forbidden:?}"
        );
    }

    let partition_path = manifest_dir
        .join("src")
        .join("storage")
        .join("buffer_pool")
        .join("partition.rs");
    let partition_body = std::fs::read_to_string(&partition_path).unwrap_or_else(|e| {
        panic!("cannot read {}: {e}", partition_path.display());
    });
    let victim_start = partition_body
        .find("fn find_victim(&mut self, durable_lsn: u64)")
        .expect("Partition::find_victim must exist");
    let victim_slice = &partition_body[victim_start..victim_start.saturating_add(2048)];

    assert!(
        victim_slice.contains("frame.latch.is_exclusively_held()"),
        "Partition::find_victim must skip exclusively latched frames"
    );
}

#[cfg(loom)]
#[test]
fn loom_eviction_cannot_reclaim_latched_frame() {
    loom::model(|| {
        let latch = loom::sync::Arc::new(PageLatch::new());
        let skipped = loom::sync::Arc::new(loom::sync::atomic::AtomicBool::new(false));

        let held = latch.lock_exclusive();
        let evictor_latch = latch.clone();
        let evictor_skipped = skipped.clone();
        let evictor = loom::thread::spawn(move || {
            if evictor_latch.is_exclusively_held() {
                evictor_skipped.store(true, loom::sync::atomic::Ordering::Release);
            }
        });

        evictor.join().unwrap();
        assert!(
            skipped.load(loom::sync::atomic::Ordering::Acquire),
            "eviction predicate must observe the exclusive latch and skip \
             the frame under cfg(loom)"
        );
        drop(held);
    });
}
