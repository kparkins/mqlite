//! US-037 §10.19 C-1 coherence regression — `load_published_coherent`
//! retries the (epoch, sequencer-frontier) publish pair so a reader
//! caught between the publisher's two atomic stores cannot evaluate a
//! freshly-published `start_ts` against a stale frontier and incorrectly
//! hide the entry.
//!
//! These tests live separately from the production `state.rs` they
//! exercise (intrusive-test-in-separate-file rule) and stage the gap
//! state directly via `pub(crate)` atomics rather than through the
//! normal commit pipeline, which closes the inter-store window too
//! quickly to observe deterministically.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::mvcc::Ts;
use crate::storage::buffer_pool::{default_sizes, BufferPool};
use crate::storage::handle::BufferPoolHandle;
use crate::storage::header::FileHeader;
use crate::storage::root_snapshot::PublishedEpoch;
use crate::storage::test_support::{ArcIo, MockIo};

use super::PagedEngine;

const SPIN_OBSERVATION_BUDGET_MS: u64 = 25;

fn buffered_engine() -> PagedEngine {
    let io = Arc::new(MockIo::default());
    let pool = Arc::new(BufferPool::new(
        default_sizes::DESKTOP,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let history_pool = Arc::new(BufferPool::new(
        default_sizes::IOT,
        Box::new(ArcIo(Arc::clone(&io))),
    ));
    let header = FileHeader::new_now();
    let handle = Arc::new(BufferPoolHandle::new(pool, history_pool, header));
    PagedEngine::new_buffered(handle, 0, 0).expect("buffered engine")
}

/// Steady state: when the publish pair is already coherent, the helper
/// returns the loaded epoch immediately without spinning.
#[test]
fn test_load_published_coherent_returns_immediately_when_pair_coherent() {
    let engine = buffered_engine();
    let epoch = engine.shared.load_published_coherent();
    let frontier = engine
        .shared
        .publish_sequencer
        .published_frontier
        .load(Ordering::Acquire);
    assert!(
        frontier >= epoch.visible_ts,
        "coherent helper must return only when frontier >= visible_ts"
    );
}

/// Stage the inter-store gap: publish a new epoch directly without
/// advancing the live `published_frontier`. A worker thread calling
/// `load_published_coherent` must spin until the test publishes the
/// frontier; only then does the helper return the new epoch.
#[test]
fn test_load_published_coherent_retries_until_frontier_catches_up() {
    let engine = Arc::new(buffered_engine());

    // Capture the steady state — frontier and current epoch's visible_ts
    // should match (publish_commit advanced both during open).
    let baseline_epoch = engine.shared.load_published();
    let baseline_frontier = engine
        .shared
        .publish_sequencer
        .published_frontier
        .load(Ordering::Acquire);
    assert_eq!(baseline_frontier, baseline_epoch.visible_ts);

    // Synthesize the inter-store gap: store a new epoch whose
    // `visible_ts` is strictly ahead of the live frontier, leaving the
    // frontier behind. This mirrors what a reader sees if scheduled
    // between `published.store(epoch)` and `published_frontier.store`.
    let gap_visible_ts = Ts {
        physical_ms: baseline_epoch.visible_ts.physical_ms + 1_000,
        logical: 0,
    };
    let gap_epoch = Arc::new(PublishedEpoch {
        visible_ts: gap_visible_ts,
        catalog: Arc::clone(&baseline_epoch.catalog),
        catalog_generation: baseline_epoch.catalog_generation,
    });
    engine.shared.published.store(Arc::clone(&gap_epoch));
    // Live frontier is intentionally NOT advanced here.

    // Worker calls the coherent helper — it must spin, not return.
    let shared = Arc::clone(&engine.shared);
    let worker = thread::spawn(move || shared.load_published_coherent());

    // Sleep long enough that a non-retrying implementation would have
    // returned. If the worker has finished, the helper bypassed its
    // retry contract.
    thread::sleep(Duration::from_millis(SPIN_OBSERVATION_BUDGET_MS));
    assert!(
        !worker.is_finished(),
        "load_published_coherent must spin while frontier < epoch.visible_ts; \
         worker returned without retry"
    );

    // Publish the second half of the pair — the worker spins out and
    // returns the gap epoch.
    engine
        .shared
        .publish_sequencer
        .published_frontier
        .store(gap_visible_ts, Ordering::Release);

    let observed = worker.join().expect("worker thread panicked");
    assert_eq!(
        observed.visible_ts, gap_visible_ts,
        "coherent helper must return the post-gap epoch once frontier catches up"
    );
    assert!(
        engine
            .shared
            .publish_sequencer
            .published_frontier
            .load(Ordering::Acquire)
            >= observed.visible_ts,
        "post-return frontier must be coherent with the returned epoch"
    );
}
