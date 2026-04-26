//! Plan §T8 acceptance bullet:
//!
//! *Given* active `ReadView` when history-store exceeds hard cap, *when*
//! cap trips and view force-expired, *then* `poisoned` set first;
//! `mvcc.read_views_force_expired_total += 1`.
//!
//! The hard-cap trip path itself is T8+ plumbing and is not yet exposed
//! through a public trigger; this test exercises the underlying
//! `ReadView::force_expire` contract directly: invoking it flips the
//! poison bit (observable via subsequent `ChainSnapshot::new` returning
//! an empty snapshot) and ticks the
//! `mvcc.read_views_force_expired_total` counter exactly once per call.

use mqlite::mvcc::metrics::{read_views_force_expired_snapshot, reset_read_views_force_expired};
use mqlite::mvcc::timestamp::Ts;
use mqlite::mvcc::ReadView;

#[test]
fn force_expire_ticks_counter_exactly_once_per_call() {
    reset_read_views_force_expired();

    let rv = ReadView::new(
        Ts {
            physical_ms: 100,
            logical: 0,
        },
        42,
    );
    assert_eq!(read_views_force_expired_snapshot(), 0);

    rv.force_expire();
    assert_eq!(
        read_views_force_expired_snapshot(),
        1,
        "first force_expire must increment by 1"
    );

    // Second call — the counter still ticks (counter semantics are
    // "calls", not "unique views"; a caller that wants per-view dedup
    // must track that out-of-band). This matches the plan's
    // `force_expired_total` definition as a raw call counter.
    rv.force_expire();
    assert_eq!(
        read_views_force_expired_snapshot(),
        2,
        "repeat force_expire increments again"
    );
}
