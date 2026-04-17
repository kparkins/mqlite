//! `WriteTxn` scaffolding — basic struct for MVCC writers.
//!
//! T3 introduces only the shape: `pending` holds the `OverflowRef` pins
//! acquired during the transaction so that if the transaction aborts
//! (panic / explicit `rollback`), Drop runs and releases refcounts
//! atomically. Full commit wiring (sec-index atomic install, journal
//! `ChainCommit` emission, oracle `commit()` call) lands in T5'.

use crate::mvcc::timestamp::Ts;
use crate::mvcc::version::OverflowRef;

/// In-progress MVCC write transaction.
///
/// Invariants carried forward to T5':
/// - `pending` owns one `OverflowRef` per new overflow chain written by
///   this transaction. If the transaction is dropped without commit, the
///   refcounts decrement back to 0 and the pages are enqueued for
///   deferred-free — no leaks on abort.
/// - `commit_ts == Ts::PENDING` until the transaction reaches commit,
///   at which point T5' stamps it with `TimestampOracle::commit()`.
#[derive(Debug)]
pub struct WriteTxn {
    /// Per-oracle transaction identifier used for self-visibility on
    /// pending entries.
    pub txn_id: u64,
    /// Commit timestamp — `Ts::PENDING` until commit (T5').
    pub commit_ts: Ts,
    /// Overflow chains written by this transaction. Each `OverflowRef`
    /// holds one refcount; if the txn is dropped without commit, the
    /// refcounts fall back to 0 and the pages are deferred-freed.
    pub pending: Vec<OverflowRef>,
}

impl WriteTxn {
    /// Create a new empty transaction.
    pub fn new(txn_id: u64) -> Self {
        Self {
            txn_id,
            commit_ts: Ts::PENDING,
            pending: Vec::new(),
        }
    }

    /// Attach an `OverflowRef` for a newly-written chain. The refcount
    /// is already held by the ref — this transfers ownership to the txn.
    pub fn attach_overflow(&mut self, r: OverflowRef) {
        self.pending.push(r);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[cfg(not(loom))]
mod tests {
    use super::*;
    use crate::mvcc::version::OverflowRef;
    use crate::storage::allocator::AllocatorHandle;
    use crate::storage::header::FileHeader;

    fn fresh_allocator() -> AllocatorHandle {
        AllocatorHandle::new(FileHeader::new(0, 0, 0))
    }

    #[test]
    fn new_txn_starts_empty() {
        let t = WriteTxn::new(7);
        assert_eq!(t.txn_id, 7);
        assert_eq!(t.commit_ts, Ts::PENDING);
        assert!(t.pending.is_empty());
    }

    #[test]
    fn attach_overflow_moves_ownership() {
        let alloc = fresh_allocator();
        let r = OverflowRef::new_owned(12, 64, alloc.clone()).unwrap();
        assert_eq!(alloc.overflow_refcount(12), 1);

        let mut t = WriteTxn::new(1);
        t.attach_overflow(r);
        assert_eq!(t.pending.len(), 1);
        assert_eq!(
            alloc.overflow_refcount(12),
            1,
            "attach is a move, not a clone — refcount unchanged"
        );
    }

    #[test]
    fn txn_drop_decrefs_pending_pins() {
        let alloc = fresh_allocator();
        let r = OverflowRef::new_owned(33, 64, alloc.clone()).unwrap();
        let mut t = WriteTxn::new(1);
        t.attach_overflow(r);
        assert_eq!(alloc.overflow_refcount(33), 1);

        drop(t);
        assert_eq!(alloc.overflow_refcount(33), 0);
        assert_eq!(alloc.deferred_free_queue().depth(), 1);
    }
}
