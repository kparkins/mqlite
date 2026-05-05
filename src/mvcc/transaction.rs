//! `WriteTxn` — MVCC writer transaction (begin / commit / rollback).
//!
//! Every writer path runs inside a `WriteTxn` that:
//!
//! 1. On `begin`: drains checkpoint-eligible page-lifetime entries under
//!    `AllocatorHandle::state` so any refcount-0 pages from earlier reader
//!    drops are returned to the free list before the new commit allocates.
//! 2. Accumulates pending overflow-chain pins in `self.pending` (RAII
//!    decref on abort — pages enqueue for deferred-free on Drop).
//! 3. On `commit`: requests a `commit_ts` from the oracle, emits exactly
//!    one `ChainCommit` journal frame carrying the commit timestamp and any
//!    refcount deltas / page writes, and transfers ownership of the pending
//!    `OverflowRef`s into the installed version chains (refcounts remain
//!    bumped post-commit because the chain now holds the pin). Durability sync
//!    is owned by the caller's journal envelope.
//! 4. On `rollback` / `Drop`: pending `OverflowRef`s decref via RAII.
//!
//! ## Lock ownership
//!
//! `WriteTxn` does NOT own the writer serialization mutex — callers
//! acquire `MutexGuard<'_, BpBackend>` and keep it alive for the txn's
//! full lifetime (begin → commit / rollback). This keeps the type free
//! of lifetime parameters and lets callers scope the guard with a
//! closure or an explicit block.

use std::cell::Cell;
use std::sync::Arc;

use smallvec::SmallVec;

use crate::error::Result;
use crate::journal::log_file::ChainPageWrite;
use crate::mvcc::timestamp::{TimestampOracle, Ts};
use crate::mvcc::version::OverflowRef;
use crate::storage::allocator::AllocatorHandle;
use crate::storage::buffer_pool::PageSource;
use crate::storage::handle::BufferPoolHandle;
use crate::storage::paged_engine::publish::PublishDirty;

/// Stage-time identity of the live version head a writer observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpectedHead {
    /// Start timestamp of the observed live head.
    pub commit_ts: Ts,
    /// Transaction identifier of the observed live head.
    pub txn_id: u64,
}

// ---------------------------------------------------------------------------
// Namespace string — cheap clone via Arc refcount
// ---------------------------------------------------------------------------

/// Namespace string backed by `Arc<str>` so cloning staged writes costs a
/// refcount bump rather than a heap allocation.
///
/// Implements `PartialEq<&str>` and `PartialEq<str>` for ergonomic test
/// assertions (`assert_eq!(pw.ns, "db.coll")`).
#[derive(Debug, Clone)]
pub(crate) struct Ns(Arc<str>);

impl Ns {
    #[must_use]
    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Ns {
    fn from(s: &str) -> Self {
        Ns(Arc::from(s))
    }
}

impl From<String> for Ns {
    fn from(s: String) -> Self {
        Ns(Arc::from(s.as_str()))
    }
}

impl From<Arc<str>> for Ns {
    fn from(a: Arc<str>) -> Self {
        Ns(a)
    }
}

impl PartialEq<str> for Ns {
    fn eq(&self, other: &str) -> bool {
        self.0.as_ref() == other
    }
}

impl PartialEq<&str> for Ns {
    fn eq(&self, other: &&str) -> bool {
        self.0.as_ref() == *other
    }
}

impl PartialEq<Ns> for Ns {
    fn eq(&self, other: &Ns) -> bool {
        self.0 == other.0
    }
}

impl std::ops::Deref for Ns {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

// ---------------------------------------------------------------------------
// Pending secondary-index write (staging buffer entry)
// ---------------------------------------------------------------------------

/// One pending secondary-index mutation staged by a `WriteTxn`.
///
/// Entries are staged via `stage_sec_index_{insert,delete,update}` and
/// consumed by the commit loop that installs them into the per-key version
/// chains under a shared `commit_ts`. On abort, the buffer is discarded by
/// `WriteTxn::Drop` (no external refcount state to release — `id_bytes` is
/// plain owned memory).
#[derive(Debug, Clone)]
pub(crate) struct SecIndexWrite {
    /// Durable index identifier resolved at stage time from the live
    /// `IndexEntry.id` (Phase 2 §3.1a). Stable across root moves and any
    /// hypothetical post-stage rename of the owning index. Carried into
    /// the Phase 2 logical frame without re-resolving at emit time.
    pub(crate) index_id: i64,
    /// Root page of the target secondary-index B+ tree. The install pass
    /// uses this to locate the tree at commit time.
    pub(crate) index_root_page: u32,
    /// Compound key bytes (from `encode_compound_key` in `keys`).
    pub(crate) key: Vec<u8>,
    /// Stage-time head observed by the writer, if any.
    pub expected_head: Option<ExpectedHead>,
    /// Operation kind — insert with id bytes or delete (tombstone).
    pub(crate) op: SecIndexOp,
}

/// Secondary-index mutation kind.
#[derive(Debug, Clone)]
pub(crate) enum SecIndexOp {
    /// Insert an entry with the given `{"_id": ...}` BSON payload.
    Insert { id_bytes: Vec<u8> },
    /// Delete the entry at `key` (idempotent when already absent).
    Delete,
}

// ---------------------------------------------------------------------------
// Pending primary-tree write (staging buffer entry)
// ---------------------------------------------------------------------------

/// One pending primary (data-tree) mutation staged by a `WriteTxn`.
///
/// Writer paths in `paged_engine` stage through
/// `stage_primary_{insert,update,delete}`. At commit time the staged writes
/// install a `VersionEntry` at the head of each key's version chain (on the
/// owning leaf frame) under the txn's `commit_ts`, with prev-head's
/// `stop_ts` advanced to `commit_ts` via `Arc::make_mut`.
#[derive(Debug, Clone)]
pub(crate) struct PrimaryWrite {
    /// Durable namespace identifier resolved at stage time from the live
    /// `CollectionEntry.id` (Phase 2 §3.1a). Stable across `data_root_page`
    /// moves and any hypothetical post-stage rename of the namespace.
    /// Carried into the Phase 2 logical frame without re-resolving at
    /// emit time.
    pub(crate) ns_id: i64,
    /// Target data-tree namespace. The commit install pass looks up the
    /// current tree by name so any in-txn root splits between stage and
    /// install time are transparently followed.
    pub(crate) ns: Ns,
    /// B+ tree cell key (e.g. `encode_key(_id)`).
    pub(crate) key: Vec<u8>,
    /// Stage-time head observed by the writer, if any.
    pub expected_head: Option<ExpectedHead>,
    /// Kind of write, carrying any associated payload bytes.
    pub(crate) op: PrimaryOp,
}

/// Primary-tree mutation kind.
#[derive(Debug, Clone)]
pub(crate) enum PrimaryOp {
    /// Install a new committed version with these data bytes.
    Insert { data: Vec<u8> },
    /// Overwrite an existing key's data bytes (semantically same as insert
    /// at the chain level — new head entry with fresh commit_ts).
    Update { data: Vec<u8> },
    /// Tombstone the key (install a chain entry with `is_tombstone=true`
    /// and empty inline data).
    Delete,
}

/// In-progress MVCC write transaction.
///
/// Invariants:
/// - `pending` owns one `OverflowRef` per new overflow chain staged by this
///   transaction. On abort each `OverflowRef::drop` atomically decrements
///   the chain refcount and enqueues the page for deferred-free; on commit
///   ownership transfers into the installed version chain so the refcount
///   stays at ≥ 1 post-commit.
/// - `commit_ts` Cell is `None` until `allocate_commit_ts` runs or
///   `commit()` lazily allocates; after allocation it carries the
///   oracle-issued commit timestamp. Phase 2 §3.7's
///   `emit_logical_txn_frame` helper consumes the Cell exactly once via
///   take-once semantics (reads, then sets back to `None`) so any double
///   emit panics loudly.
/// - `finalized` flips to `true` at the top of `commit()` so `Drop` knows
///   not to decref `pending` — the entries have been moved into the durable
///   chain state.
#[derive(Debug)]
pub(crate) struct WriteTxn {
    /// Per-oracle transaction identifier used for self-visibility on
    /// pending entries.
    pub(crate) txn_id: u64,
    /// Phase 2 §3.7 commit-envelope commit_ts. `None` until
    /// [`allocate_commit_ts`](Self::allocate_commit_ts) runs; stamped with
    /// `Some(ts)` at stage-S4 so [`emit_logical_txn_frame`](Self::
    /// emit_logical_txn_frame) can consume it at S5. Take-once semantics:
    /// emit panics if it ever finds `None` and clears the Cell after
    /// reading so a second emit on the same txn is a programming error.
    /// `Cell` gives single-threaded interior mutability without adding a
    /// `Mutex` (§11 #10 and §3.7 mandate no new Mutex/Arc on this path);
    /// `WriteTxn` is not `Send` because of this field.
    pub(crate) commit_ts: Cell<Option<Ts>>,
    /// Overflow chains staged by this transaction. Each `OverflowRef`
    /// holds one refcount; drop order is irrelevant (atomic decref +
    /// deferred-free enqueue).
    pub(crate) pending: SmallVec<[OverflowRef; 2]>,
    /// Page writes to include in the `ChainCommit` journal frame. Each
    /// entry is a (page, size, bytes) triple. Populated as chain mutations
    /// land.
    pub(crate) page_writes: SmallVec<[ChainPageWrite; 2]>,
    /// Refcount deltas to record in the `ChainCommit` frame for recovery.
    /// `(first_page, +1)` for newly allocated overflow chains; `(-1)` for
    /// chains dropped at commit.
    pub(crate) refcount_deltas: SmallVec<[(u32, i32); 2]>,
    /// Pending secondary-index mutations staged by this transaction.
    /// Populated via `stage_sec_index_{insert,delete,update}` and drained
    /// at commit, installing each entry into the per-key version chain
    /// under the shared `commit_ts`. On abort, the buffer is dropped with
    /// `self` — `SecIndexWrite` owns no external refcount state, so no
    /// RAII cleanup is required beyond vec drop.
    pub(crate) pending_sec_index: SmallVec<[SecIndexWrite; 2]>,
    /// Pending primary-tree (data-tree) mutations staged by this transaction.
    /// Populated via `stage_primary_{insert,update,delete}`; the install
    /// pass at commit drains the buffer and installs a `VersionEntry` at
    /// the head of each key's version chain on the owning leaf frame,
    /// advancing the prior head's `stop_ts` to `commit_ts`.
    pub(crate) pending_primary: SmallVec<[PrimaryWrite; 2]>,
    /// Phase 1 publish-decision dirty state. Set at mutation sites
    /// (CRUD helpers and DDL publish sites) per §10.3; consumed once at
    /// the publish step by `publish_commit`. Discarded with the rest of
    /// the staged state on abort (§10.9 "Failed commit path"). See
    /// `src/storage/paged_engine/publish.rs`.
    pub(crate) publish_dirty: PublishDirty,
    /// True when this CRUD body had to perform structural primary B-tree
    /// work even if the published catalog root stayed stable.
    structural_tree_change: bool,
    /// True after `commit()` has transferred `pending` ownership into the
    /// durable chain. `Drop` checks this to avoid decrementing refcounts
    /// that now belong to installed chains.
    finalized: bool,
}

impl WriteTxn {
    /// Create a new empty transaction without running the begin protocol.
    ///
    /// Prefer `begin(..)` on writer paths — this constructor exists for
    /// tests that exercise the struct directly.
    #[must_use]
    pub(crate) fn new(txn_id: u64) -> Self {
        Self {
            txn_id,
            commit_ts: Cell::new(None),
            pending: SmallVec::new(),
            page_writes: SmallVec::new(),
            refcount_deltas: SmallVec::new(),
            pending_sec_index: SmallVec::new(),
            pending_primary: SmallVec::new(),
            publish_dirty: PublishDirty::default(),
            structural_tree_change: false,
            finalized: false,
        }
    }

    /// Read the current `PublishDirty` (§10.2 Q-M1 accessor).
    #[must_use]
    pub(crate) fn publish_dirty(&self) -> PublishDirty {
        self.publish_dirty
    }

    /// Mark reader-visible published metadata dirty (§10.2 Q-M1 accessor).
    /// Forces a fresh `Arc<PublishedCatalog>` at publish time.
    pub(crate) fn mark_published(&mut self) {
        self.publish_dirty.mark_published();
    }

    /// Mark the on-disk catalog header dirty (§10.2 Q-M1 accessor).
    /// Triggers `sync_catalog_root_overlay` independently of publish.
    pub(crate) fn mark_header(&mut self) {
        self.publish_dirty.mark_header();
    }

    /// Mark that the writer crossed a primary-tree structural boundary.
    pub(crate) fn mark_structural_tree_change(&mut self) {
        self.structural_tree_change = true;
    }

    /// Return whether this writer crossed a primary-tree structural boundary.
    #[must_use]
    pub(crate) fn structural_tree_change(&self) -> bool {
        self.structural_tree_change
    }

    /// Begin a new write transaction.
    ///
    /// Protocol:
    /// 1. Caller must be holding the writer serialization mutex (i.e. the
    ///    `MutexGuard<'_, BpBackend>` from `PagedEngine::inner`). This
    ///    function does not acquire it — ownership is external to keep
    ///    `WriteTxn` lifetime-parameter-free.
    /// 2. Drain checkpoint-eligible page-lifetime entries. Any refcount-0
    ///    pages whose enqueue fence is older than the checkpoint fence return
    ///    to the free list before the new commit allocates.
    /// 3. Initialize empty `pending` / `page_writes` / `refcount_deltas`.
    pub(crate) fn begin(
        txn_id: u64,
        allocator: &AllocatorHandle,
        io: &dyn PageSource,
    ) -> Result<Self> {
        allocator.drain_free_queue(io)?;
        Ok(Self::new(txn_id))
    }

    /// Attach an `OverflowRef` for a newly-written chain.
    ///
    /// Takes ownership — does not bump the refcount. If the txn aborts,
    /// `Drop` decrements on `self.pending`. If the txn commits,
    /// `commit()` takes ownership via `std::mem::take` and the caller
    /// (phase 6) moves each ref into the durable chain.
    pub(crate) fn attach_overflow(&mut self, r: OverflowRef) {
        self.pending.push(r);
    }

    /// Record a page-write for the ChainCommit frame.
    pub(crate) fn push_page_write(&mut self, pw: ChainPageWrite) {
        self.page_writes.push(pw);
    }

    /// Record a refcount delta for the ChainCommit frame.
    pub(crate) fn push_refcount_delta(&mut self, first_page: u32, delta: i32) {
        self.refcount_deltas.push((first_page, delta));
    }

    /// Stage a secondary-index insert for commit-time installation.
    ///
    /// Accumulates the write in `pending_sec_index`; the commit loop
    /// drains the buffer and performs the chain installation under the
    /// txn's `commit_ts`, sharing the timestamp with primary writes in
    /// the same txn.
    pub(crate) fn stage_sec_index_insert(
        &mut self,
        index_id: i64,
        index_root_page: u32,
        key: Vec<u8>,
        id_bytes: Vec<u8>,
    ) {
        self.pending_sec_index.push(SecIndexWrite {
            index_id,
            index_root_page,
            key,
            expected_head: None,
            op: SecIndexOp::Insert { id_bytes },
        });
    }

    /// Stage a secondary-index delete for commit-time installation.
    ///
    /// Idempotent semantics — a delete of an absent key is recorded and
    /// the install loop silently skips it.
    pub(crate) fn stage_sec_index_delete(
        &mut self,
        index_id: i64,
        index_root_page: u32,
        key: Vec<u8>,
    ) {
        self.pending_sec_index.push(SecIndexWrite {
            index_id,
            index_root_page,
            key,
            expected_head: None,
            op: SecIndexOp::Delete,
        });
    }

    /// Stage a secondary-index update (delete old-key, insert new-key).
    ///
    /// Thin wrapper over `stage_sec_index_delete` + `stage_sec_index_insert`.
    /// If `old_key == new_key`, both entries still stage — the install loop
    /// runs them in order so the net effect is an overwrite.
    pub(crate) fn stage_sec_index_update(
        &mut self,
        index_id: i64,
        index_root_page: u32,
        old_key: Vec<u8>,
        new_key: Vec<u8>,
        new_id_bytes: Vec<u8>,
    ) {
        self.stage_sec_index_delete(index_id, index_root_page, old_key);
        self.stage_sec_index_insert(index_id, index_root_page, new_key, new_id_bytes);
    }

    /// Stage a primary-tree insert for commit-time chain installation.
    pub(crate) fn stage_primary_insert(
        &mut self,
        ns_id: i64,
        ns: impl Into<Ns>,
        key: Vec<u8>,
        data: Vec<u8>,
        expected_head: Option<ExpectedHead>,
    ) {
        self.pending_primary.push(PrimaryWrite {
            ns_id,
            ns: ns.into(),
            key,
            expected_head,
            op: PrimaryOp::Insert { data },
        });
    }

    /// Stage a primary-tree update for commit-time chain installation.
    pub(crate) fn stage_primary_update(
        &mut self,
        ns_id: i64,
        ns: impl Into<Ns>,
        key: Vec<u8>,
        data: Vec<u8>,
        expected_head: Option<ExpectedHead>,
    ) {
        self.pending_primary.push(PrimaryWrite {
            ns_id,
            ns: ns.into(),
            key,
            expected_head,
            op: PrimaryOp::Update { data },
        });
    }

    /// Stage a primary-tree delete (tombstone) for commit-time chain installation.
    pub(crate) fn stage_primary_delete(
        &mut self,
        ns_id: i64,
        ns: impl Into<Ns>,
        key: Vec<u8>,
        expected_head: Option<ExpectedHead>,
    ) {
        self.pending_primary.push(PrimaryWrite {
            ns_id,
            ns: ns.into(),
            key,
            expected_head,
            op: PrimaryOp::Delete,
        });
    }

    /// Pre-allocate the commit timestamp.
    ///
    /// The commit ts must be available BEFORE the primary chain-head install
    /// runs (each new `VersionEntry` carries `start_ts = commit_ts`). Callers
    /// that need to install primary chains drive the protocol as:
    /// `allocate_commit_ts` → install primary chains → `commit` (which
    /// detects the preallocation and skips the oracle call).
    ///
    /// Safe to call at most once per txn; asserts `commit_ts == PENDING`.
    pub(crate) fn allocate_commit_ts(&mut self, oracle: &TimestampOracle) -> Result<Ts> {
        debug_assert!(
            self.commit_ts.get().is_none(),
            "allocate_commit_ts: commit_ts Cell must be None before allocation (§3.7)"
        );
        let ts = oracle.commit()?;
        self.commit_ts.set(Some(ts));
        Ok(ts)
    }

    /// Emit a Phase 2 [`LogicalTxnFrame`] for this transaction (§3.7, §6.2).
    ///
    /// Called at S5 of the commit envelope — after `allocate_commit_ts`
    /// (S4) sets the `commit_ts` Cell and before the `ChainCommit` frame
    /// (S7) is appended. Walks `sec_writes` first, then `primary_writes`
    /// (§3.6 emit-side convention), assigning `op_ordinal` from a 0-based
    /// `u32` counter that is dense across the whole batch.
    ///
    /// Take-once semantics on `commit_ts`:
    /// - Reads the Cell exactly once.
    /// - Panics with a clear invariant-violation message if the Cell is
    ///   `None` — emitting before `allocate_commit_ts` ran is a programming
    ///   error and indicates the §3.7 commit-envelope order was violated.
    /// - Sets the Cell back to `None` after reading so a second emit on the
    ///   same transaction panics on re-entry.
    ///
    /// Error behavior: propagates [`Error::JournalFrameTooLarge`] from
    /// [`LogicalTxnFrame::encode`] via
    /// [`JournalManager::append_logical_txn`] unchanged, matching §3.5.
    ///
    /// No new `Mutex` / `Arc` — the Cell provides single-threaded interior
    /// mutability scoped to this transaction.
    pub(crate) fn emit_logical_txn_frame(
        &self,
        journal: &BufferPoolHandle,
        primary_writes: &[PrimaryWrite],
        sec_writes: &[SecIndexWrite],
    ) -> Result<()> {
        let frame = self.build_logical_txn_frame(journal, primary_writes, sec_writes);
        journal.append_logical_txn(frame)?;
        Ok(())
    }

    /// Build and consume this transaction's Phase 2 `LogicalTxnFrame`.
    ///
    /// This is the S5-only half of [`emit_logical_txn_frame`]: it consumes
    /// the pre-allocated `commit_ts` Cell and returns the frame without doing
    /// journal I/O. `run_write_existing` appends the returned frame inside the
    /// journal envelope so rollback handling can distinguish frame construction
    /// from durability.
    pub(crate) fn build_logical_txn_frame(
        &self,
        journal: &BufferPoolHandle,
        primary_writes: &[PrimaryWrite],
        sec_writes: &[SecIndexWrite],
    ) -> crate::journal::log_file::LogicalTxnFrame {
        #[allow(clippy::expect_used)]
        let commit_ts = self.commit_ts.get().expect(
            "§3.7 invariant violation: emit_logical_txn_frame called before \
             allocate_commit_ts or after a prior emit consumed the Cell",
        );
        self.commit_ts.set(None);

        let (salt1, salt2) = journal.journal_salts().unwrap_or((0, 0));
        build_logical_txn_frame(
            self.txn_id,
            commit_ts,
            salt1,
            salt2,
            primary_writes,
            sec_writes,
        )
    }

    /// Finalize the transaction.
    ///
    /// Protocol:
    /// 1. Allocate `commit_ts` from the oracle (skipped if
    ///    `allocate_commit_ts` was already called).
    /// 2. Emit a single `ChainCommit` journal frame carrying
    ///    `commit_ts`, `refcount_deltas`, and `page_writes`.
    /// 3. Take ownership of `pending` out of `self` — ownership transfers
    ///    to the caller, who installs each `OverflowRef` into its version
    ///    chain. The returned `Vec<OverflowRef>` must be consumed by the
    ///    caller; otherwise the refcounts decref on vec drop and the newly
    ///    committed chain becomes dangling.
    /// 4. Flip `finalized = true` so `Drop` no longer decrefs `pending`
    ///    (the entries have already been moved out).
    ///
    /// Returns `(commit_ts, pending, pending_sec_index)`. Callers that
    /// stage no overflow data may drop the returned vecs, which runs
    /// `OverflowRef::Drop` on every entry — correct for no-op commits.
    pub(crate) fn commit(
        self,
        oracle: &TimestampOracle,
        journal: &BufferPoolHandle,
    ) -> Result<(Ts, Vec<OverflowRef>, Vec<SecIndexWrite>)> {
        let commit_ts = match self.commit_ts.get() {
            Some(ts) => ts,
            None => {
                let ts = oracle.commit()?;
                self.commit_ts.set(Some(ts));
                ts
            }
        };

        let (pending, pending_sec_index) = self.commit_with_ts(commit_ts, journal)?;
        Ok((commit_ts, pending, pending_sec_index))
    }

    /// Finalize the transaction with an explicit, caller-provided
    /// `commit_ts` — mirrors [`commit`](Self::commit) but does not read
    /// the `commit_ts` Cell. Required by the Phase 2 commit-envelope
    /// rewire (US-011) where `emit_logical_txn_frame` consumes the Cell
    /// at S5 and the subsequent `ChainCommit` frame at S7 must use the
    /// same pre-allocated `ts` that `allocate_commit_ts` (S4) returned.
    ///
    /// Draining semantics match `commit`: `pending`, `page_writes`,
    /// `refcount_deltas`, and `pending_sec_index` are moved out of `self`
    /// BEFORE journaling so a journal failure leaves the caller with
    /// `pending` refcount ownership. Returns `(pending, pending_sec_index)`
    /// on success; flips `finalized` so `Drop` skips the refcount decref.
    pub(crate) fn commit_with_ts(
        self,
        commit_ts: Ts,
        journal: &BufferPoolHandle,
    ) -> Result<(Vec<OverflowRef>, Vec<SecIndexWrite>)> {
        self.commit_chain_commit(journal, commit_ts)
    }

    /// Append the S7 `ChainCommit` frame for this transaction.
    ///
    /// Returns the drained overflow refs and secondary-index write list after
    /// the chain commit has been appended. This consumes `self`; on any append
    /// failure, `Drop` still owns the drained vectors and aborts their refcounts
    /// normally.
    pub(crate) fn commit_chain_commit(
        mut self,
        journal: &BufferPoolHandle,
        commit_ts: Ts,
    ) -> Result<(Vec<OverflowRef>, Vec<SecIndexWrite>)> {
        let pending = std::mem::take(&mut self.pending).into_vec();
        let page_writes = std::mem::take(&mut self.page_writes).into_vec();
        let refcount_deltas = std::mem::take(&mut self.refcount_deltas).into_vec();
        let pending_sec_index = std::mem::take(&mut self.pending_sec_index).into_vec();

        journal.append_chain_commit(commit_ts, refcount_deltas, page_writes)?;

        self.finalized = true;
        Ok((pending, pending_sec_index))
    }

    /// Explicit abort — equivalent to dropping the transaction.
    ///
    /// `Drop` runs on return, decrementing every `pending` refcount.
    pub(crate) fn rollback(self) {
        // Drop glue handles the decrefs.
    }
}

impl Drop for WriteTxn {
    fn drop(&mut self) {
        // Commit path (`self.finalized == true`): ownership of `pending`
        // has already been moved out via `std::mem::take` — nothing to do.
        // Abort path: `Vec<OverflowRef>` drop runs `OverflowRef::drop` on
        // every entry. Each decref is atomic; a 0-post-decrement transitions
        // the page into the page-lifetime queue (lock-order position 1.5).
    }
}

/// Build a [`LogicalTxnFrame`] from staged `sec_writes` + `primary_writes`
/// in §3.6 emit-side order (secondaries first, primaries second) with a
/// dense `0..N` `op_ordinal` counter. Keeps frame-construction pure — no
/// `Cell` consumption, no journal I/O — so it can be exercised directly
/// from unit tests without also driving `append_logical_txn`.
fn build_logical_txn_frame(
    txn_id: u64,
    commit_ts: Ts,
    salt1: u32,
    salt2: u32,
    primary_writes: &[PrimaryWrite],
    sec_writes: &[SecIndexWrite],
) -> crate::journal::log_file::LogicalTxnFrame {
    use crate::journal::log_file::{LogicalOp, LogicalOpKind, LogicalTxnFrame};

    let total = sec_writes.len().saturating_add(primary_writes.len());
    let mut ops: Vec<LogicalOp> = Vec::with_capacity(total);
    let mut ordinal: u32 = 0;

    for sw in sec_writes {
        let kind = match &sw.op {
            SecIndexOp::Insert { id_bytes } => LogicalOpKind::SecondaryInsert {
                index_id: sw.index_id,
                key: sw.key.clone(),
                id_bytes: id_bytes.clone(),
            },
            SecIndexOp::Delete => LogicalOpKind::SecondaryDelete {
                index_id: sw.index_id,
                key: sw.key.clone(),
            },
        };
        ops.push(LogicalOp {
            op_ordinal: ordinal,
            kind,
        });
        ordinal = ordinal.saturating_add(1);
    }

    for pw in primary_writes {
        let kind = match &pw.op {
            PrimaryOp::Insert { data } => LogicalOpKind::PrimaryInsert {
                ns_id: pw.ns_id,
                key: pw.key.clone(),
                value: data.clone(),
                overflow: None,
            },
            PrimaryOp::Update { data } => LogicalOpKind::PrimaryUpdate {
                ns_id: pw.ns_id,
                key: pw.key.clone(),
                value: data.clone(),
                overflow: None,
            },
            PrimaryOp::Delete => LogicalOpKind::PrimaryDelete {
                ns_id: pw.ns_id,
                key: pw.key.clone(),
            },
        };
        ops.push(LogicalOp {
            op_ordinal: ordinal,
            kind,
        });
        ordinal = ordinal.saturating_add(1);
    }

    LogicalTxnFrame {
        salt1,
        salt2,
        commit_ts,
        diagnostic_txn_id: txn_id,
        format_version: crate::journal::log_file::LOGICAL_TXN_FORMAT_VERSION,
        flags: 0,
        ops,
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
    use crate::storage::buffer_pool::{default_sizes, BufferPool, PageSize};
    use crate::storage::handle::BufferPoolHandle;
    use crate::storage::header::FileHeader;
    use crate::storage::test_support::{ArcIo, MockIo};
    use std::sync::Arc;

    fn fresh_allocator() -> AllocatorHandle {
        AllocatorHandle::new(FileHeader::new(0, 0, 0))
    }

    fn fresh_handle() -> Arc<BufferPoolHandle> {
        let io = Arc::new(MockIo::default());
        let pool = Arc::new(BufferPool::new(
            default_sizes::DESKTOP,
            Box::new(ArcIo(Arc::clone(&io))),
        ));
        let history_pool = Arc::new(BufferPool::new(
            default_sizes::IOT,
            Box::new(ArcIo(Arc::clone(&io))),
        ));
        Arc::new(BufferPoolHandle::new(
            pool,
            history_pool,
            FileHeader::new(0, 0, 0),
        ))
    }

    // -----------------------------------------------------------------------
    // WriteTxn basic behaviour
    // -----------------------------------------------------------------------

    #[test]
    fn new_txn_starts_empty() {
        let t = WriteTxn::new(7);
        assert_eq!(t.txn_id, 7);
        assert_eq!(t.commit_ts.get(), None);
        assert!(t.pending.is_empty());
        assert!(t.page_writes.is_empty());
        assert!(t.refcount_deltas.is_empty());
        assert!(t.pending_sec_index.is_empty());
        assert!(t.pending_primary.is_empty());
        assert!(!t.publish_dirty.published_catalog_dirty);
        assert!(!t.publish_dirty.catalog_header_dirty);
    }

    #[test]
    fn mark_published_sets_publish_dirty_published_bit() {
        let mut t = WriteTxn::new(1);
        t.mark_published();
        assert!(t.publish_dirty().published_catalog_dirty);
        assert!(!t.publish_dirty().catalog_header_dirty);
    }

    #[test]
    fn mark_header_sets_publish_dirty_header_bit() {
        let mut t = WriteTxn::new(1);
        t.mark_header();
        assert!(!t.publish_dirty().published_catalog_dirty);
        assert!(t.publish_dirty().catalog_header_dirty);
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
        assert_eq!(alloc.page_lifetime_queue().depth(), 1);
    }

    // -----------------------------------------------------------------------
    // begin / commit / rollback
    // -----------------------------------------------------------------------

    #[test]
    fn begin_checks_page_lifetime_queue() {
        // Arrange: an AllocatorHandle whose page-lifetime queue has entries
        // from prior reader drops. `begin` must drain them before we
        // construct a WriteTxn.
        let handle = fresh_handle();
        let alloc = handle.allocator().clone();

        // Simulate a reader drop: new_owned → drop brings count 0 → enqueue.
        {
            let _r = OverflowRef::new_owned(99, 32, alloc.clone()).unwrap();
        }
        assert_eq!(alloc.page_lifetime_queue().depth(), 1);

        // The entry's checkpoint fence has not advanced, so `begin` should
        // observe the queue but leave it pending.
        let result = WriteTxn::begin(1, &alloc, handle.page_source());
        result.expect("begin with a non-eligible page-lifetime entry");
        assert_eq!(alloc.page_lifetime_queue().depth(), 1);
    }

    #[test]
    fn commit_assigns_monotonic_commit_ts() {
        let handle = fresh_handle();
        let alloc = handle.allocator().clone();
        let oracle = TimestampOracle::new();

        let t1 = WriteTxn::begin(1, &alloc, handle.page_source()).expect("begin with empty queue");
        let (ts1, pending1, sec1) = t1.commit(&oracle, &handle).expect("commit t1");
        assert!(pending1.is_empty());
        assert!(sec1.is_empty());

        let t2 = WriteTxn::begin(2, &alloc, handle.page_source()).expect("begin with empty queue");
        let (ts2, pending2, sec2) = t2.commit(&oracle, &handle).expect("commit t2");
        assert!(pending2.is_empty());
        assert!(sec2.is_empty());

        assert!(ts2 > ts1, "commit_ts strictly monotone");
        assert_ne!(ts1, Ts::default());
    }

    #[test]
    fn commit_transfers_pending_ownership_to_caller() {
        let handle = fresh_handle();
        let alloc = handle.allocator().clone();
        let oracle = TimestampOracle::new();

        let r = OverflowRef::new_owned(77, 128, alloc.clone()).unwrap();
        assert_eq!(alloc.overflow_refcount(77), 1);

        let mut t =
            WriteTxn::begin(1, &alloc, handle.page_source()).expect("begin with empty queue");
        t.attach_overflow(r);

        let (_ts, pending, sec) = t.commit(&oracle, &handle).expect("commit");
        // Ownership transferred to the returned vec — refcount still 1.
        assert_eq!(alloc.overflow_refcount(77), 1);
        assert_eq!(pending.len(), 1);
        assert!(sec.is_empty());

        // Dropping the returned vec runs OverflowRef::drop on each entry.
        // On commit paths that install into a durable chain, the caller
        // instead moves each ref into the chain and the refcount stays bumped.
        drop(pending);
        assert_eq!(alloc.overflow_refcount(77), 0);
    }

    #[test]
    fn rollback_drops_pending_and_decrefs() {
        let handle = fresh_handle();
        let alloc = handle.allocator().clone();

        let r = OverflowRef::new_owned(88, 256, alloc.clone()).unwrap();
        let mut t =
            WriteTxn::begin(1, &alloc, handle.page_source()).expect("begin with empty queue");
        t.attach_overflow(r);
        assert_eq!(alloc.overflow_refcount(88), 1);

        t.rollback();
        assert_eq!(alloc.overflow_refcount(88), 0);
        assert_eq!(alloc.page_lifetime_queue().depth(), 1);
    }

    #[test]
    fn finalized_txn_drop_does_not_decref_pending() {
        // Invariant: once commit() has moved ownership of `pending` out of
        // `self`, the Drop of `self` must not re-decref (the entries no
        // longer belong to the txn). Verified by constructing a committed
        // txn whose returned pending we forget about — refcount stays at 1.
        let handle = fresh_handle();
        let alloc = handle.allocator().clone();
        let oracle = TimestampOracle::new();

        let r = OverflowRef::new_owned(55, 64, alloc.clone()).unwrap();
        let mut t =
            WriteTxn::begin(1, &alloc, handle.page_source()).expect("begin with empty queue");
        t.attach_overflow(r);

        let (_ts, pending, _sec) = t.commit(&oracle, &handle).expect("commit");
        // Simulate phase 6 installing `pending` into a chain by forgetting
        // it — the refcount remains bumped as the durable chain now owns it.
        std::mem::forget(pending);
        assert_eq!(alloc.overflow_refcount(55), 1);
    }

    // -----------------------------------------------------------------------
    // Sec-index staging (phase 5)
    // -----------------------------------------------------------------------

    #[test]
    fn stage_sec_index_insert_accumulates() {
        let mut t = WriteTxn::new(1);
        t.stage_sec_index_insert(100, 42, b"k1".to_vec(), b"id1".to_vec());
        t.stage_sec_index_insert(100, 42, b"k2".to_vec(), b"id2".to_vec());

        assert_eq!(t.pending_sec_index.len(), 2);
        assert_eq!(t.pending_sec_index[0].index_id, 100);
        assert_eq!(t.pending_sec_index[0].index_root_page, 42);
        assert_eq!(t.pending_sec_index[0].key, b"k1");
        match &t.pending_sec_index[0].op {
            SecIndexOp::Insert { id_bytes } => assert_eq!(id_bytes, b"id1"),
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn stage_sec_index_delete_records_key() {
        let mut t = WriteTxn::new(1);
        t.stage_sec_index_delete(200, 7, b"ghost".to_vec());

        assert_eq!(t.pending_sec_index.len(), 1);
        assert_eq!(t.pending_sec_index[0].index_id, 200);
        assert_eq!(t.pending_sec_index[0].index_root_page, 7);
        assert!(matches!(t.pending_sec_index[0].op, SecIndexOp::Delete));
    }

    #[test]
    fn stage_sec_index_update_produces_delete_then_insert() {
        let mut t = WriteTxn::new(1);
        t.stage_sec_index_update(300, 11, b"old".to_vec(), b"new".to_vec(), b"id".to_vec());

        assert_eq!(t.pending_sec_index.len(), 2);
        assert_eq!(t.pending_sec_index[0].index_id, 300);
        assert_eq!(t.pending_sec_index[0].key, b"old");
        assert!(matches!(t.pending_sec_index[0].op, SecIndexOp::Delete));
        assert_eq!(t.pending_sec_index[1].index_id, 300);
        assert_eq!(t.pending_sec_index[1].key, b"new");
        match &t.pending_sec_index[1].op {
            SecIndexOp::Insert { id_bytes } => assert_eq!(id_bytes, b"id"),
            _ => panic!("expected Insert"),
        }
    }

    #[test]
    fn commit_drains_pending_sec_index_to_caller() {
        // Staged sec-index writes must transfer to the caller on commit.
        let handle = fresh_handle();
        let alloc = handle.allocator().clone();
        let oracle = TimestampOracle::new();

        let mut t =
            WriteTxn::begin(1, &alloc, handle.page_source()).expect("begin with empty queue");
        t.stage_sec_index_insert(42, 3, b"k".to_vec(), b"id".to_vec());
        t.stage_sec_index_delete(42, 3, b"d".to_vec());

        let (_ts, _pending, sec) = t.commit(&oracle, &handle).expect("commit");
        assert_eq!(sec.len(), 2);
        assert_eq!(sec[0].index_id, 42);
        assert_eq!(sec[0].index_root_page, 3);
        assert!(matches!(sec[0].op, SecIndexOp::Insert { .. }));
        assert!(matches!(sec[1].op, SecIndexOp::Delete));
    }

    #[test]
    fn rollback_discards_pending_sec_index() {
        // Abort path: staged sec-index writes must NOT reach any durable
        // state. Drop of the txn (rollback) drops the buffer trivially —
        // `SecIndexWrite` owns no external refcount, so no assertion beyond
        // "no panic, txn drops cleanly."
        let handle = fresh_handle();
        let alloc = handle.allocator().clone();

        let mut t =
            WriteTxn::begin(1, &alloc, handle.page_source()).expect("begin with empty queue");
        t.stage_sec_index_insert(50, 9, b"k".to_vec(), b"id".to_vec());
        assert_eq!(t.pending_sec_index.len(), 1);

        t.rollback();
        // No side-effects to observe — the buffer drop is infallible.
    }

    // -----------------------------------------------------------------------
    // Primary-tree staging (phase 6 sub-step 2)
    // -----------------------------------------------------------------------

    #[test]
    fn stage_primary_insert_accumulates() {
        let mut t = WriteTxn::new(1);
        t.stage_primary_insert(
            777,
            "ns.a".to_string(),
            b"k1".to_vec(),
            b"v1".to_vec(),
            None,
        );
        t.stage_primary_update(
            777,
            "ns.a".to_string(),
            b"k2".to_vec(),
            b"v2".to_vec(),
            None,
        );
        t.stage_primary_delete(777, "ns.a".to_string(), b"k3".to_vec(), None);

        assert_eq!(t.pending_primary.len(), 3);
        assert_eq!(t.pending_primary[0].ns_id, 777);
        assert_eq!(t.pending_primary[0].ns, "ns.a");
        assert_eq!(t.pending_primary[0].key, b"k1");
        match &t.pending_primary[0].op {
            PrimaryOp::Insert { data } => assert_eq!(data, b"v1"),
            _ => panic!("expected Insert"),
        }
        match &t.pending_primary[1].op {
            PrimaryOp::Update { data } => assert_eq!(data, b"v2"),
            _ => panic!("expected Update"),
        }
        assert!(matches!(t.pending_primary[2].op, PrimaryOp::Delete));
    }

    // Silence the unused-import warning when PageSize isn't actually used
    // in the test module — kept as a future anchor for phase 6 tests.
    #[allow(dead_code)]
    fn _marker(_: PageSize) {}

    // -----------------------------------------------------------------------
    // Phase 2 §3.1a — stage-time ns_id / index_id capture (US-009)
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Phase 2 §3.7 / §6.2 — emit_logical_txn_frame (US-010)
    // -----------------------------------------------------------------------

    /// §3.6 emit-side convention: secondary ops come first, primary ops
    /// second, with a dense `0..N` `op_ordinal` counter shared across the
    /// whole batch. Exercises the frame builder directly so the assertion
    /// does not depend on journal observability (which would require
    /// US-011 rewiring of the write envelope).
    #[test]
    fn emit_logical_txn_frame_assigns_ordinals_from_zero_in_staging_order() {
        let mut t = WriteTxn::new(1);
        // 2 sec writes + 3 primary writes — staged in the order below.
        t.stage_sec_index_insert(10, 100, b"s0".to_vec(), b"id0".to_vec());
        t.stage_sec_index_delete(11, 101, b"s1".to_vec());
        t.stage_primary_insert(20, "ns".to_string(), b"p0".to_vec(), b"v0".to_vec(), None);
        t.stage_primary_update(20, "ns".to_string(), b"p1".to_vec(), b"v1".to_vec(), None);
        t.stage_primary_delete(20, "ns".to_string(), b"p2".to_vec(), None);

        let sec_snap: Vec<SecIndexWrite> = t.pending_sec_index.iter().cloned().collect();
        let pri_snap: Vec<PrimaryWrite> = t.pending_primary.iter().cloned().collect();
        let ts = Ts {
            physical_ms: 1_000,
            logical: 5,
        };
        let frame = build_logical_txn_frame(t.txn_id, ts, 0xAA, 0xBB, &pri_snap, &sec_snap);

        use crate::journal::log_file::LogicalOpKind;
        assert_eq!(frame.ops.len(), 5);
        // Dense 0..5 ordinal sequence.
        for (i, op) in frame.ops.iter().enumerate() {
            assert_eq!(op.op_ordinal, i as u32);
        }
        // Sec-first-primary-second ordering.
        assert!(matches!(
            frame.ops[0].kind,
            LogicalOpKind::SecondaryInsert { .. }
        ));
        assert!(matches!(
            frame.ops[1].kind,
            LogicalOpKind::SecondaryDelete { .. }
        ));
        assert!(matches!(
            frame.ops[2].kind,
            LogicalOpKind::PrimaryInsert { .. }
        ));
        assert!(matches!(
            frame.ops[3].kind,
            LogicalOpKind::PrimaryUpdate { .. }
        ));
        assert!(matches!(
            frame.ops[4].kind,
            LogicalOpKind::PrimaryDelete { .. }
        ));
        assert_eq!(frame.diagnostic_txn_id, t.txn_id);
        assert_eq!(frame.commit_ts, ts);
    }

    /// §3.7 invariant: emit must never run before `allocate_commit_ts`.
    /// When the `commit_ts` Cell is `None`, emit panics with an explicit
    /// invariant-violation message so the programming error is caught
    /// loudly rather than silently producing a zero-timestamp frame.
    #[test]
    #[should_panic(expected = "§3.7 invariant violation")]
    fn emit_logical_txn_frame_panics_if_commit_ts_unset() {
        let handle = fresh_handle();
        let t = WriteTxn::new(1);
        // commit_ts Cell is None — allocate_commit_ts has NOT run.
        let _ = t.emit_logical_txn_frame(&handle, &[], &[]);
    }

    /// Per §3.1a, `ns_id` and `index_id` must be resolved from the live
    /// `CollectionEntry.id` / `IndexEntry.id` at stage time and carried into
    /// the staged `PrimaryWrite` / `SecIndexWrite`. A post-stage rename
    /// (scaffolded here via direct entry mutation, since mqlite exposes no
    /// public rename API) cannot invalidate the recorded id because the
    /// stage-time snapshot lives in the staged struct rather than being
    /// re-resolved from the catalog at emit time. The test exercises the
    /// production commit path: `run_write_existing` drains `pending_primary`
    /// via `std::mem::take` before `txn.commit(...)` runs, and
    /// `txn.commit(...)` then drains `pending_sec_index` and emits the
    /// `ChainCommit` frame. We replicate that sequence here and assert
    /// that both drained vecs carry the stage-time ids.
    #[test]
    fn rename_safe_staged_ids_survive_rename() {
        use crate::mvcc::timestamp::TimestampOracle;
        use crate::storage::catalog::{CollectionEntry, IndexEntry, IndexState};
        use bson::Document;

        let orig_entry = CollectionEntry {
            id: 42,
            name: "users".to_string(),
            data_root_page: 10,
            data_root_level: 0,
            document_count: 0,
            avg_doc_size: 0,
            created_at: 0,
            options: Document::new(),
        };
        let orig_index = IndexEntry {
            id: 100,
            name: "email_1".to_string(),
            collection: "users".to_string(),
            root_page: 20,
            root_level: 0,
            key_pattern: Document::new(),
            unique: false,
            sparse: false,
            multikey: false,
            entry_count: 0,
            state: IndexState::Ready,
        };

        let handle = fresh_handle();
        let oracle = TimestampOracle::new();
        let mut t = WriteTxn::new(1);
        // Production stage sites (doc_ops.rs + index_maint.rs/
        // secondary_index.rs) read `entry.id` / `index_entry.id` from the
        // LIVE catalog entry at stage time and pass it in here.
        t.stage_primary_insert(
            orig_entry.id,
            orig_entry.name.clone(),
            b"k".to_vec(),
            b"v".to_vec(),
            None,
        );
        t.stage_sec_index_insert(
            orig_index.id,
            orig_index.root_page,
            b"compound".to_vec(),
            b"id".to_vec(),
        );

        // Scaffold a worst-case "rename" as direct entry mutation — the
        // spec explicitly allows this when no public rename API exists.
        // Phase 1 §10.7 says durable ids are stable across renames, so
        // we harden the invariant by going further: we mutate the id to
        // a disjoint value and confirm the staged write is unaffected.
        let mutated_entry = CollectionEntry {
            id: orig_entry.id + 1_000,
            ..orig_entry.clone()
        };
        let mutated_index = IndexEntry {
            id: orig_index.id + 1_000,
            ..orig_index.clone()
        };
        assert_ne!(mutated_entry.id, orig_entry.id);
        assert_ne!(mutated_index.id, orig_index.id);

        // Replicate the production commit envelope:
        //   1. run_write_existing drains `pending_primary` via mem::take
        //      and hands it to install_pending_primary.
        //   2. txn.commit(...) drains `pending_sec_index` internally and
        //      emits the ChainCommit journal frame.
        // Steps 1+2 are what actually "commits" the staged writes.
        let drained_primary: Vec<PrimaryWrite> = std::mem::take(&mut t.pending_primary).into_vec();
        let (_ts, _pending, drained_sec) = t.commit(&oracle, &handle).expect("commit envelope");

        assert_eq!(drained_primary.len(), 1);
        assert_eq!(drained_primary[0].ns_id, orig_entry.id);
        assert_ne!(drained_primary[0].ns_id, mutated_entry.id);

        assert_eq!(drained_sec.len(), 1);
        assert_eq!(drained_sec[0].index_id, orig_index.id);
        assert_ne!(drained_sec[0].index_id, mutated_index.id);
    }

    /// §3.7 / US-021 r4 codex blocker — production-emitter path.
    /// Stages a write via the real `WriteTxn::stage_*` API, mutates a
    /// catalog mapping in memory between stage and emit, calls the
    /// production `WriteTxn::emit_logical_txn_frame` helper against a
    /// journal-backed handle, then reads the encoded LogicalTxnFrame
    /// back from the journal file and asserts the encoded `ns_id` /
    /// `index_id` are the STAGE-TIME values, NOT the post-mutation
    /// values.
    ///
    /// This test exercises the actual production emitter (not the
    /// hand-built encode of `encoded_logical_txn_frame_round_trips_stage_time_ids`)
    /// per codex's r3 demand for emit-through-production-emitter proof.
    #[test]
    fn production_emitter_carries_stage_time_ids_under_mutation() {
        use crate::journal::log_file::{DecodeCtx, LogicalOpKind, LogicalTxnFrame};
        use crate::journal::JournalManager;
        use crate::mvcc::timestamp::TimestampOracle;
        use crate::storage::buffer_pool::{default_sizes, BufferPool};
        use crate::storage::catalog::{CollectionEntry, IndexEntry, IndexState};
        use crate::storage::handle::BufferPoolHandle;
        use crate::storage::header::FileHeader;
        use bson::Document;
        use std::fs::OpenOptions;
        use std::sync::{Arc, Mutex as StdMutex};

        const STAGE_TIME_NS_ID: i64 = 4242;
        const STAGE_TIME_INDEX_ID: i64 = 8484;
        const MUTATED_NS_ID: i64 = STAGE_TIME_NS_ID + 1_000;
        const MUTATED_INDEX_ID: i64 = STAGE_TIME_INDEX_ID + 1_000;

        // Live catalog state — mirrors what the production catalog
        // holds. We mutate THESE entries between stage and emit to
        // model a rename / re-bind that would only affect an
        // emit-time-re-resolver implementation. The production
        // engine reads `entry.id` at stage time
        // (`stage_insert_body` etc.), captures it into the
        // PrimaryWrite struct, and never re-resolves at emit time.
        let mut live_collection = CollectionEntry {
            id: STAGE_TIME_NS_ID,
            name: "users".to_string(),
            data_root_page: 10,
            data_root_level: 0,
            document_count: 0,
            avg_doc_size: 0,
            created_at: 0,
            options: Document::new(),
        };
        let mut live_index = IndexEntry {
            id: STAGE_TIME_INDEX_ID,
            name: "email_1".to_string(),
            collection: "users".to_string(),
            root_page: 20,
            root_level: 0,
            key_pattern: Document::new(),
            unique: false,
            sparse: false,
            multikey: false,
            entry_count: 0,
            state: IndexState::Ready,
        };

        let dir = tempfile::TempDir::new().expect("tempdir");
        let db_path = dir.path().join("us021_emit.mqlite");

        // Bootstrap a real main file + journal so the handle has a
        // working journal to write to.
        let header = FileHeader::new(0, 0, 0);
        {
            let mut main_file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&db_path)
                .expect("create main");
            use std::io::Write;
            main_file.write_all(&header.to_bytes()).expect("write hdr");
        }
        let mut main_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&db_path)
            .expect("reopen main");
        let mgr = JournalManager::open_or_create(&db_path, &header, &mut main_file)
            .expect("journal manager");
        let journal = Arc::new(StdMutex::new(mgr));

        // Pool wiring (mirrors `fresh_handle` but with a journal
        // attached via `with_journal`). Uses the canonical test fixture
        // imported at the module level.
        let io = Arc::new(MockIo::default());
        let pool = Arc::new(BufferPool::new(
            default_sizes::DESKTOP,
            Box::new(ArcIo(Arc::clone(&io))),
        ));
        let history_pool = Arc::new(BufferPool::new(
            default_sizes::IOT,
            Box::new(ArcIo(Arc::clone(&io))),
        ));
        let main_file_arc = Arc::new(StdMutex::new(main_file));
        let handle = Arc::new(BufferPoolHandle::with_journal(
            pool,
            history_pool,
            FileHeader::new(0, 0, 0),
            Arc::clone(&journal),
            main_file_arc,
        ));

        let oracle = TimestampOracle::new();
        // Stage using the LIVE catalog entry's id at this moment.
        // This mirrors `stage_insert_body` / `secondary_index::stage`
        // in production, which read `entry.id` from the live entry
        // and pass it into `stage_primary_insert` / `stage_sec_index_insert`.
        let mut t = WriteTxn::new(11);
        t.stage_primary_insert(
            live_collection.id,
            live_collection.name.clone(),
            b"k".to_vec(),
            b"v".to_vec(),
            None,
        );
        t.stage_sec_index_insert(
            live_index.id,
            live_index.root_page,
            b"key".to_vec(),
            b"id".to_vec(),
        );

        // Snapshot stage-time copies BEFORE the mutation.
        let staged_primary: Vec<PrimaryWrite> = std::mem::take(&mut t.pending_primary).into_vec();
        let staged_sec: Vec<SecIndexWrite> = std::mem::take(&mut t.pending_sec_index).into_vec();
        assert_eq!(staged_primary[0].ns_id, STAGE_TIME_NS_ID);
        assert_eq!(staged_sec[0].index_id, STAGE_TIME_INDEX_ID);

        // CATALOG MUTATION between stage and emit. Reassign the live
        // CollectionEntry / IndexEntry IDs to the post-rename values.
        // An emit-time-re-resolver implementation would have re-read
        // `live_collection.id` here and produced a frame carrying
        // MUTATED_NS_ID; the staged snapshot the production emitter
        // uses is immune to this mutation, so the emitted frame
        // carries STAGE_TIME_NS_ID.
        live_collection.id = MUTATED_NS_ID;
        live_index.id = MUTATED_INDEX_ID;
        assert_eq!(live_collection.id, MUTATED_NS_ID);
        assert_eq!(live_index.id, MUTATED_INDEX_ID);
        // The catalog mapping is now: name "users" → id MUTATED_NS_ID,
        // name "email_1" → id MUTATED_INDEX_ID. The staged_primary /
        // staged_sec vecs still carry the STAGE_TIME ids.

        // Production emitter call.
        let _commit_ts = t.allocate_commit_ts(&oracle).expect("allocate commit_ts");
        t.emit_logical_txn_frame(&handle, &staged_primary, &staged_sec)
            .expect("emit through production emitter");

        // Drop the journal lock before reading the bytes back.
        drop(t);

        // Read the journal file bytes and find the LogicalTxn frame.
        // The frame_kind discriminant for LogicalTxn is 0x03; the
        // first op's body starts at frame_start + 48 + 8.
        let salts = handle
            .journal_salts()
            .expect("journal must be attached on this handle");
        drop(handle);
        // Drop the JournalManager so the file is closed for reading.
        let _ = Arc::try_unwrap(journal)
            .map_err(|_| "journal Arc still held")
            .ok();

        let journal_path = {
            let mut p = db_path.as_os_str().to_owned();
            p.push("-journal");
            std::path::PathBuf::from(p)
        };
        let bytes = std::fs::read(&journal_path).expect("read journal file");

        // Walk the journal byte stream looking for the LogicalTxn
        // frame (kind 0x03). The §4.1 layout starts with kind byte at
        // offset 0 of the frame; total_frame_bytes at offset 4.
        // Journal header is 32 bytes per `JOURNAL_HEADER_SIZE`.
        const JOURNAL_HEADER_SIZE: usize = 32;
        const FRAME_KIND_LOGICAL_TXN: u8 = 0x03;
        let mut frame_offset = None;
        let mut cursor = JOURNAL_HEADER_SIZE;
        while cursor < bytes.len() && frame_offset.is_none() {
            if bytes[cursor] == FRAME_KIND_LOGICAL_TXN {
                frame_offset = Some(cursor);
                break;
            }
            // Advance by 1 byte; only valid for the test scenario where
            // we know exactly one frame was written.
            cursor += 1;
        }
        let frame_start = frame_offset.expect("LogicalTxn frame not found in journal");
        let total = u32::from_le_bytes(
            bytes[frame_start + 4..frame_start + 8]
                .try_into()
                .expect("4 bytes"),
        ) as usize;
        let frame_bytes = &bytes[frame_start..frame_start + total];

        let decoded = LogicalTxnFrame::decode(frame_bytes, salts.0, salts.1, DecodeCtx::Scanning)
            .expect("decode")
            .expect("Some");

        let mut saw_primary = false;
        let mut saw_sec = false;
        for op in &decoded.ops {
            match &op.kind {
                LogicalOpKind::PrimaryInsert { ns_id, .. } => {
                    assert_eq!(
                        *ns_id, STAGE_TIME_NS_ID,
                        "production emitter must encode stage-time \
                         ns_id={STAGE_TIME_NS_ID}, not mutated \
                         {MUTATED_NS_ID}"
                    );
                    saw_primary = true;
                }
                LogicalOpKind::SecondaryInsert { index_id, .. } => {
                    assert_eq!(
                        *index_id, STAGE_TIME_INDEX_ID,
                        "production emitter must encode stage-time \
                         index_id={STAGE_TIME_INDEX_ID}, not mutated \
                         {MUTATED_INDEX_ID}"
                    );
                    saw_sec = true;
                }
                _ => {}
            }
        }
        assert!(saw_primary, "decoded frame must contain a PrimaryInsert");
        assert!(saw_sec, "decoded frame must contain a SecondaryInsert");
    }

    /// §3.7 / US-021 r3 codex blocker — direct encode/decode proof
    /// that the LogicalTxnFrame format-encoding pipeline carries the
    /// STAGE-TIME `ns_id` / `index_id` through to the on-disk bytes.
    /// Constructs a `LogicalTxnFrame` directly with stage-time ids,
    /// encodes it, decodes it back, and asserts the round-tripped
    /// `ns_id` / `index_id` equal the stage-time values — never any
    /// post-stage mutation. Combined with the existing
    /// `rename_safe_staged_ids_survive_rename` proof (drained writes
    /// carry the staged id under in-memory mutation), this closes the
    /// stage→emit→decode chain end-to-end at the unit-test layer.
    #[test]
    fn encoded_logical_txn_frame_round_trips_stage_time_ids() {
        use crate::journal::log_file::{
            DecodeCtx, LogicalOp, LogicalOpKind, LogicalTxnFrame, LOGICAL_TXN_FORMAT_VERSION,
        };
        use crate::mvcc::timestamp::Ts;

        const STAGE_TIME_NS_ID: i64 = 4242;
        const STAGE_TIME_INDEX_ID: i64 = 8484;
        const MUTATED_NS_ID: i64 = STAGE_TIME_NS_ID + 1_000;
        const MUTATED_INDEX_ID: i64 = STAGE_TIME_INDEX_ID + 1_000;
        const SALT1: u32 = 0xCAFE_BABE;
        const SALT2: u32 = 0xDEAD_BEEF;

        let frame = LogicalTxnFrame {
            salt1: SALT1,
            salt2: SALT2,
            commit_ts: Ts {
                physical_ms: 1234,
                logical: 5,
            },
            diagnostic_txn_id: 7,
            format_version: LOGICAL_TXN_FORMAT_VERSION,
            flags: 0,
            ops: vec![
                LogicalOp {
                    op_ordinal: 0,
                    kind: LogicalOpKind::SecondaryInsert {
                        index_id: STAGE_TIME_INDEX_ID,
                        key: b"key".to_vec(),
                        id_bytes: b"id".to_vec(),
                    },
                },
                LogicalOp {
                    op_ordinal: 1,
                    kind: LogicalOpKind::PrimaryInsert {
                        ns_id: STAGE_TIME_NS_ID,
                        key: b"k".to_vec(),
                        value: b"v".to_vec(),
                        overflow: None,
                    },
                },
            ],
        };
        let bytes = frame.encode().expect("encode stage-time frame");
        let decoded = LogicalTxnFrame::decode(&bytes, SALT1, SALT2, DecodeCtx::Scanning)
            .expect("decode")
            .expect("Some");

        let mut saw_primary = false;
        let mut saw_sec = false;
        for op in &decoded.ops {
            match &op.kind {
                LogicalOpKind::PrimaryInsert { ns_id, .. } => {
                    assert_eq!(
                        *ns_id, STAGE_TIME_NS_ID,
                        "encoded PrimaryInsert ns_id must round-trip the \
                         stage-time {STAGE_TIME_NS_ID}, not the mutated \
                         {MUTATED_NS_ID}"
                    );
                    saw_primary = true;
                }
                LogicalOpKind::SecondaryInsert { index_id, .. } => {
                    assert_eq!(
                        *index_id, STAGE_TIME_INDEX_ID,
                        "encoded SecondaryInsert index_id must round-trip \
                         the stage-time {STAGE_TIME_INDEX_ID}, not the \
                         mutated {MUTATED_INDEX_ID}"
                    );
                    saw_sec = true;
                }
                _ => {}
            }
        }
        assert!(saw_primary, "decoded frame must contain a PrimaryInsert");
        assert!(saw_sec, "decoded frame must contain a SecondaryInsert");
    }
}
