//! `WriteTxn` — MVCC writer transaction (begin / commit / rollback).
//!
//! Every writer path runs inside a `WriteTxn` that:
//!
//! 1. On `begin`: drains checkpoint-eligible page-lifetime entries under
//!    `AllocatorHandle::state` so any refcount-0 pages from earlier reader
//!    drops are returned to the free list before the new commit allocates.
//! 2. Accumulates pending overflow-chain pins in `self.pending` (RAII
//!    decref on abort — pages enqueue for deferred-free on Drop).
//! 3. On the Phase 8 commit path: the caller drains the staged chain payload
//!    into one self-contained log record and transfers ownership of the
//!    pending `OverflowRef`s into installed version chains. Durability sync is
//!    owned by the caller's LSN group-commit boundary.
//! 4. On `Drop`: pending `OverflowRef`s decref via RAII.
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

/// Encoded ChainCommit payload and ownership drained from a write txn.
pub(crate) struct PreparedChainCommit {
    /// Overflow refs drained from the transaction.
    pub(crate) pending: Vec<OverflowRef>,
    /// Secondary-index writes drained from the transaction.
    pub(crate) pending_sec_index: Vec<SecIndexWrite>,
    /// Encoded ChainCommit frame bytes for the Phase 8 CRUD log record.
    pub(crate) payload: Vec<u8>,
}

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
#[derive(Debug, Clone, PartialEq, Eq)]
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
/// Entries are staged via `stage_sec_index_{insert,delete}` and
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
    /// Root level of the target secondary-index B+ tree captured with the
    /// root page under the writer's metadata read guard.
    pub(crate) index_root_level: u8,
    /// Compound-key sort directions for unique indexes. Non-unique index
    /// writes do not need prefix conflict ranges at install time.
    pub(crate) unique_directions: Option<Vec<bool>>,
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
    /// Target data-tree namespace retained for diagnostics and logical frames.
    pub(crate) ns: Ns,
    /// Root page of the target primary B+ tree captured under the writer's
    /// metadata read guard.
    pub(crate) root_page: u32,
    /// Root level of the target primary B+ tree captured with `root_page`.
    pub(crate) root_level: u8,
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
    /// Populated via `stage_sec_index_{insert,delete}` and drained at
    /// commit, installing each entry into the per-key version chain
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
        }
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
        index_root_level: u8,
        unique_directions: Option<Vec<bool>>,
        key: Vec<u8>,
        id_bytes: Vec<u8>,
    ) {
        self.pending_sec_index.push(SecIndexWrite {
            index_id,
            index_root_page,
            index_root_level,
            unique_directions,
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
        index_root_level: u8,
        key: Vec<u8>,
    ) {
        self.pending_sec_index.push(SecIndexWrite {
            index_id,
            index_root_page,
            index_root_level,
            unique_directions: None,
            key,
            expected_head: None,
            op: SecIndexOp::Delete,
        });
    }

    /// Stage a primary-tree insert for commit-time chain installation.
    pub(crate) fn stage_primary_insert(
        &mut self,
        ns_id: i64,
        ns: impl Into<Ns>,
        root_page: u32,
        root_level: u8,
        key: Vec<u8>,
        data: Vec<u8>,
        expected_head: Option<ExpectedHead>,
    ) {
        self.pending_primary.push(PrimaryWrite {
            ns_id,
            ns: ns.into(),
            root_page,
            root_level,
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
        root_page: u32,
        root_level: u8,
        key: Vec<u8>,
        data: Vec<u8>,
        expected_head: Option<ExpectedHead>,
    ) {
        self.pending_primary.push(PrimaryWrite {
            ns_id,
            ns: ns.into(),
            root_page,
            root_level,
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
        root_page: u32,
        root_level: u8,
        key: Vec<u8>,
        expected_head: Option<ExpectedHead>,
    ) {
        self.pending_primary.push(PrimaryWrite {
            ns_id,
            ns: ns.into(),
            root_page,
            root_level,
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
    /// [`LogicalTxnFrame::encode`] unchanged, matching §3.5.
    ///
    /// No new `Mutex` / `Arc` — the Cell provides single-threaded interior
    /// mutability scoped to this transaction.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn emit_logical_txn_frame(
        // allow-legacy-journal-audit: test-only retired logical append probe
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
    /// journal I/O. `run_write_commit_envelope` appends the returned frame
    /// inside the journal envelope so rollback handling can distinguish frame
    /// construction from durability.
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
    /// Returns `(commit_ts, pending, pending_sec_index)`. Callers that
    /// stage no overflow data may drop the returned vecs, which runs
    /// `OverflowRef::Drop` on every entry — correct for no-op commits.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn commit(
        // allow-legacy-journal-audit: test-only retired ChainCommit append probe
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
    /// on success.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn commit_with_ts(
        // allow-legacy-journal-audit: test-only retired ChainCommit append probe
        self,
        commit_ts: Ts,
        journal: &BufferPoolHandle,
    ) -> Result<(Vec<OverflowRef>, Vec<SecIndexWrite>)> {
        let (pending, pending_sec_index, _end_lsn) =
            self.commit_chain_commit(journal, commit_ts)?;
        Ok((pending, pending_sec_index))
    }

    /// Append the S7 `ChainCommit` frame for this transaction.
    ///
    /// Returns the drained overflow refs, secondary-index write list, and
    /// chain-commit end LSN after the frame has been appended. This consumes
    /// `self`; on any append failure, `Drop` still owns the drained vectors
    /// and aborts their refcounts normally.
    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn commit_chain_commit(
        // allow-legacy-journal-audit: test-only retired ChainCommit append probe
        mut self,
        journal: &BufferPoolHandle,
        commit_ts: Ts,
    ) -> Result<(Vec<OverflowRef>, Vec<SecIndexWrite>, u64)> {
        let pending = std::mem::take(&mut self.pending).into_vec();
        let page_writes = std::mem::take(&mut self.page_writes).into_vec();
        let refcount_deltas = std::mem::take(&mut self.refcount_deltas).into_vec();
        let pending_sec_index = std::mem::take(&mut self.pending_sec_index).into_vec();

        let end_lsn =
            journal.append_chain_commit_end_lsn(commit_ts, refcount_deltas, page_writes)?;

        Ok((pending, pending_sec_index, end_lsn))
    }

    /// Build and drain the ChainCommit payload without appending it.
    pub(crate) fn prepare_chain_commit_payload(
        mut self,
        journal: &BufferPoolHandle,
        commit_ts: Ts,
    ) -> Result<PreparedChainCommit> {
        let pending = std::mem::take(&mut self.pending).into_vec();
        let page_writes = std::mem::take(&mut self.page_writes).into_vec();
        let refcount_deltas = std::mem::take(&mut self.refcount_deltas).into_vec();
        let pending_sec_index = std::mem::take(&mut self.pending_sec_index).into_vec();
        let (salt1, salt2) = journal.journal_salts().unwrap_or((0, 0));
        let payload = crate::journal::log_file::ChainCommitFrame {
            salt1,
            salt2,
            commit_ts,
            refcount_deltas,
            page_writes,
        }
        .encode()?;

        Ok(PreparedChainCommit {
            pending,
            pending_sec_index,
            payload,
        })
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
#[path = "tests/transaction.rs"]
mod tests;
