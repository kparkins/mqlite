//! Ordinary CRUD commit envelope extracted from `paged_engine.rs`.
//!
//! Owns the full logical-write lifecycle (`run_write` →
//! `run_write_commit_envelope`), the namespace-identity capture/revalidation
//! helpers, the publish-slot registration / pre-durable cleanup helpers, the
//! batched insert-many fast path, and the logical-append percentile probe.
//! Kept out of the root engine file so the production CRUD path stays readable.

use std::cell::Cell;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use bson::{Bson, Document};

#[cfg(any(test, feature = "test-hooks"))]
use super::hidden_accessors::{LegacyCommitFailpoint, Us026PostRegisterFailpoint};

use crate::error::{Error, Result, WriteConflictReason};
use crate::journal::wire::LogRecordDraft;
use crate::keys::encode_key;
use crate::mvcc::transaction::WriteTxn;
use crate::storage::root_snapshot::PublishedCatalog;
use crate::validation::validate_document;

use super::doc_helpers::ensure_id;
use super::index_maint::{
    flip_pending_to_aborted_for, flip_pending_to_committed_for, install_pending_primary,
    install_pending_sec_index,
};
use super::publish::rebuild_and_publish;
use super::state::{MetadataState, SharedState};
use super::visibility::WriteVisibility;
use super::{IndexCatalogIdentity, NamespaceCatalogIdentity, PagedEngine};

/// Internal result class for the batched insert-many fast path.
pub(crate) enum InsertManyBatchError {
    /// Staging failed before any journal reservation; caller may retry the
    /// valid prefix or remaining batch without duplicating any durable writes.
    Staging { index: usize, error: Error },
    /// The commit envelope or namespace bootstrap failed; caller must surface
    /// this error because durable state may have been reserved or written.
    Commit(Error),
}

fn duplicate_primary_key_error() -> Error {
    Error::DuplicateKey {
        detail: "document with _id already exists".to_owned(),
    }
}

fn preflight_insert_many_primary_keys(
    docs: &mut [Document],
) -> std::result::Result<(), InsertManyBatchError> {
    let mut keys = HashSet::with_capacity(docs.len());
    for (index, doc) in docs.iter_mut().enumerate() {
        if let Err(error) = validate_document(doc) {
            return Err(InsertManyBatchError::Staging { index, error });
        }
        let id = ensure_id(doc);
        if !keys.insert(encode_key(&id)) {
            return Err(InsertManyBatchError::Staging {
                index,
                error: duplicate_primary_key_error(),
            });
        }
    }
    Ok(())
}

/// RAII guard that records the logical-frame-append duration sample and
/// recomputes the percentile gauges (p50/p95/p99) from the ring buffer.
///
/// The recorded duration now spans Phase 8 draft construction, reservation,
/// positioned write, and ready marking. It is still a coarse append-envelope
/// sample; percentile recomputation happens on drop after the hot write work.
struct LogicalTxnAppendPercentileRefresh {
    start: Instant,
}

impl LogicalTxnAppendPercentileRefresh {
    fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl Drop for LogicalTxnAppendPercentileRefresh {
    fn drop(&mut self) {
        let elapsed_ms = self.start.elapsed().as_millis() as u64;
        crate::mvcc::metrics::record_logical_txn_append_duration_ms_and_maybe_recompute(elapsed_ms);
    }
}

/// Namespace catalog identity captured in S1 of the CRUD commit envelope.
///
/// `ns_id` is the resolved durable namespace id and `catalog_gen` is the
/// `catalog_generation` snapshotted under the S1 metadata-read scope (the
/// cheap DDL dirty bit). Both are eager: `ns_id` feeds S2 write-visibility
/// setup on every commit, and `catalog_gen` is a `u64` copy.
///
/// `captured_epoch` retains the `Arc<PublishedEpoch>` loaded at capture so
/// the AC #4 captured-identity gate can lazily materialize the
/// *capture-time* `NamespaceCatalogIdentity` only inside its slow path (when
/// the cheap `catalog_gen` dirty bit actually changed). Holding the `Arc` is
/// a refcount bump — no `NamespaceCatalogIdentity` (Vec + per-index String +
/// bson `Document`) is cloned on the steady-state no-DDL fast path. See the
/// S3.5 gate for why that slow path cannot be taken under today's locking.
pub(super) struct CapturedNamespaceIdentity {
    pub(super) ns_id: Option<i64>,
    pub(super) catalog_gen: u64,
    pub(super) captured_epoch: Arc<crate::storage::root_snapshot::PublishedEpoch>,
}

impl PagedEngine {
    fn namespace_catalog_identity(
        md: &MetadataState,
        ns: &str,
    ) -> Result<Option<NamespaceCatalogIdentity>> {
        let cat = md.catalog_lock();
        let Some(collection) = cat.get_collection(ns)? else {
            return Ok(None);
        };
        let indexes = cat
            .list_indexes(ns)?
            .into_iter()
            .map(|index| IndexCatalogIdentity {
                id: index.id,
                name: index.name,
                key_pattern: index.key_pattern,
                unique: index.unique,
                sparse: index.sparse,
                state: index.state,
            })
            .collect();
        Ok(Some(NamespaceCatalogIdentity {
            ns_id: collection.id,
            indexes,
        }))
    }

    fn namespace_catalog_identity_from_published(
        catalog: &PublishedCatalog,
        ns: &str,
    ) -> Option<NamespaceCatalogIdentity> {
        let snapshot = catalog.get_by_name(ns)?;
        let indexes = snapshot
            .indexes
            .iter()
            .map(|index| IndexCatalogIdentity {
                id: index.id,
                name: index.name.clone(),
                key_pattern: index.key_pattern.clone(),
                unique: index.unique,
                sparse: index.sparse,
                state: index.state,
            })
            .collect();
        Some(NamespaceCatalogIdentity {
            ns_id: snapshot.id,
            indexes,
        })
    }

    /// CRUD write lifecycle.
    ///
    /// Drives: metadata.read() → bootstrap-if-missing → private logical
    /// WriteTxn setup → body → install Pending deltas → Phase 8 LogRecord
    /// reservation/write/durability gate → flip Pending to Committed →
    /// ordered publish.
    pub(super) fn run_write<F, R>(&self, ns: &str, f: F) -> Result<R>
    where
        F: FnOnce(&SharedState, &MetadataState, &mut WriteTxn, &WriteVisibility<'_>) -> Result<R>,
    {
        self.shared.check_engine_not_poisoned()?;
        let published = self.shared.published.load_full();
        if let Some(snapshot) = published.catalog.get_by_name(ns) {
            let ns_id = snapshot.id;
            drop(published);
            return self.run_write_commit_envelope(ns, Some(ns_id), f);
        }
        drop(published);

        // Take a read guard to decide whether this write must bootstrap the
        // namespace before entering the ordinary commit envelope.
        let md_read = self
            .metadata
            .read()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
        let ns_missing = self
            .metadata_state
            .catalog_lock()
            .get_collection(ns)?
            .is_none();
        if ns_missing {
            drop(md_read);
            let ns_id = self.bootstrap_namespace(ns)?;
            return self.run_write_commit_envelope(ns, Some(ns_id), f);
        }
        drop(md_read);
        self.run_write_commit_envelope(ns, None, f)
    }

    /// Resolve the namespace id + catalog generation for S1 of the CRUD
    /// commit envelope, called while `self.metadata.read()` is held.
    ///
    /// Returns a `CapturedNamespaceIdentity` (`ns_id`, `catalog_gen`, and the
    /// retained `captured_epoch` Arc). Deliberately does NOT materialize a
    /// `NamespaceCatalogIdentity`: the full identity (Vec + per-index String +
    /// bson `Document`) is only consumed by the AC #4 gate's slow path, which
    /// cannot run under today's locking (see the S3.5 gate), so cloning it on
    /// every commit was pure fast-path waste. The gate rebuilds the
    /// capture-time identity from `captured_epoch` lazily if it ever needs it.
    ///
    /// Existence resolution is cheap: a published-snapshot `get_by_name`
    /// lookup, with a live-catalog `get_collection` existence probe as the
    /// fallback for a freshly-bootstrapped namespace not yet reflected in the
    /// captured published epoch. Neither path clones index identity.
    ///
    /// Must not touch `self.metadata` — the caller holds the read guard and
    /// passes the already-loaded `captured_epoch` snapshot.
    ///
    /// §10.17.1: `catalog_generation` is the cheap dirty bit for DDL. CRUD
    /// never advances it; only DDL does through `next_catalog_gen`. If it
    /// changes before the AC #4 gate, the caller revalidates the captured
    /// identity before deciding whether this writer is stale.
    fn capture_namespace_identity(
        &self,
        ns: &str,
        bootstrapped_ns_id: Option<i64>,
        captured_epoch: Arc<crate::storage::root_snapshot::PublishedEpoch>,
    ) -> Result<CapturedNamespaceIdentity> {
        let catalog_gen = captured_epoch.catalog_generation;
        // Cheap existence + id resolution. `get_by_name` returns the durable
        // namespace id without cloning index identity; the live-catalog
        // fallback covers a just-bootstrapped namespace not yet published.
        let published_ns_id = captured_epoch.catalog.get_by_name(ns).map(|snap| snap.id);
        let ns_id = match bootstrapped_ns_id {
            Some(id) => {
                // Bootstrap caller already resolved the id; require the
                // namespace to still exist (published snapshot or live
                // catalog) or it was dropped between bootstrap and here.
                let live = published_ns_id.is_some()
                    || self.metadata_state.catalog_lock().get_collection(ns)?.is_some();
                if !live {
                    return Err(Error::WriteConflict {
                        reason: WriteConflictReason::CatalogGenerationChanged,
                    });
                }
                Some(id)
            }
            None => match published_ns_id {
                Some(id) => Some(id),
                None => self
                    .metadata_state
                    .catalog_lock()
                    .get_collection(ns)?
                    .map(|collection| collection.id),
            },
        };
        Ok(CapturedNamespaceIdentity {
            ns_id,
            catalog_gen,
            captured_epoch,
        })
    }

    /// Ordinary CRUD commit envelope after namespace bootstrap has been settled.
    ///
    /// Metadata-guard protocol: there is exactly one `metadata.read()`
    /// acquisition in this function. It is held across ordinary CRUD's full
    /// private-logical lifecycle so DDL cannot mutate the catalog identity
    /// while the writer installs resident Pending deltas, appends logical
    /// durability records, and publishes through the sequencer.
    /// `NsWriterRegistry` is no longer ordinary CRUD's same-namespace
    /// serialization authority.
    ///
    /// AC #4 captured-identity gate (§10.17.1): immediately before the
    /// durable journal envelope this function compares the
    /// `catalog_generation` captured in the S1 scope against the current
    /// published catalog generation. A mismatch triggers a target-namespace
    /// identity revalidation; if that namespace/index identity changed, the
    /// writer returns `WriteConflict { CatalogGenerationChanged }` while
    /// rollback is still purely in-memory. Catalog DDL on unrelated
    /// namespaces does not invalidate the writer's captured identity. (See the
    /// S3.5 gate body for why this revalidation cannot actually fire under
    /// today's metadata-guard locking and is kept as forward-defense.)
    ///
    /// `bootstrapped_ns_id` is a slight misnomer: `Some(id)` does NOT mean
    /// *this* call bootstrapped the namespace. `run_write` passes `Some(id)`
    /// on BOTH the steady-state fast path (the namespace already existed in
    /// the published catalog — the common case) and the bootstrap path (the
    /// namespace was just created by `bootstrap_namespace`). It is `None` only
    /// for the rare in-flight window where the namespace exists in the live
    /// `Catalog` but is not yet reflected in the published epoch. The name
    /// reads as "a caller-resolved namespace id is available", not "freshly
    /// bootstrapped".
    ///
    /// Skipped id re-check: when `bootstrapped_ns_id` is `Some(id)`, S1 trusts
    /// `id` verbatim and only re-checks that the namespace still EXISTS (a
    /// liveness probe against the captured published snapshot, falling back to
    /// the live catalog). It deliberately does NOT re-derive the id from the
    /// catalog and assert it equals `id`. This is safe: the caller resolved
    /// `id` under either the published snapshot or `metadata.read()`, durable
    /// namespace ids are monotonic and never reused (Phase 1 §10.7), and a
    /// drop-then-recreate of the same name yields a fresh id whose absence the
    /// liveness probe (drop) or the S3.5 gate (recreate bumps the generation)
    /// would catch. The id-equality check is therefore redundant and skipped
    /// to keep S1 clone-free.
    pub(super) fn run_write_commit_envelope<F, R>(
        &self,
        ns: &str,
        bootstrapped_ns_id: Option<i64>,
        f: F,
    ) -> Result<R>
    where
        F: FnOnce(&SharedState, &MetadataState, &mut WriteTxn, &WriteVisibility<'_>) -> Result<R>,
    {
        self.shared.check_engine_not_poisoned()?;
        let _checkpoint_writer_admission = self.shared.checkpoint_admission.admit_writer()?;
        let _crud_read = self
            .metadata
            .read()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
        let captured_epoch = self.shared.published.load_full();

        // --- S1: capture namespace catalog identity ---
        // Resolved under the metadata read guard; see `capture_namespace_identity`.
        // Use `published.load_full()` directly — the §10.5 single-load gate is
        // scoped to reads only. The loaded `Arc<PublishedEpoch>` is moved into
        // `captured` so the S3.5 gate can lazily rebuild the capture-time
        // identity from it; the steady-state fast path never reads it again.
        let captured = self.capture_namespace_identity(ns, bootstrapped_ns_id, captured_epoch)?;
        let captured_ns_id = captured.ns_id;
        // --- S2: writer visibility setup ---
        // COMMIT-ENVELOPE-RESIDUE: A (visibility setup fails before journal append).
        let vis = self.write_visibility_after_capture(ns, captured_ns_id)?;

        // Pre-reservation failures are cleaned up without touching the log;
        // post-reservation failures poison the reserved LSN slot.
        let txn_id = vis.read_view.txn_id;
        let mut txn = WriteTxn::new(txn_id);

        // --- S3: execute write body ---
        // The catalog itself is behind `Mutex<Catalog>`, while page latches and
        // expected-head checks serialize resident chain mutation.
        #[cfg(any(test, feature = "test-hooks"))]
        super::hidden_accessors::write_body_entry_if_installed(&self.shared, ns);
        let body_result = f(&self.shared, &self.metadata_state, &mut txn, &vis);

        match body_result {
            Ok(value) => {
                // Root-neutral vs root-changing classification. Header sync
                // still captures catalog-root movement; logical-chain installs
                // can also make primary B-tree structural progress without
                // forcing a fresh catalog header.
                let mut root_changing = false;

                // Refresh the logical-txn append-duration percentiles after
                // the Phase 8 log append envelope completes.
                let _logical_txn_append_pct_refresh = LogicalTxnAppendPercentileRefresh::new();

                let sec_writes = std::mem::take(&mut txn.pending_sec_index);
                let primary_writes = std::mem::take(&mut txn.pending_primary);

                // --- S3.5: captured-identity gate ---
                // Compare the `catalog_generation` we snapshotted in the S1
                // metadata-read scope against the live published generation. A
                // DDL that completed between capture and now bumped
                // `PublishedEpoch.catalog_generation` via the `next_catalog_gen`
                // reservation; ordinary CRUD never bumps it. Because the
                // generation is global, a mismatch is only a signal to
                // revalidate the target namespace/index identity, not an
                // automatic conflict for unrelated DDL.
                //
                // This gate runs before `register_with_oracle`, before any
                // Pending install, and before log reservation. Rollback is
                // purely in-memory.
                //
                // DEFENSE-IN-DEPTH — this gate CANNOT fire under today's
                // locking, and the work below is deliberately structured so the
                // steady-state no-DDL fast path pays nothing for it. Every
                // crate-wide `catalog_generation` advance is stamped exclusively
                // by a DDL publish closure (`drop_namespace`,
                // `run_namespace_create_ddl`/`bootstrap_namespace`,
                // `create_index_reserve`/`_commit`/`_cleanup`, `drop_index` —
                // all via `run_catalog_ddl_envelope`), and each of those holds
                // `metadata.write()` from before it reserves the generation
                // until after it publishes. This envelope holds `metadata.read()`
                // (the `_crud_read` guard) continuously from the S1 capture
                // through this point, so no DDL can complete its generation-
                // stamping publish in that window: `current_gen` always equals
                // `captured.catalog_gen` here. (Single-threaded reopen recovery
                // installs a generation without `metadata.write()`, but no CRUD
                // envelope runs concurrently with recovery, so it is exempt.)
                // The gate is retained as forward-defense: if a future change
                // ever narrowed the CRUD read-guard hold so a DDL generation
                // bump could interleave, this revalidation would catch a writer
                // whose target namespace/index identity actually changed while
                // still tolerating unrelated-namespace DDL.
                //
                // && short-circuit economics: the cheap `u64` generation compare
                // is the outer guard, so when it is equal (always, today) the
                // RHS never evaluates — no `published.load_full()`, no
                // capture-time identity rebuild, and no current-identity clone
                // (each a Vec + per-index String + bson `Document`) on the fast
                // path. Both identities are materialized lazily inside the slow
                // arm only, the capture-time one from the `Arc<PublishedEpoch>`
                // retained at S1.
                if self.shared.published.load().catalog_generation != captured.catalog_gen {
                    // Slow arm — unreachable under today's locking (see above).
                    // Write-path direct load (§10.5 single-load gate is
                    // read-path only); see the matching note at the S1 capture.
                    let current_epoch = self.shared.published.load_full();
                    let current_identity = match Self::namespace_catalog_identity_from_published(
                        &current_epoch.catalog,
                        ns,
                    ) {
                        Some(identity) => Some(identity),
                        None => Self::namespace_catalog_identity(&self.metadata_state, ns)?,
                    };
                    // CAVEAT (forward-defense accuracy): the `None` fallback
                    // below reads the CURRENT live catalog, not a capture-time
                    // snapshot — for a live-but-unpublished namespace the
                    // rebuilt "captured" identity is therefore not
                    // snapshot-stable. Inert today (this arm is unreachable
                    // while the envelope holds `metadata.read()`), but if this
                    // gate is ever made reachable by narrowing the read-guard
                    // hold, the identity must be frozen eagerly at S1 instead.
                    let captured_identity = match Self::namespace_catalog_identity_from_published(
                        &captured.captured_epoch.catalog,
                        ns,
                    ) {
                        Some(identity) => Some(identity),
                        None => Self::namespace_catalog_identity(&self.metadata_state, ns)?,
                    };
                    if current_identity != captured_identity {
                        drop(txn);
                        return Err(Error::WriteConflict {
                            reason: WriteConflictReason::CatalogGenerationChanged,
                        });
                    }
                }

                let txn_id = txn.txn_id;

                // --- S4: oracle slot registration ---
                let slot = match self.register_ordinary_crud_slot() {
                    Ok(slot) => slot,
                    Err(e) => {
                        drop(txn);
                        return Err(e);
                    }
                };
                let commit_ts = slot.commit_ts();
                txn.commit_ts.set(Some(commit_ts));
                // commit_ts monotonicity is guaranteed by the oracle +
                // sequencer; only a code bug could regress it, so this is a
                // `debug_assert!` (the same invariant is re-checked at the
                // actual store site in `publish_commit`). The probe load is
                // gated to debug builds so the hot path pays no
                // `published.load_full()` here in release, and so a violated
                // invariant can never panic-poison the held `_crud_read`
                // metadata guard in production.
                #[cfg(debug_assertions)]
                {
                    let prev_published = self.shared.published.load_full();
                    debug_assert!(
                        commit_ts > prev_published.visible_ts,
                        "commit_ts must advance beyond previous PublishedEpoch"
                    );
                }

                let dirty = txn.publish_dirty;

                // --- S5: build log-record payload ---
                let frame =
                    txn.build_logical_txn_frame(&self.shared.handle, &primary_writes, &sec_writes);

                let logical_payload = match frame.encode() {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        drop(txn);
                        return Err(
                            self.cleanup_registered_pre_durable_failure(txn_id, slot, e)
                        );
                    }
                };
                let logical_payload_len = logical_payload.len();

                let prepared =
                    match txn.prepare_chain_commit_payload(&self.shared.handle, commit_ts) {
                        Ok(prepared) => prepared,
                        Err(e) => {
                            return Err(
                                self.cleanup_registered_pre_durable_failure(txn_id, slot, e)
                            );
                        }
                    };
                let draft = LogRecordDraft::crud(
                    txn_id,
                    slot.publish_seq(),
                    commit_ts,
                    logical_payload,
                    prepared.payload,
                );

                // --- S6: install Pending secondary-index deltas ---
                let sec_pages = match install_pending_sec_index(
                    &self.shared,
                    &self.metadata_state,
                    sec_writes.into_vec(),
                    &vis,
                    commit_ts,
                    txn_id,
                ) {
                    Ok(pages) => pages,
                    Err(e) => {
                        return Err(
                            self.cleanup_registered_pre_durable_failure(txn_id, slot, e)
                        );
                    }
                };

                // --- S7: install Pending primary-index deltas ---
                #[cfg(any(test, feature = "test-hooks"))]
                if let Err(e) =
                    super::hidden_accessors::us019_maybe_fail_primary_install(&self.shared)
                {
                    return Err(self.cleanup_registered_pre_durable_failure(txn_id, slot, e));
                }
                let primary_install = install_pending_primary(
                    &self.shared,
                    &self.metadata_state,
                    primary_writes.into_vec(),
                    &vis,
                    commit_ts,
                    txn_id,
                );
                let (primary_pages, primary_structural_tree_change) = match primary_install {
                    Ok(result) => result,
                    Err(e) => {
                        return Err(
                            self.cleanup_registered_pre_durable_failure(txn_id, slot, e)
                        );
                    }
                };
                root_changing |= primary_structural_tree_change;
                let mut pending_pages = sec_pages;
                pending_pages.extend(primary_pages);

                // --- S8: reserve log record ---
                #[cfg(any(test, feature = "test-hooks"))]
                if let Err(e) = super::hidden_accessors::us026_fail_if_armed(
                    &self.shared,
                    Us026PostRegisterFailpoint::BeforeLogReservation,
                ) {
                    return Err(self.cleanup_registered_pre_durable_failure(txn_id, slot, e));
                }
                #[cfg(any(test, feature = "test-hooks"))]
                super::hidden_accessors::before_log_reservation_if_installed(&self.shared);

                let reserve_start = Instant::now();
                let reserved = match self.shared.handle.reserve_log_record(draft) {
                    Ok(reserved) => reserved,
                    Err(e) => {
                        crate::mvcc::metrics::record_commit_envelope_stage_duration(
                            crate::mvcc::metrics::CommitEnvelopeStage::LogReserve,
                            reserve_start.elapsed(),
                        );
                        return Err(
                            self.cleanup_registered_pre_durable_failure(txn_id, slot, e)
                        );
                    }
                };
                crate::mvcc::metrics::record_commit_envelope_stage_duration(
                    crate::mvcc::metrics::CommitEnvelopeStage::LogReserve,
                    reserve_start.elapsed(),
                );
                let commit_end_lsn = reserved.end_lsn();

                // §4.6 deviation: US-006 installs Pending pages as
                // Unflushable before reservation; this post-reservation stamp
                // is the first point where those pages become flushable by LSN.
                //
                // Why this ordering is mandatory: the install steps above
                // dirtied the leaf frames before this commit's end LSN existed,
                // and the buffer pool marks every dirty unpin Unflushable. An
                // Unflushable frame can never be chosen for write-back, so
                // between install and here the new bytes are pinned in memory
                // and cannot race ahead of their own log record onto disk —
                // which would be a WAL violation (an effect on disk with no
                // durable redo). Reserving the log record yields the commit's
                // end LSN; stamping the touched pages with it converts them
                // from Unflushable to flushable-once-durable-through that LSN.
                // From this point the WAL-before-data fence in `flush` and in
                // CLOCK eviction governs them normally.
                #[cfg(any(test, feature = "test-hooks"))]
                if let Err(e) = super::hidden_accessors::fail_dirty_lsn_stamp_if_armed(&self.shared)
                {
                    return Err(self.poison_after_reserved_log_failure(&reserved, e));
                }
                if let Err(e) = self
                    .shared
                    .handle
                    .stamp_dirty_pages_lsn(&pending_pages, commit_end_lsn)
                {
                    return Err(self.poison_after_reserved_log_failure(&reserved, e));
                }
                #[cfg(any(test, feature = "test-hooks"))]
                if let Err(e) =
                    super::hidden_accessors::fail_after_dirty_lsn_stamp_if_armed(&self.shared)
                {
                    return Err(self.poison_after_reserved_log_failure(&reserved, e));
                }
                // --- S8b: write and mark log record ready ---
                let write_ready_start = Instant::now();
                let written_end_lsn = match reserved.write_and_mark() {
                    Ok(end_lsn) => end_lsn,
                    Err(e) => {
                        crate::mvcc::metrics::record_commit_envelope_stage_duration(
                            crate::mvcc::metrics::CommitEnvelopeStage::LogWriteReady,
                            write_ready_start.elapsed(),
                        );
                        return Err(self.poison_after_log_manager_failure(e));
                    }
                };
                crate::mvcc::metrics::record_commit_envelope_stage_duration(
                    crate::mvcc::metrics::CommitEnvelopeStage::LogWriteReady,
                    write_ready_start.elapsed(),
                );
                debug_assert_eq!(written_end_lsn, commit_end_lsn);
                crate::mvcc::metrics::record_logical_txn_append_bytes(logical_payload_len as u64);
                crate::mvcc::metrics::record_journal_chain_commit_frame();
                // --- S9: durability gate ---
                self.wait_for_commit_durability(commit_end_lsn)?;
                #[cfg(any(test, feature = "test-hooks"))]
                if super::hidden_accessors::fail_after_durable_before_flip_if_armed(&self.shared)
                    .is_err()
                {
                    return Err(self.engine_fatal(
                        crate::error::EngineFatalReason::PostDurablePendingFlipFailure,
                    ));
                }

                #[cfg(test)]
                super::hidden_accessors::publish_pause_if_installed(&self.shared);

                // --- S10: Pending-to-Committed flip ---
                // The log record is ready/durable before this flip. The flip
                // runs before publish so no reader can observe the new epoch
                // with uncommitted heads.
                let pending_flip_start = Instant::now();
                let pending_flip_result =
                    flip_pending_to_committed_for(&self.shared, txn_id, commit_ts, &pending_pages)
                        .map_err(|_| {
                            self.engine_fatal(
                                crate::error::EngineFatalReason::PostDurablePendingFlipFailure,
                            )
                        });
                crate::mvcc::metrics::record_commit_envelope_stage_duration(
                    crate::mvcc::metrics::CommitEnvelopeStage::PendingFlip,
                    pending_flip_start.elapsed(),
                );
                pending_flip_result?;
                #[cfg(any(test, feature = "test-hooks"))]
                {
                    super::hidden_accessors::us009_record_committed_flip(&self.shared);
                    if super::hidden_accessors::us009_fail_after_committed_flip_if_armed(
                        &self.shared,
                    )
                    .is_err()
                    {
                        return Err(self.engine_fatal(
                            crate::error::EngineFatalReason::PostDurablePendingFlipFailure,
                        ));
                    }
                }

                #[cfg(any(test, feature = "test-hooks"))]
                super::hidden_accessors::legacy_commit_abort_if_armed(
                    LegacyCommitFailpoint::AfterLegacyCommitBeforePublish,
                );

                // --- S11: ordered publish via sequencer ---
                let shared = Arc::clone(&self.shared);
                let metadata_state = Arc::clone(&self.metadata_state);
                let publish_start = Instant::now();
                let publish_result =
                    self.shared
                        .publish_sequencer
                        .mark_ready(slot, move |publish_ts| {
                            #[cfg(any(test, feature = "test-hooks"))]
                            super::hidden_accessors::us009_record_publish_ready(&shared);
                            rebuild_and_publish(&shared, &metadata_state, publish_ts, dirty, None)
                        });
                crate::mvcc::metrics::record_commit_envelope_stage_duration(
                    crate::mvcc::metrics::CommitEnvelopeStage::PublishReady,
                    publish_start.elapsed(),
                );
                match publish_result {
                    Ok(()) => {}
                    Err(Error::EngineFatal { reason }) => {
                        return Err(Error::EngineFatal { reason });
                    }
                    Err(_) => {
                        return Err(self.engine_fatal(
                            crate::error::EngineFatalReason::PostDurablePublishFailure,
                        ));
                    }
                }

                // --- S12: post-publish metrics and interval sync ---
                if root_changing {
                    crate::mvcc::metrics::record_crud_commit_root_changing();
                } else {
                    crate::mvcc::metrics::record_crud_commit_root_neutral();
                }
                self.maybe_sync_interval_after_publish()?;
                Ok(value)
            }
            Err(e) => {
                // COMMIT-ENVELOPE-RESIDUE: A (S3 body failure before journal append).
                drop(txn);
                Err(e)
            }
        }
    }

    pub(super) fn register_ordinary_crud_slot(
        &self,
    ) -> Result<super::publish_sequencer::PublishSlotGuard> {
        let publish_sequencer = &self.shared.publish_sequencer;
        publish_sequencer.register_with_oracle(&self.shared.oracle)
    }

    pub(super) fn cleanup_registered_pre_durable_failure(
        &self,
        txn_id: u64,
        slot: super::publish_sequencer::PublishSlotGuard,
        error: Error,
    ) -> Error {
        if let Error::EngineFatal { reason } = error {
            return super::state::poison_after_durable_commit(&self.shared, reason);
        }
        // Flip this txn's resident Pending heads to Aborted BEFORE aborting the
        // publish slot. The order matters: marking the slot aborted advances
        // the dense publish window past it, so a later commit can advance the
        // published frontier beyond this slot's commit_ts. If any resident head
        // were still Pending at that point, a foreign reader at a read_ts above
        // the slot would treat the Pending-below-frontier head as committed
        // (the frontier-passage rule in `mvcc::chain_snapshot::version_visible_to`)
        // — a dirty read of never-committed, aborted data.
        //
        // The flip can fail non-fatally (bounded-retry exhaustion under chain
        // contention, or a pin failure). It used to be swallowed
        // (`let _ = ...`), leaving the heads Pending while the slot still
        // aborted — exactly the dirty-read window above. On flip Err we must
        // NOT abort the slot. Poison the engine instead: the txn was never
        // durable, so reopen-recovery discards it wholesale, the resident
        // Pending heads die with the process, and no in-flight reader ever sees
        // the slot pass the frontier. The publish slot stays pending for reopen
        // ownership (the poisoned sequencer's `mark_*` are no-ops), so the
        // frontier can never advance past it in this process.
        if let Err(_flip_err) = flip_pending_to_aborted_for(&self.shared, txn_id) {
            return super::state::poison_after_durable_commit(
                &self.shared,
                crate::error::EngineFatalReason::PreDurableAbortFlipFailure,
            );
        }
        self.shared.publish_sequencer.mark_aborted(slot);
        error
    }

    fn write_visibility_after_capture(
        &self,
        ns: &str,
        captured_ns_id: Option<i64>,
    ) -> Result<WriteVisibility<'_>> {
        let start = Instant::now();
        loop {
            match WriteVisibility::new(&self.shared, ns) {
                Ok(vis) => return Ok(vis),
                Err(Error::CollectionNotFound { .. })
                    if captured_ns_id.is_some() && start.elapsed() < self.busy_timeout =>
                {
                    std::thread::yield_now();
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Insert all documents in one ordinary CRUD commit envelope.
    ///
    /// Staging failures are reported separately so the public `insert_many`
    /// layer can preserve ordered/unordered bulk-write semantics without
    /// retrying post-reservation failures.
    pub(crate) fn insert_many_batch(
        &self,
        ns: &str,
        mut docs: Vec<Document>,
    ) -> std::result::Result<Vec<Bson>, InsertManyBatchError> {
        self.shared
            .check_engine_not_poisoned()
            .map_err(InsertManyBatchError::Commit)?;
        preflight_insert_many_primary_keys(&mut docs)?;

        let body_started = Cell::new(false);
        let body_completed = Cell::new(false);
        let staging_error_index = Cell::new(None);
        let result = self.run_write(ns, |shared, md, txn, vis| {
            body_started.set(true);
            let mut ids = Vec::with_capacity(docs.len());
            for (index, doc) in docs.into_iter().enumerate() {
                match super::doc_ops::stage_insert(shared, md, txn, vis, ns, doc) {
                    Ok(id) => ids.push(id),
                    Err(error) => {
                        staging_error_index.set(Some(index));
                        return Err(error);
                    }
                }
            }
            body_completed.set(true);
            Ok(ids)
        });

        match result {
            Ok(ids) => Ok(ids),
            Err(error) if body_started.get() && !body_completed.get() => {
                Err(InsertManyBatchError::Staging {
                    index: staging_error_index.get().unwrap_or(0),
                    error,
                })
            }
            Err(error) => Err(InsertManyBatchError::Commit(error)),
        }
    }
}
