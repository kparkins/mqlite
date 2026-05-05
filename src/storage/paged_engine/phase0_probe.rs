//! Hidden Phase 0 write-envelope probes for `PagedEngine`.
//!
//! Kept out of `paged_engine.rs` so the production CRUD path stays readable.

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::mvcc::transaction::WriteTxn;
use crate::storage::buffer_pool::PageSize;
use crate::storage::phase0_probe::{Phase0ProbeCut, Phase0ProbeReport};
use crate::storage::txn_page_store::{PageOrigin, PageReservation, TxnOverlay};

use super::catalog_ops::{catalog_lock, new_store, rebuild_and_publish_locked};
use super::doc_ops;
use super::index_maint::{
    commit_pending_primary_states, commit_pending_primary_states_with_overlay,
    commit_pending_sec_index_states, install_pending_primary, install_pending_sec_index,
};
use super::visibility::WriteVisibility;
use super::PagedEngine;

impl PagedEngine {
    fn phase0_probe_visible(&self, ns: &str, inserted_id: &Bson) -> Result<bool> {
        let filter = bson::doc! { "_id": inserted_id.clone() };
        Ok(doc_ops::find_one(self, ns, &filter)?.is_some())
    }

    fn phase0_stop_before_recovery(
        report: Phase0ProbeReport,
        txn: WriteTxn,
        overlay: TxnOverlay,
    ) -> Result<Phase0ProbeReport> {
        std::mem::forget(txn);
        std::mem::forget(overlay);
        Ok(report)
    }

    pub(super) fn phase0_probe_insert_impl(
        &self,
        ns: &str,
        doc: Document,
        cut: Phase0ProbeCut,
    ) -> Result<Phase0ProbeReport> {
        let _writer_ticket = {
            let _md_read = self
                .metadata
                .read()
                .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
            let ns_id = match catalog_lock(&self.metadata_state).get_collection(ns)? {
                Some(collection) => collection.id,
                None => {
                    return Err(Error::CollectionNotFound {
                        name: ns.to_owned(),
                    });
                }
            };
            self.shared.ns_writers.admit(ns_id, self.busy_timeout)?
        };
        let vis = WriteVisibility::new(&self.shared, ns)?;
        let mark = self.shared.handle.begin_txn()?;
        let mut overlay = TxnOverlay::new();
        let ready = self
            .shared
            .handle
            .allocator()
            .drain_deferred_free_reservations();
        for page in ready {
            overlay.push_reservation(PageReservation {
                page,
                size: PageSize::Large32k,
                origin: PageOrigin::DeferredFree,
            });
        }
        let txn_id = vis.read_view.txn_id;
        let mut txn = WriteTxn::new(txn_id);

        let body_result = doc_ops::stage_insert_body(
            &self.shared,
            &self.metadata_state,
            &mut overlay,
            &mut txn,
            &vis,
            ns,
            doc,
        );
        let inserted_id = match body_result {
            Ok(id) => id,
            Err(e) => {
                drop(txn);
                let _ = self.rollback_overlay_and_wal(overlay, mark);
                return Err(e);
            }
        };
        let root_changing = overlay.has_header_update();

        let _journal = self.lock_journal_mutex();

        let sec_writes = std::mem::take(&mut txn.pending_sec_index);
        let primary_writes = std::mem::take(&mut txn.pending_primary);
        if primary_writes.is_empty() {
            drop(txn);
            let _ = self.rollback_overlay_and_wal(overlay, mark);
            return Err(Error::Internal(
                "phase0 probe insert produced no primary write".into(),
            ));
        }
        let mut report = Phase0ProbeReport {
            commit_ts: None,
            publish_ts: None,
            pre_publish_visible: None,
            post_publish_visible: None,
        };
        if cut == Phase0ProbeCut::AfterStageBeforeCommitTs {
            return Self::phase0_stop_before_recovery(report, txn, overlay);
        }

        let commit_ts = match txn.allocate_commit_ts(&self.shared.oracle) {
            Ok(ts) => ts,
            Err(e) => {
                drop(txn);
                let _ = self.rollback_overlay_and_wal(overlay, mark);
                return Err(e);
            }
        };
        report.commit_ts = Some((commit_ts.physical_ms, commit_ts.logical));
        if matches!(
            cut,
            Phase0ProbeCut::AfterCommitTsBeforeLogicalFrame | Phase0ProbeCut::AfterAllocateCommitTs
        ) {
            return Self::phase0_stop_before_recovery(report, txn, overlay);
        }

        let frame = txn.build_logical_txn_frame(&self.shared.handle, &primary_writes, &sec_writes);
        if cut == Phase0ProbeCut::AfterLogicalFrameBeforeAppend {
            return Self::phase0_stop_before_recovery(report, txn, overlay);
        }

        if let Err(e) = self
            .shared
            .handle
            .append_logical_txn(frame)
            .and_then(|_| self.shared.handle.fsync_logical_tail())
        {
            drop(txn);
            let _ = self.rollback_overlay_and_wal(overlay, mark);
            return Err(e);
        }
        if cut == Phase0ProbeCut::AfterLogicalAppendBeforeChainCommit {
            return Self::phase0_stop_before_recovery(report, txn, overlay);
        }

        let dirty = txn.publish_dirty();
        let txn_id = txn.txn_id;
        txn.commit_chain_commit(&self.shared.handle, commit_ts)?;
        if matches!(
            cut,
            Phase0ProbeCut::AfterChainCommitBeforeSecondaryInstall
                | Phase0ProbeCut::AfterChainCommitBeforeCommitTxn
        ) {
            return Ok(report);
        }

        install_pending_sec_index(
            &self.shared,
            &self.metadata_state,
            &mut overlay,
            sec_writes.to_vec(),
            &vis,
            commit_ts,
            txn_id,
        )?;

        install_pending_primary(
            &self.shared,
            &self.metadata_state,
            &mut overlay,
            primary_writes.to_vec(),
            &vis,
            commit_ts,
            txn_id,
        )?;
        if matches!(
            cut,
            Phase0ProbeCut::AfterPrimaryInstallBeforeOverlayCommit
                | Phase0ProbeCut::AfterInstallPendingPrimary
        ) {
            return Ok(report);
        }

        let mut root_neutral_overlay = if root_changing {
            let mut base_store = new_store(&self.shared);
            if let Err(e) = overlay.commit(&mut base_store, &self.shared.handle) {
                let _ = self.shared.handle.rollback_txn(mark);
                return Err(e);
            }
            None
        } else {
            Some(overlay)
        };
        if matches!(
            cut,
            Phase0ProbeCut::AfterOverlayCommitBeforeFlush | Phase0ProbeCut::AfterOverlayCommit
        ) {
            return Ok(report);
        }

        self.flush_under_journal_mutex()?;
        if root_changing {
            self.commit_legacy_header_frame()?;
        }
        if matches!(
            cut,
            Phase0ProbeCut::AfterStructuralFlushBeforePublish
                | Phase0ProbeCut::AfterFlushBeforeChainCommit
                | Phase0ProbeCut::AfterCommitTxnBeforePublish
        ) {
            return Ok(report);
        }

        report.pre_publish_visible = Some(self.phase0_probe_visible(ns, &inserted_id)?);
        // Phase 1 §10.3 — mirror the CRUD commit path: pass the txn's
        // dirty flags into the publish helper. A root-neutral probe
        // insert will reuse the existing Arc<PublishedCatalog>. `dirty`
        // was captured above before `commit_chain_commit()` consumed the txn.
        // Phase 5 §10.17.1 / US-006 — phase0_probe simulates the ordinary
        // CRUD commit path: pass `reserved_catalog_gen=None` so the new
        // published epoch inherits the prior `catalog_generation`.
        rebuild_and_publish_locked(&self.shared, &self.metadata_state, commit_ts, dirty, None)?;
        commit_pending_sec_index_states(
            &self.shared,
            &self.metadata_state,
            &sec_writes,
            commit_ts,
            txn_id,
        )?;
        if let Some(overlay) = root_neutral_overlay.as_mut() {
            commit_pending_primary_states_with_overlay(
                &self.shared,
                &self.metadata_state,
                overlay,
                &primary_writes,
                commit_ts,
                txn_id,
            )?;
        } else {
            commit_pending_primary_states(
                &self.shared,
                &self.metadata_state,
                &primary_writes,
                commit_ts,
                txn_id,
            )?;
        }
        report.publish_ts = Some((commit_ts.physical_ms, commit_ts.logical));
        report.post_publish_visible = Some(self.phase0_probe_visible(ns, &inserted_id)?);
        if let Some(overlay) = root_neutral_overlay {
            let mut base_store = new_store(&self.shared);
            overlay.commit(&mut base_store, &self.shared.handle)?;
            self.flush_under_journal_mutex()?;
            self.commit_legacy_header_frame()?;
        }
        if root_changing {
            crate::mvcc::metrics::record_crud_commit_root_changing();
        } else {
            crate::mvcc::metrics::record_crud_commit_root_neutral();
        }
        Ok(report)
    }
}
