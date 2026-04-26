//! Hidden Phase 0 write-envelope probes for `PagedEngine`.
//!
//! Kept out of `paged_engine.rs` so the production CRUD path stays readable.

use std::sync::atomic::Ordering;

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::mvcc::transaction::WriteTxn;
use crate::storage::buffer_pool::PageSize;
use crate::storage::phase0_probe::{Phase0ProbeCut, Phase0ProbeReport};
use crate::storage::txn_page_store::{PageOrigin, PageReservation, TxnOverlay};

use super::catalog_ops::{catalog_lock, new_store, rebuild_and_publish_locked};
use super::doc_ops;
use super::index_maint::{install_pending_primary, install_pending_sec_index};
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

    fn phase0_stop_after_overlay_commit(
        report: Phase0ProbeReport,
        txn: WriteTxn,
    ) -> Result<Phase0ProbeReport> {
        std::mem::forget(txn);
        Ok(report)
    }

    pub(super) fn phase0_probe_insert_impl(
        &self,
        ns: &str,
        doc: Document,
        cut: Phase0ProbeCut,
    ) -> Result<Phase0ProbeReport> {
        let md_read = self
            .metadata
            .read()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
        let ns_missing = catalog_lock(&md_read).get_collection(ns)?.is_none();
        if ns_missing {
            return Err(Error::CollectionNotFound {
                name: ns.to_owned(),
            });
        }

        let lane = self.lane_for(ns);
        let _lane_guard = self.acquire_lane(lane)?;
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
        let txn_id = self.shared.txn_counter.fetch_add(1, Ordering::Relaxed);
        let mut txn = WriteTxn::new(txn_id);

        let body_result =
            doc_ops::stage_insert_body(&self.shared, &md_read, &mut overlay, &mut txn, ns, doc);
        let inserted_id = match body_result {
            Ok(id) => id,
            Err(e) => {
                drop(txn);
                let _ = self.rollback_overlay_and_wal(overlay, mark);
                return Err(e);
            }
        };
        let root_changing = overlay.has_header_pre();

        let _commit = self
            .commit_seq
            .lock()
            .map_err(|_| Error::Internal("commit_seq mutex poisoned".into()))?;

        let sec_writes = std::mem::take(&mut txn.pending_sec_index);
        if let Err(e) = install_pending_sec_index(
            &self.shared,
            &md_read,
            &mut overlay,
            sec_writes.to_vec(),
            &mut txn,
        ) {
            drop(txn);
            let _ = self.rollback_overlay_and_wal(overlay, mark);
            return Err(e);
        }

        let primary_writes = std::mem::take(&mut txn.pending_primary);
        if primary_writes.is_empty() {
            drop(txn);
            let _ = self.rollback_overlay_and_wal(overlay, mark);
            return Err(Error::Internal(
                "phase0 probe insert produced no primary write".into(),
            ));
        }

        let commit_ts = match txn.allocate_commit_ts(&self.shared.oracle) {
            Ok(ts) => ts,
            Err(e) => {
                drop(txn);
                let _ = self.rollback_overlay_and_wal(overlay, mark);
                return Err(e);
            }
        };
        let mut report = Phase0ProbeReport {
            commit_ts: Some((commit_ts.physical_ms, commit_ts.logical)),
            publish_ts: None,
            pre_publish_visible: None,
            post_publish_visible: None,
        };
        if cut == Phase0ProbeCut::AfterAllocateCommitTs {
            return Self::phase0_stop_before_recovery(report, txn, overlay);
        }

        if let Err(e) = install_pending_primary(
            &self.shared,
            &md_read,
            &mut overlay,
            primary_writes.to_vec(),
            commit_ts,
            txn.txn_id,
        ) {
            drop(txn);
            let _ = self.rollback_overlay_and_wal(overlay, mark);
            return Err(e);
        }
        if cut == Phase0ProbeCut::AfterInstallPendingPrimary {
            return Self::phase0_stop_before_recovery(report, txn, overlay);
        }

        let mut base_store = new_store(&self.shared);
        if let Err(e) = overlay.commit(&mut base_store, &self.shared.handle) {
            drop(txn);
            let _ = self.shared.handle.rollback_txn(mark);
            return Err(e);
        }
        if cut == Phase0ProbeCut::AfterOverlayCommit {
            return Self::phase0_stop_after_overlay_commit(report, txn);
        }

        self.shared.handle.flush()?;
        if cut == Phase0ProbeCut::AfterFlushBeforeChainCommit {
            return Self::phase0_stop_after_overlay_commit(report, txn);
        }

        // Capture the dirty flags before `txn.commit()` consumes the txn;
        // the publish step below needs them to choose rebuild vs reuse.
        let dirty = txn.publish_dirty();
        let (_commit_ts, _installed, _sec_index) =
            txn.commit(&self.shared.oracle, &self.shared.handle)?;
        if cut == Phase0ProbeCut::AfterChainCommitBeforeCommitTxn {
            return Ok(report);
        }

        let db_page_count = self
            .shared
            .handle
            .allocator()
            .with_header(|h| h.total_page_count)?;
        let header_data = self
            .shared
            .handle
            .allocator()
            .with_header(|h| h.to_bytes())?;
        let emergency =
            self.shared
                .handle
                .commit_txn(0, PageSize::Small4k, &header_data, db_page_count)?;
        if emergency {
            crate::mvcc::metrics::record_emergency_checkpoint_trigger();
            let _ = self.shared.handle.emergency_checkpoint();
        }
        if cut == Phase0ProbeCut::AfterCommitTxnBeforePublish {
            return Ok(report);
        }

        report.pre_publish_visible = Some(self.phase0_probe_visible(ns, &inserted_id)?);
        // Phase 1 §10.3 — mirror the CRUD commit path: pass the txn's
        // dirty flags into the publish helper. A root-neutral probe
        // insert will reuse the existing Arc<PublishedCatalog>. `dirty`
        // was captured above before `txn.commit()` consumed the txn.
        rebuild_and_publish_locked(&self.shared, &md_read, commit_ts, dirty)?;
        report.publish_ts = Some((commit_ts.physical_ms, commit_ts.logical));
        report.post_publish_visible = Some(self.phase0_probe_visible(ns, &inserted_id)?);
        if root_changing {
            crate::mvcc::metrics::record_crud_commit_root_changing();
        } else {
            crate::mvcc::metrics::record_crud_commit_root_neutral();
        }
        Ok(report)
    }
}
