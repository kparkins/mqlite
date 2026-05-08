//! Hidden Phase 0 write-envelope probes for `PagedEngine`.
//!
//! Kept out of `paged_engine.rs` so the production CRUD path stays readable.

use std::sync::Arc;

use bson::{Bson, Document};

use crate::error::{EngineFatalReason, Error, Result};
use crate::journal::log_file::LogRecordDraft;
use crate::mvcc::transaction::WriteTxn;
use crate::options::FindOptions;
use crate::storage::write_crash_cut_contract::{Phase0ProbeCut, Phase0ProbeReport};

use super::publish::rebuild_and_publish;
use super::doc_ops;
use super::index_maint::{
    flip_pending_to_committed_for, install_pending_primary, install_pending_sec_index,
};
use super::visibility::WriteVisibility;
use super::PagedEngine;

impl PagedEngine {
    fn crash_cut_probe_visible(&self, ns: &str, inserted_id: &Bson) -> Result<bool> {
        let filter = bson::doc! { "_id": inserted_id.clone() };
        let (docs, _explain) = doc_ops::find(self, ns, &filter, &FindOptions::default())?;
        Ok(!docs.is_empty())
    }

    fn phase0_stop_before_recovery(
        report: Phase0ProbeReport,
        txn: WriteTxn,
    ) -> Result<Phase0ProbeReport> {
        std::mem::forget(txn);
        Ok(report)
    }

    pub(crate) fn crash_cut_probe_insert(
        &self,
        ns: &str,
        doc: Document,
        cut: Phase0ProbeCut,
    ) -> Result<Phase0ProbeReport> {
        let _checkpoint_writer_admission = self.shared.checkpoint_admission.admit_writer()?;
        let _crud_read = self
            .metadata
            .read()
            .map_err(|_| Error::Internal("metadata RwLock poisoned".into()))?;
        if self.metadata_state.catalog_lock()
            .get_collection(ns)?
            .is_none()
        {
            return Err(Error::CollectionNotFound {
                name: ns.to_owned(),
            });
        }

        let vis = WriteVisibility::new(&self.shared, ns)?;
        let txn_id = vis.read_view.txn_id;
        let mut txn = WriteTxn::new(txn_id);
        let inserted_id = doc_ops::stage_insert(
            &self.shared,
            &self.metadata_state,
            &mut txn,
            &vis,
            ns,
            doc,
        )?;
        let mut report = Phase0ProbeReport {
            commit_ts: None,
            publish_ts: None,
            pre_publish_visible: None,
            post_publish_visible: None,
        };
        if cut == Phase0ProbeCut::AfterStageBeforeCommitTs {
            return Self::phase0_stop_before_recovery(report, txn);
        }

        let sec_writes = std::mem::take(&mut txn.pending_sec_index);
        let primary_writes = std::mem::take(&mut txn.pending_primary);
        if primary_writes.is_empty() {
            drop(txn);
            return Err(Error::Internal(
                "phase0 probe insert produced no primary write".into(),
            ));
        }

        let slot = self.register_ordinary_crud_slot()?;
        let commit_ts = slot.commit_ts();
        txn.commit_ts.set(Some(commit_ts));
        report.commit_ts = Some((commit_ts.physical_ms, commit_ts.logical));
        if matches!(cut, Phase0ProbeCut::AfterCommitTsBeforeLogicalFrame) {
            return Self::phase0_stop_before_recovery(report, txn);
        }

        let frame = txn.build_logical_txn_frame(&self.shared.handle, &primary_writes, &sec_writes);
        if cut == Phase0ProbeCut::AfterLogicalFrameBeforeReservation {
            return Self::phase0_stop_before_recovery(report, txn);
        }

        let dirty = txn.publish_dirty;
        let root_changing = false;
        let logical_payload = match frame.encode() {
            Ok(bytes) => bytes,
            Err(e) => {
                drop(txn);
                return Err(self.cleanup_registered_pre_durable_failure(txn_id, slot, None, e));
            }
        };
        let sec_pages = match install_pending_sec_index(
            &self.shared,
            &self.metadata_state,
            sec_writes.to_vec(),
            &vis,
            commit_ts,
            txn_id,
        ) {
            Ok(pages) => pages,
            Err(e) => {
                return Err(self.cleanup_registered_pre_durable_failure(txn_id, slot, None, e));
            }
        };
        let (primary_pages, primary_structural_tree_change) = match install_pending_primary(
            &self.shared,
            &self.metadata_state,
            primary_writes.to_vec(),
            &vis,
            commit_ts,
            txn_id,
        ) {
            Ok(result) => result,
            Err(e) => {
                return Err(self.cleanup_registered_pre_durable_failure(txn_id, slot, None, e));
            }
        };
        let root_changing = root_changing | primary_structural_tree_change;
        let mut pending_pages = sec_pages;
        pending_pages.extend(primary_pages);
        if matches!(cut, Phase0ProbeCut::AfterPendingInstallBeforeReservation) {
            return Self::phase0_stop_before_recovery(report, txn);
        }
        let prepared = match txn.prepare_chain_commit_payload(&self.shared.handle, commit_ts) {
            Ok(prepared) => prepared,
            Err(e) => {
                return Err(self.cleanup_registered_pre_durable_failure(txn_id, slot, None, e));
            }
        };
        let _pending = prepared.pending;
        let _pending_sec_index = prepared.pending_sec_index;

        let draft = LogRecordDraft::crud(
            txn_id,
            slot.publish_seq(),
            commit_ts,
            logical_payload,
            prepared.payload,
        );
        let reserved = match self.shared.handle.reserve_log_record(draft) {
            Ok(reserved) => reserved,
            Err(e) => {
                return Err(self.cleanup_registered_pre_durable_failure(txn_id, slot, None, e));
            }
        };
        let commit_end_lsn = reserved.end_lsn();
        if let Err(e) = self
            .shared
            .handle
            .stamp_dirty_pages_lsn(&pending_pages, commit_end_lsn)
        {
            return Err(self.poison_after_reserved_log_failure(&reserved, e));
        }
        let written_end_lsn = match reserved.write_and_mark() {
            Ok(end_lsn) => end_lsn,
            Err(e) => return Err(self.poison_after_log_manager_failure(e)),
        };
        debug_assert_eq!(written_end_lsn, commit_end_lsn);
        if matches!(cut, Phase0ProbeCut::AfterLogRecordWriteBeforeDurabilityWait) {
            return Ok(report);
        }

        self.wait_for_commit_durability(commit_end_lsn)?;
        if matches!(cut, Phase0ProbeCut::AfterDurabilityWaitBeforePublish) {
            return Ok(report);
        }

        report.pre_publish_visible = Some(self.crash_cut_probe_visible(ns, &inserted_id)?);
        flip_pending_to_committed_for(&self.shared, txn_id, commit_ts, &pending_pages)
            .map_err(|_| self.engine_fatal(EngineFatalReason::PostDurablePendingFlipFailure))?;
        let shared = Arc::clone(&self.shared);
        let metadata_state = Arc::clone(&self.metadata_state);
        self.shared
            .publish_sequencer
            .mark_ready(slot, move |publish_ts| {
                rebuild_and_publish(&shared, &metadata_state, publish_ts, dirty, None)
            })?;
        report.publish_ts = Some((commit_ts.physical_ms, commit_ts.logical));
        report.post_publish_visible = Some(self.crash_cut_probe_visible(ns, &inserted_id)?);

        if root_changing {
            crate::mvcc::metrics::record_crud_commit_root_changing();
        } else {
            crate::mvcc::metrics::record_crud_commit_root_neutral();
        }
        Ok(report)
    }
}
