//! Test-only [`BufferPool`] chain reconciliation helper.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::mvcc::metrics;
use crate::mvcc::read_view::ReadViewRegistry;
use crate::mvcc::timestamp::Ts;
use crate::mvcc::version::VersionEntry;
use crate::storage::allocator::AllocatorHandle;
use crate::storage::buffer_pool::{BufferPool, ReplaceLeafError, RetainedLeafChains};
use crate::storage::reconcile::driver::{TreeIdent, TreeKind};

const RECONCILE_COMPAT_COLLECTION_ID: i64 = 0;

impl BufferPool {
    /// Reconcile the per-key delta chains on leaf page `page`.
    ///
    /// Walks every chain on the frame and drops entries whose `stop_ts`
    /// is `<= oldest_required_ts`; no live reader can see them.
    ///
    /// The partition mutex is released before `drain_free_queue`, so the
    /// allocator-state mutex is never nested under a partition mutex.
    pub(crate) fn reconcile(
        &self,
        page: u32,
        registry: &ReadViewRegistry,
        allocator: &AllocatorHandle,
    ) -> Result<usize> {
        let ort = registry.oldest_required_ts();

        let Some((new_base, retained_chains, dropped)) = ({
            let guard = self
                .inner_32k
                .lock()
                .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
            let Some(&idx) = guard.page_map.get(&page) else {
                return Ok(0);
            };
            let frame = guard.frames[idx].as_ref().ok_or_else(|| {
                Error::Internal("page_map invariant: frame must exist at mapped slot".into())
            })?;

            let mut dropped_count = 0usize;
            let mut retained_chains = RetainedLeafChains::new();

            for (key, chain_arc) in &frame.deltas {
                let before = chain_arc.len();
                let retained: VecDeque<VersionEntry> = chain_arc
                    .iter()
                    .filter(|entry| entry.stop_ts == Ts::MAX || entry.stop_ts > ort)
                    .cloned()
                    .collect();
                dropped_count += before - retained.len();

                if !retained.is_empty() {
                    retained_chains.insert(key.clone(), Arc::new(retained));
                }
            }

            Some((
                frame.data.load_full().as_ref().clone(),
                retained_chains,
                dropped_count,
            ))
        }) else {
            return Ok(0);
        };

        if dropped > 0 {
            let pin = self
                .pin_leaf_for_reconcile(
                    TreeIdent {
                        collection_id: RECONCILE_COMPAT_COLLECTION_ID,
                        kind: TreeKind::Primary,
                    },
                    page,
                )
                .map_err(|err| match err {
                    ReplaceLeafError::NotResident => {
                        Error::Internal("buffer pool reconcile: resident frame disappeared".into())
                    }
                    ReplaceLeafError::NotLeaf => {
                        Error::Internal("buffer pool reconcile: target frame is not a leaf".into())
                    }
                })?;
            let mut pin = pin;
            self.replace_leaf_and_chains(&mut pin, new_base, retained_chains)
                .map_err(|err| match err {
                    ReplaceLeafError::NotResident => {
                        Error::Internal("buffer pool reconcile: resident frame disappeared".into())
                    }
                    ReplaceLeafError::NotLeaf => Error::Internal(
                        "buffer pool reconcile: replacement frame is not a leaf".into(),
                    ),
                })?;
        }

        metrics::record_reconcile_entries_dropped(dropped as u64);
        metrics::set_deferred_free_queue_depth(allocator.page_lifetime_queue().depth() as u64);

        allocator.drain_free_queue(self.io.as_ref())?;

        Ok(dropped)
    }
}
