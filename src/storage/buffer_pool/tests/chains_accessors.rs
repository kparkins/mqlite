//! Test-only [`BufferPool`] chain snapshot accessors.

use std::sync::Arc;

use crate::error::{Error, Result};
use crate::mvcc::read_view::{ChainSnapshot, ReadView};

use super::BufferPool;

impl BufferPool {
    /// Build a [`ChainSnapshot`] from the per-key MVCC delta chains on
    /// leaf page `page`. Returns `None` if the page is not currently
    /// resident.
    pub(crate) fn snapshot_chains(
        &self,
        page: u32,
        view: Option<Arc<ReadView>>,
    ) -> Result<Option<ChainSnapshot>> {
        let guard = self
            .inner_32k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
        let Some(&idx) = guard.page_map.get(&page) else {
            return Ok(None);
        };
        let frame = guard.frames[idx].as_ref().ok_or_else(|| {
            Error::Internal("page_map invariant: frame must exist at mapped slot".into())
        })?;
        Ok(Some(ChainSnapshot::new(&frame.deltas, view)))
    }
}
