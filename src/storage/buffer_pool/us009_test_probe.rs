//! US-009 test-only resident-chain probes.

use crate::error::{Error, Result};
use crate::mvcc::VersionEntry;

use super::BufferPool;

impl BufferPool {
    /// Return a cloned resident chain for US-009 integration tests.
    pub(crate) fn us009_chain_entries(&self, page: u32, key: &[u8]) -> Result<Vec<VersionEntry>> {
        let guard = self
            .inner_32k
            .lock()
            .map_err(|_| Error::Internal("buffer pool mutex poisoned".into()))?;
        let Some(&idx) = guard.page_map.get(&page) else {
            return Ok(Vec::new());
        };
        let frame = guard.frames[idx].as_ref().ok_or_else(|| {
            Error::Internal("page_map invariant: frame must exist at mapped slot".into())
        })?;
        Ok(frame
            .deltas
            .get(key)
            .map(|chain| chain.iter().cloned().collect())
            .unwrap_or_default())
    }
}
