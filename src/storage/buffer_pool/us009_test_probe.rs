//! US-009 test-only resident-chain probes.

use crate::error::Result;
use crate::mvcc::VersionEntry;

use super::BufferPool;

impl BufferPool {
    /// Return a cloned resident chain for US-009 integration tests.
    pub(crate) fn us009_chain_entries(&self, page: u32, key: &[u8]) -> Result<Vec<VersionEntry>> {
        let Some(latched) = self.pin_resident_32k_for_read(page)? else {
            return Ok(Vec::new());
        };
        // SAFETY: the resident frame is pinned and protected by the shared
        // page latch held by `latched` while this probe clones the chain.
        let frame = unsafe { &*latched.frame_ptr };
        Ok(frame
            .deltas
            .get(key)
            .map(|chain| chain.iter().cloned().collect())
            .unwrap_or_default())
    }
}
