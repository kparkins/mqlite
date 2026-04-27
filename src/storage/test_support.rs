//! In-memory `PageSource` fixture shared by storage and MVCC unit tests.
//!
//! Tests across the crate need a journal-less buffer-pool backing store
//! with no real I/O. Before this module each consumer rolled its own
//! identical `MockIo` + `ArcIo` pair; this module is the single source of
//! truth.
//!
//! Buffer-pool stress tests that need read/write counters
//! (`buffer_pool::tests`) and the allocator unit tests
//! (`allocator_tests`) intentionally keep their own variants — their
//! shapes are not interchangeable with this fixture.
#![cfg(test)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use crate::error::{Error, Result};
use crate::storage::buffer_pool::{PageSize, PageSource};

/// In-memory page store backing `BufferPool` in unit tests.
#[derive(Default)]
pub(crate) struct MockIo {
    pub(crate) pages: StdMutex<HashMap<u32, Vec<u8>>>,
}

impl MockIo {
    /// Wrap a fresh empty `MockIo` in an `Arc` for shared ownership across
    /// the buffer pool and the test body.
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

/// Newtype wrapper so `Arc<MockIo>` can be passed as `Box<dyn PageSource>`.
pub(crate) struct ArcIo(pub(crate) Arc<MockIo>);

impl PageSource for ArcIo {
    fn read_page(&self, page: u32, _size: PageSize, buf: &mut [u8]) -> Result<()> {
        let pages = self
            .0
            .pages
            .lock()
            .map_err(|_| Error::Internal("mock io pages mutex poisoned".into()))?;
        if let Some(data) = pages.get(&page) {
            let n = buf.len().min(data.len());
            buf[..n].copy_from_slice(&data[..n]);
            if n < buf.len() {
                buf[n..].fill(0);
            }
        } else {
            buf.fill(0);
        }
        Ok(())
    }

    fn write_page(&self, page: u32, _size: PageSize, buf: &[u8]) -> Result<()> {
        self.0
            .pages
            .lock()
            .map_err(|_| Error::Internal("mock io pages mutex poisoned".into()))?
            .insert(page, buf.to_vec());
        Ok(())
    }
}
