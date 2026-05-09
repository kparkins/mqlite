//! Read-path epoch-load assertions for unit tests.

thread_local! {
    pub(crate) static EPOCH_LOAD_COUNT: std::cell::Cell<u32> =
        const { std::cell::Cell::new(0) };
}

/// RAII guard that checks how many times a read operation loads the published
/// epoch.
#[derive(Debug)]
pub(crate) struct ReadOpScope {
    start: u32,
    limit: u32,
}

impl ReadOpScope {
    /// Begin a scope that tolerates up to `limit` epoch loads.
    pub(crate) fn new(limit: u32) -> Self {
        let start = EPOCH_LOAD_COUNT.with(|c| c.get());
        Self { start, limit }
    }
}

impl Drop for ReadOpScope {
    fn drop(&mut self) {
        let end = EPOCH_LOAD_COUNT.with(|c| c.get());
        let delta = end.saturating_sub(self.start);
        assert!(
            delta <= self.limit,
            "operation performed {} epoch loads, limit {}",
            delta,
            self.limit
        );
    }
}
