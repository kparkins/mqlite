//! Empty test-hook state for production builds.

#[derive(Default)]
pub(crate) struct SharedStateTestHooks;

impl SharedStateTestHooks {
    /// Construct the (empty) production test-hook state. Mirrors the fielded
    /// test/test-hooks variant so `SharedState` construction is cfg-agnostic.
    pub(crate) fn new() -> Self {
        SharedStateTestHooks
    }
}
