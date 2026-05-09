//! Test-hook state attached to [`SharedState`](super::SharedState).

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicU8};
use std::sync::Mutex;
#[cfg(test)]
use std::sync::{Arc, Barrier};

use super::super::hidden_accessors::{
    BeforeLogReservationHook, CreateIndexBuildHook, WriteBodyEntryHook,
};

pub(crate) struct SharedStateTestHooks {
    /// Per-engine publish-pause rendezvous hook for commit-order tests.
    #[cfg(test)]
    pub publish_pause_hook: Mutex<Option<Arc<Barrier>>>,
    /// Per-engine counter for post-open recovery epoch stores.
    pub recovery_open_published_store_count: AtomicU64,
    /// Primary-install fault injector.
    pub us019_primary_install_failures: AtomicU8,
    /// Primary-install attempt counter.
    pub us019_primary_install_attempts: AtomicU64,
    /// Event order counter for committed-flip and publish-ready probes.
    pub us009_event_order_counter: AtomicU64,
    /// Order at which Pending entries flipped to Committed.
    pub us009_committed_flip_order: AtomicU64,
    /// Order at which the CRUD publish step became ready.
    pub us009_publish_ready_order: AtomicU64,
    /// One-shot failure after committed flip and before publish.
    pub us009_fail_after_committed_flip: AtomicU8,
    /// One-shot post-register cleanup failpoint.
    pub us026_post_register_failpoint: AtomicU8,
    /// Failure injected after log reservation and before dirty-page LSN stamp.
    pub fail_next_dirty_lsn_stamp: AtomicU8,
    /// Failure injected after dirty-page LSN stamp and before record write.
    pub fail_next_after_dirty_lsn_stamp: AtomicU8,
    /// Failure injected after durable record write and before committed flip.
    pub fail_next_after_durable_before_flip: AtomicU8,
    /// Namespace-keyed write-body entry rendezvous hooks.
    pub write_body_entry_hooks: Mutex<HashMap<String, VecDeque<WriteBodyEntryHook>>>,
    /// One-shot pause after Pending install and before log reservation.
    pub before_log_reservation_hook: Mutex<Option<BeforeLogReservationHook>>,
    /// Create-index build-scan rendezvous hooks.
    pub create_index_build_hooks: Mutex<HashMap<(String, String), VecDeque<CreateIndexBuildHook>>>,
    /// Monotonic ids for test-only rendezvous hooks.
    pub write_body_entry_hook_next_id: AtomicU64,
}

impl Default for SharedStateTestHooks {
    fn default() -> Self {
        Self {
            #[cfg(test)]
            publish_pause_hook: Mutex::new(None),
            recovery_open_published_store_count: AtomicU64::new(0),
            us019_primary_install_failures: AtomicU8::new(0),
            us019_primary_install_attempts: AtomicU64::new(0),
            us009_event_order_counter: AtomicU64::new(0),
            us009_committed_flip_order: AtomicU64::new(0),
            us009_publish_ready_order: AtomicU64::new(0),
            us009_fail_after_committed_flip: AtomicU8::new(0),
            us026_post_register_failpoint: AtomicU8::new(0),
            fail_next_dirty_lsn_stamp: AtomicU8::new(0),
            fail_next_after_dirty_lsn_stamp: AtomicU8::new(0),
            fail_next_after_durable_before_flip: AtomicU8::new(0),
            write_body_entry_hooks: Mutex::new(HashMap::new()),
            before_log_reservation_hook: Mutex::new(None),
            create_index_build_hooks: Mutex::new(HashMap::new()),
            write_body_entry_hook_next_id: AtomicU64::new(1),
        }
    }
}
