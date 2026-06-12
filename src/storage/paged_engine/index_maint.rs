//! Secondary-index maintenance + pending-write installation facade.
//!
//! R7 split: the bodies that used to live here were moved VERBATIM into
//! focused sibling modules. This module stays as the facade so existing
//! `index_maint::X` paths across the engine and its tests keep resolving
//! unchanged:
//!
//! - [`pending_install`] — `classify_delta_install`, `install_pending_*`, and
//!   the full Pending→Committed/Aborted flip machinery.
//! - [`index_write_maint`] — per-write secondary maintenance
//!   (`maintain_secondary_on_*`).
//! - [`index_read_helpers`] — read-side helpers (`index_bounds_free`,
//!   `index_entry_id_free`).
//! - [`checkpoint_materialize`] — `materialize_*_deltas_for_checkpoint` and the
//!   `apply_*_checkpoint_delta` folders (guarded by the chain-free O(n²) fix).
//! - [`index_ddl`] — the create/drop index lifecycle (`create_index`,
//!   `drop_index`, `list_indexes`, `CreateIndexReservation`, `ReserveOutcome`).

pub(super) use super::checkpoint_materialize::{
    materialize_primary_deltas_for_checkpoint, materialize_ready_secondary_deltas_for_checkpoint,
};
pub(super) use super::index_ddl::{create_index, drop_index, list_indexes};
// `ReserveOutcome` is produced and consumed inside `index_ddl`; the only
// callers that reach it through the `index_maint` facade are the `#[cfg(test)]`
// recovery / pending-write harnesses, so gate the re-export to avoid an
// unused-import warning in the non-test lib build. (`CreateIndexReservation`
// has no facade consumers at all — import it from `index_ddl` directly.)
#[cfg(test)]
pub(super) use super::index_ddl::ReserveOutcome;
pub(super) use super::index_read_helpers::{index_bounds_free, index_entry_id_free};
pub(super) use super::index_write_maint::{
    maintain_secondary_on_delete, maintain_secondary_on_insert,
    maintain_secondary_on_insert_snapshot, maintain_secondary_on_update,
};
pub(super) use super::pending_install::{
    flip_pending_to_aborted_for, flip_pending_to_committed_for, install_pending_primary,
    install_pending_sec_index,
};
