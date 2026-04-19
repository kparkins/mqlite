//! Published read-side snapshot.
//!
//! Readers load an `Arc<PublishedSnapshot>` atomically via
//! `ArcSwap::load()` and use it to locate B-tree root pages and issue
//! a `ReadView` at `publish_ts`. Writers publish a new snapshot on
//! commit by `ArcSwap::store()`. Under v1-MWMR (PR 8) this is the
//! only read-path coordination point; readers never take the engine
//! write mutex.

use std::collections::HashMap;

use bson::Document;

use crate::mvcc::timestamp::Ts;
use crate::storage::catalog::IndexState;

/// Root-of-tree metadata for one namespace.
#[derive(Clone)]
pub(crate) struct NamespaceSnapshot {
    pub data_root_page: u32,
    pub data_root_level: u8,
    pub indexes: Vec<PublishedIndex>,
}

/// Stable fields of an `IndexEntry` as of the published snapshot.
#[derive(Clone)]
pub(crate) struct PublishedIndex {
    pub name: String,
    pub root_page: u32,
    pub root_level: u8,
    pub key_pattern: Document,
    pub unique: bool,
    pub sparse: bool,
    /// Lifecycle state. Query planning must skip any index whose state
    /// is not `Ready` — the contents may be incomplete.
    pub state: IndexState,
}

/// Latest atomically published view of the database.
#[derive(Clone)]
pub(crate) struct PublishedSnapshot {
    /// Commit timestamp of the txn that produced this snapshot.
    pub publish_ts: Ts,
    /// One entry per live namespace.
    pub namespaces: HashMap<String, NamespaceSnapshot>,
}
