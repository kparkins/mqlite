//! TTL sweep: delete documents that have outlived a TTL index's
//! `expireAfterSeconds` window.
//!
//! ## Candidate matching
//!
//! mqlite's comparison operators are **not** type-bracketed: `compare_bson`
//! (via [`crate::keys::encode_key`]) imposes a single total order across all
//! BSON types, so a raw `{field: {$lt: <date>}}` filter would also match every
//! number, string, ObjectId, and boolean — all of which sort *before* the date
//! type byte (`TYPE_DATE = 0x80` in `src/keys/mod.rs`). That would wrongly
//! delete non-date documents.
//!
//! Instead the sweep scans candidates with `{field: {$exists: true}}` (merged
//! with the index's `partialFilterExpression` under `$and` when present) and
//! performs the date comparison **in code** on the resolved field value. The
//! check is array-aware: when the field holds an array, the document expires if
//! ANY element is a `DateTime` older than the cutoff (MongoDB uses the earliest
//! date, which is equivalent for the past-cutoff test). Non-date values and
//! missing fields never expire.
//!
//! ## Deletion path
//!
//! Expired `_id`s are collected per namespace and deleted through the ordinary
//! engine delete path via `{_id: {$in: [...]}}` (`many = true`), so journaling,
//! MVCC, and secondary-index maintenance all apply and no new storage primitive
//! is introduced. Racing writers are tolerated: a document deleted by another
//! writer between the scan and the `$in` delete simply does not match, and the
//! delete reports a smaller count without erroring.

use bson::{Bson, DateTime, Document};

use crate::error::Result;
use crate::options::FindOptions;
use crate::query::get_nested_field;
use crate::storage::catalog::IndexState;

use super::doc_helpers::now_millis;
use super::PagedEngine;

/// Milliseconds in one second.
const MILLIS_PER_SECOND: i64 = 1000;

/// One TTL index's sweep parameters, captured from the published snapshot.
struct TtlTarget {
    /// Qualified namespace name (e.g. `"app.events"`).
    namespace: String,
    /// The single indexed field (dotted path supported).
    field: String,
    /// TTL window in seconds.
    expire_after_seconds: i64,
    /// Optional partial-filter expression to restrict expiry to matching docs.
    partial_filter_expression: Option<Document>,
}

impl PagedEngine {
    /// Sweep every TTL index in every collection, deleting expired documents.
    ///
    /// Returns the total number of documents deleted across all TTL indexes.
    ///
    /// Deletes route through the ordinary delete path, so the sweep is safe to
    /// run concurrently with writers and is idempotent with respect to
    /// already-deleted documents.
    ///
    /// # Errors
    ///
    /// Propagates any error from the underlying scan or delete path.
    pub(crate) fn sweep_expired(&self) -> Result<u64> {
        self.shared.check_engine_not_poisoned()?;
        let targets = self.collect_ttl_targets();
        let now = now_millis();
        let mut total_deleted = 0_u64;
        for target in targets {
            total_deleted += self.sweep_one_ttl_index(&target, now)?;
        }
        Ok(total_deleted)
    }

    /// Snapshot the TTL indexes from the published catalog.
    ///
    /// Only `Ready` indexes carrying `expireAfterSeconds` are returned; building
    /// indexes are skipped because their contents may be incomplete.
    fn collect_ttl_targets(&self) -> Vec<TtlTarget> {
        let snap = self.shared.load_published();
        let mut targets = Vec::new();
        for ns_snap in snap.catalog.namespaces.values() {
            let ns_name = match snap
                .catalog
                .namespace_id_by_name
                .iter()
                .find(|(_, id)| **id == ns_snap.id)
            {
                Some((name, _)) => name.clone(),
                None => continue,
            };
            for index in &ns_snap.indexes {
                let Some(seconds) = index.expire_after_seconds else {
                    continue;
                };
                if index.state != IndexState::Ready {
                    continue;
                }
                // TTL is validated as single-field at create time; take the
                // sole key. Skip defensively if the pattern is unexpectedly
                // empty.
                let Some(field) = index.key_pattern.keys().next() else {
                    continue;
                };
                targets.push(TtlTarget {
                    namespace: ns_name.clone(),
                    field: field.to_owned(),
                    expire_after_seconds: seconds,
                    partial_filter_expression: index.partial_filter_expression.clone(),
                });
            }
        }
        targets
    }

    /// Sweep a single TTL index: scan candidates, collect expired `_id`s, and
    /// delete them. Returns the number of documents deleted.
    fn sweep_one_ttl_index(&self, target: &TtlTarget, now_millis: i64) -> Result<u64> {
        let cutoff_millis = now_millis.saturating_sub(
            target
                .expire_after_seconds
                .saturating_mul(MILLIS_PER_SECOND),
        );

        let scan_filter = build_scan_filter(&target.field, &target.partial_filter_expression);
        let (candidates, _explain) =
            self.find(&target.namespace, &scan_filter, &FindOptions::default())?;

        let mut expired_ids = Vec::new();
        for doc in &candidates {
            let Some(value) = get_nested_field(doc, &target.field) else {
                continue;
            };
            if value_is_expired(value, cutoff_millis) {
                if let Some(id) = doc.get("_id") {
                    expired_ids.push(id.clone());
                }
            }
        }

        if expired_ids.is_empty() {
            return Ok(0);
        }

        let delete_filter = bson::doc! { "_id": { "$in": Bson::Array(expired_ids) } };
        let result = self.delete(&target.namespace, &delete_filter, true)?;
        Ok(result.deleted_count)
    }
}

/// Build the candidate-scan filter for a TTL field.
///
/// Without a partial filter this is `{field: {$exists: true}}`. With one it is
/// `{$and: [{field: {$exists: true}}, <pfe>]}` so only PFE-matching documents
/// are considered for expiry (matching MongoDB's partial-TTL semantics).
fn build_scan_filter(field: &str, pfe: &Option<Document>) -> Document {
    let exists = bson::doc! { field: { "$exists": true } };
    match pfe {
        None => exists,
        Some(pfe) => bson::doc! {
            "$and": Bson::Array(vec![
                Bson::Document(exists),
                Bson::Document(pfe.clone()),
            ]),
        },
    }
}

/// Return whether `value` makes the document expired given `cutoff_millis`.
///
/// A scalar `DateTime` expires when it is strictly older than the cutoff. An
/// array expires when ANY element is a `DateTime` older than the cutoff. Every
/// other value type (and a non-date array element) never expires.
fn value_is_expired(value: &Bson, cutoff_millis: i64) -> bool {
    match value {
        Bson::DateTime(dt) => date_is_expired(*dt, cutoff_millis),
        Bson::Array(elements) => elements.iter().any(|element| match element {
            Bson::DateTime(dt) => date_is_expired(*dt, cutoff_millis),
            _ => false,
        }),
        _ => false,
    }
}

/// Return whether `dt` is strictly older than the cutoff.
fn date_is_expired(dt: DateTime, cutoff_millis: i64) -> bool {
    dt.timestamp_millis() < cutoff_millis
}
