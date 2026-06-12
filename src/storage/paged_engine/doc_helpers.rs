//! Pure document helpers shared across `doc_ops`: id assignment, validation,
//! projection, sorting, and unique-constraint checks.

use std::time::{SystemTime, UNIX_EPOCH};

use bson::{Bson, Document};

use crate::error::{Error, Result};
use crate::keys::encode_key;
use crate::mvcc::read_view::ReadView;
use crate::mvcc::transaction::{PrimaryOp, PrimaryWrite};
use crate::query::get_nested_field;
use crate::storage::btree::{BTree, BTreePageStore, HistoryProbe};
use crate::storage::oid::ObjectIdGenerator;

/// Return current Unix milliseconds.
pub(in crate::storage::paged_engine) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Ensure a document has an `_id` field.  Auto-assigns an [`ObjectId`] if absent.
pub(in crate::storage::paged_engine) fn ensure_id(doc: &mut Document) -> Bson {
    if let Some(id) = doc.get("_id") {
        id.clone()
    } else {
        let oid = Bson::ObjectId(ObjectIdGenerator::generate());
        doc.insert("_id", oid.clone());
        oid
    }
}

/// Validate that an index key pattern does not request an unsupported index type.
///
/// Rejects `text`, `2d`, `2dsphere`, and `hashed` indexes (not yet implemented).
pub(in crate::storage::paged_engine) fn validate_index_keys(keys: &Document) -> Result<()> {
    const SUGGESTION: &str = "Supported: single-field, compound, unique, sparse, multikey, \
         and partial indexes. Text, geospatial, hashed, and TTL indexes are \
         planned for a future release.";

    for (_field, value) in keys {
        if let Bson::String(t) = value {
            if matches!(t.as_str(), "text" | "2d" | "2dsphere" | "hashed") {
                return Err(Error::UnsupportedIndexOption {
                    option: t.to_owned(),
                    suggestion: SUGGESTION.to_owned(),
                });
            }
        }
    }
    Ok(())
}

/// Check unique index constraints against MVCC-visible rows and staged writes.
///
/// `unique_specs` is a list of `(index_name, fields, sparse)` for each unique
/// index. If any visible or same-transaction document matches the new doc on
/// all indexed fields with a different `_id`, returns [`Error::DuplicateKey`].
pub(in crate::storage::paged_engine) fn check_unique_constraints_mvcc<S: BTreePageStore>(
    tree: &BTree<S>,
    unique_specs: &[(String, Vec<String>, bool)],
    new_doc: &Document,
    view: &ReadView,
    history: Option<&dyn HistoryProbe>,
    pending: &[PrimaryWrite],
    ns: &str,
) -> Result<()> {
    if unique_specs.is_empty() {
        return Ok(());
    }

    let null_encoded = encode_key(&Bson::Null);
    let new_id = new_doc.get("_id").unwrap_or(&Bson::Null);

    for (idx_name, fields, sparse) in unique_specs {
        let encode_fields = |out: &mut Vec<Vec<u8>>, doc: &Document| {
            out.clear();
            out.extend(
                fields
                    .iter()
                    .map(|f| encode_key(doc.get(f.as_str()).unwrap_or(&Bson::Null))),
            );
        };
        let duplicate_key = || Error::DuplicateKey {
            detail: format!(
                "E11000 duplicate key error — unique index '{}': dup key {{{}}}",
                idx_name,
                fields
                    .iter()
                    .map(|f| format!("{}: {:?}", f, new_doc.get(f.as_str())))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        };

        let mut new_encoded = Vec::with_capacity(fields.len());
        encode_fields(&mut new_encoded, new_doc);

        if *sparse && new_encoded.iter().all(|v| v == &null_encoded) {
            continue;
        }

        let pairs = tree.range_scan_mvcc(None, None, view, history)?;
        let mut existing_encoded = Vec::with_capacity(fields.len());
        for (_, bson_bytes) in pairs {
            let existing: Document =
                bson::from_slice(&bson_bytes).map_err(Error::BsonDeserialization)?;

            encode_fields(&mut existing_encoded, &existing);
            if new_encoded == existing_encoded
                && existing.get("_id").unwrap_or(&Bson::Null) != new_id
            {
                return Err(duplicate_key());
            }
        }

        for staged in pending.iter().filter(|staged| staged.ns.as_str() == ns) {
            let data = match &staged.op {
                PrimaryOp::Insert { data } | PrimaryOp::Update { data } => data,
                PrimaryOp::Delete => continue,
            };
            let existing: Document = bson::from_slice(data).map_err(Error::BsonDeserialization)?;

            encode_fields(&mut existing_encoded, &existing);
            if new_encoded == existing_encoded
                && existing.get("_id").unwrap_or(&Bson::Null) != new_id
            {
                return Err(duplicate_key());
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Sort / projection helpers (replicated from engine.rs for local use)
// ---------------------------------------------------------------------------

pub(crate) fn compare_docs(
    a: &Document,
    b: &Document,
    sort: &Document,
) -> std::cmp::Ordering {
    for (field, dir) in sort {
        let ascending = !matches!(dir, Bson::Int32(-1) | Bson::Int64(-1));
        let av = get_nested_field(a, field).cloned().unwrap_or(Bson::Null);
        let bv = get_nested_field(b, field).cloned().unwrap_or(Bson::Null);
        let ord = encode_key(&av).cmp(&encode_key(&bv));
        if ord == std::cmp::Ordering::Equal {
            continue;
        }
        return if ascending { ord } else { ord.reverse() };
    }
    std::cmp::Ordering::Equal
}

/// Dotted path separator used by MongoDB projection field names.
const PATH_SEP: char = '.';

/// A node in the projection path trie.
///
/// Each node represents one path segment. A node is `terminal` when a
/// projection spec named exactly that path (e.g. `"a.b"` makes the `b`
/// node under `a` terminal). Children are kept in an insertion-ordered
/// `Vec` so shared-prefix specs merge under one parent (spec point 3).
///
/// MongoDB raises a "Path collision" error when a projection names both a
/// prefix and a path beneath it (e.g. `{"a": 1, "a.b": 1}`). Because
/// [`apply_projection_to_doc`] is infallible (its callers in `aggregate.rs`
/// and `read_exec.rs` do not thread a `Result`), collisions are resolved
/// **last-spec-wins** instead of erroring: a later child spec clears an
/// earlier terminal, and a later terminal spec clears earlier children.
#[derive(Default)]
struct ProjTrie {
    terminal: bool,
    children: Vec<(String, ProjTrie)>,
}

impl ProjTrie {
    /// Insert a dotted `path` into the trie, marking its final node terminal.
    ///
    /// Applies last-spec-wins collision resolution: descending through an
    /// existing terminal clears that terminal (a child now overrides it), and
    /// marking a node terminal clears any children it had accumulated.
    fn insert(&mut self, path: &str) {
        let mut node = self;
        let mut parts = path.split(PATH_SEP).peekable();
        while let Some(seg) = parts.next() {
            let is_last = parts.peek().is_none();
            if is_last {
                // Last-spec-wins: a terminal prefix drops any earlier
                // children projected beneath it.
                node = node.child_entry(seg);
                node.terminal = true;
                node.children.clear();
                return;
            }
            // Descending past a node clears its terminal flag so a deeper
            // child spec overrides an earlier prefix spec.
            node = node.child_entry(seg);
            node.terminal = false;
        }
    }

    /// Return a mutable reference to the child node for `seg`, inserting an
    /// empty node (preserving insertion order) when absent.
    fn child_entry(&mut self, seg: &str) -> &mut ProjTrie {
        let pos = match self.children.iter().position(|(k, _)| k == seg) {
            Some(pos) => pos,
            None => {
                self.children.push((seg.to_owned(), ProjTrie::default()));
                self.children.len() - 1
            }
        };
        &mut self.children[pos].1
    }

    /// Look up the immediate child node for `seg`, if any.
    fn child(&self, seg: &str) -> Option<&ProjTrie> {
        self.children
            .iter()
            .find(|(k, _)| k == seg)
            .map(|(_, node)| node)
    }
}

/// Project a single BSON value against a non-terminal trie `node` in
/// inclusion mode, returning the projected value to keep, or `None` when the
/// value must be omitted from the parent.
///
/// - Document: keep only fields named by the node (recursing for subnodes);
///   preserves the document's own field order (spec point 6).
/// - Array: project each element; document elements are retained even when
///   they become empty `{}`; non-document elements are removed (spec point 1).
/// - Scalar at a prefix position: omitted (spec point 1).
fn project_value_include(value: &Bson, node: &ProjTrie) -> Option<Bson> {
    match value {
        Bson::Document(sub) => Some(Bson::Document(project_doc_include(sub, node))),
        Bson::Array(arr) => {
            let projected = arr
                .iter()
                .filter_map(|elem| match elem {
                    Bson::Document(sub) => {
                        Some(Bson::Document(project_doc_include(sub, node)))
                    }
                    _ => None,
                })
                .collect();
            Some(Bson::Array(projected))
        }
        _ => None,
    }
}

/// Build an inclusion-projected document from `doc` against the trie `node`,
/// preserving `doc`'s field order.
fn project_doc_include(doc: &Document, node: &ProjTrie) -> Document {
    let mut result = Document::new();
    for (key, value) in doc {
        let Some(child) = node.child(key) else {
            continue;
        };
        if child.terminal {
            result.insert(key, value.clone());
        } else if let Some(projected) = project_value_include(value, child) {
            result.insert(key, projected);
        }
    }
    result
}

/// Apply an exclusion-projected pass over `doc` in place against `node`.
///
/// For each field named by a child: a terminal child removes the field; a
/// non-terminal child recurses into the field's subdocument (or each document
/// element of an array). Fields not named by the trie pass through unchanged
/// (spec point 2).
fn project_doc_exclude(doc: &mut Document, node: &ProjTrie) {
    for (seg, child) in &node.children {
        if child.terminal {
            doc.remove(seg);
            continue;
        }
        match doc.get_mut(seg) {
            Some(Bson::Document(sub)) => project_doc_exclude(sub, child),
            Some(Bson::Array(arr)) => {
                for elem in arr.iter_mut() {
                    if let Bson::Document(sub) = elem {
                        project_doc_exclude(sub, child);
                    }
                }
            }
            _ => {}
        }
    }
}

/// Apply a MongoDB find projection to a single document.
///
/// Supports dotted paths (`{"a.b": 1}`) at arbitrary depth (spec point 7),
/// including projection into arrays of embedded documents. Inclusion mode is
/// selected when any non-`_id` projection value is truthy (spec point 5);
/// `_id` is retained in inclusion mode unless explicitly excluded.
///
/// Path collisions (`{"a": 1, "a.b": 1}`) — which MongoDB rejects — are
/// resolved last-spec-wins here because this function is infallible; see
/// [`ProjTrie`].
pub(crate) fn apply_projection_to_doc(mut doc: Document, proj: &Document) -> Document {
    let is_inclusion = proj
        .iter()
        .filter(|(k, _)| k.as_str() != "_id")
        .any(|(_, v)| !matches!(v, Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false)));

    let explicit_id_excl = proj
        .get("_id")
        .is_some_and(|v| matches!(v, Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false)));

    if is_inclusion {
        let mut trie = ProjTrie::default();
        for (k, v) in proj {
            if k == "_id" {
                continue;
            }
            if !matches!(v, Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false)) {
                trie.insert(k);
            }
        }

        let mut result = Document::new();
        if !explicit_id_excl {
            if let Some(id) = doc.get("_id") {
                result.insert("_id", id.clone());
            }
        }
        // Walk the document in its own field order (spec point 6), skipping
        // `_id` which is handled above to honor its inclusion override.
        for (key, value) in &doc {
            if key == "_id" {
                continue;
            }
            let Some(child) = trie.child(key) else {
                continue;
            };
            if child.terminal {
                result.insert(key, value.clone());
            } else if let Some(projected) = project_value_include(value, child) {
                result.insert(key, projected);
            }
        }
        result
    } else {
        let mut trie = ProjTrie::default();
        for (k, _) in proj {
            trie.insert(k);
        }
        project_doc_exclude(&mut doc, &trie);
        doc
    }
}

#[cfg(test)]
mod tests {
    use super::apply_projection_to_doc;
    use bson::{doc, Bson};

    #[test]
    fn nested_inclusion_keeps_only_named_subfield() {
        let input = doc! { "_id": 1, "a": { "b": 2, "c": 3 }, "d": 4 };
        let out = apply_projection_to_doc(input, &doc! { "a.b": 1 });
        assert_eq!(out, doc! { "_id": 1, "a": { "b": 2 } });
    }

    #[test]
    fn nested_inclusion_omits_scalar_at_prefix() {
        let input = doc! { "_id": 1, "a": 7 };
        let out = apply_projection_to_doc(input, &doc! { "a.b": 1 });
        assert_eq!(out, doc! { "_id": 1 });
    }

    #[test]
    fn nested_exclusion_removes_only_named_subfield() {
        let input = doc! { "_id": 1, "a": { "b": 2, "c": 3 }, "d": 4 };
        let out = apply_projection_to_doc(input, &doc! { "a.b": 0 });
        assert_eq!(out, doc! { "_id": 1, "a": { "c": 3 }, "d": 4 });
    }

    #[test]
    fn array_of_docs_inclusion_retains_empty_and_drops_non_docs() {
        let input = doc! {
            "_id": 1,
            "a": [
                doc! { "b": 1, "x": 9 },
                doc! { "x": 9 },
                Bson::Int32(7),
            ],
        };
        let out = apply_projection_to_doc(input, &doc! { "a.b": 1 });
        // Document elements project to only `b`; the doc without `b`
        // becomes empty `{}` and is retained; the scalar `7` is removed.
        assert_eq!(
            out,
            doc! { "_id": 1, "a": [doc! { "b": 1 }, doc! {}] }
        );
    }

    #[test]
    fn array_of_docs_exclusion_removes_subfield_per_element() {
        let input = doc! {
            "_id": 1,
            "a": [
                doc! { "b": 1, "c": 2 },
                doc! { "c": 3 },
                Bson::Int32(7),
            ],
        };
        let out = apply_projection_to_doc(input, &doc! { "a.b": 0 });
        assert_eq!(
            out,
            doc! { "_id": 1, "a": [doc! { "c": 2 }, doc! { "c": 3 }, Bson::Int32(7)] }
        );
    }

    #[test]
    fn shared_prefix_inclusion_merges_into_one_subdoc() {
        let input = doc! { "_id": 1, "a": { "b": 1, "c": 2, "d": 3 } };
        let out = apply_projection_to_doc(input, &doc! { "a.b": 1, "a.c": 1 });
        assert_eq!(out, doc! { "_id": 1, "a": { "b": 1, "c": 2 } });
    }

    #[test]
    fn three_level_depth_inclusion() {
        let input = doc! {
            "_id": 1,
            "a": { "b": { "c": 5, "x": 6 }, "y": 7 },
        };
        let out = apply_projection_to_doc(input, &doc! { "a.b.c": 1 });
        assert_eq!(out, doc! { "_id": 1, "a": { "b": { "c": 5 } } });
    }

    #[test]
    fn id_excluded_in_nested_inclusion() {
        let input = doc! { "_id": 1, "a": { "b": 2, "c": 3 } };
        let out = apply_projection_to_doc(input, &doc! { "a.b": 1, "_id": 0 });
        assert_eq!(out, doc! { "a": { "b": 2 } });
    }

    #[test]
    fn inclusion_preserves_document_field_order() {
        // Projection names `c` before `a`, but output must follow the
        // document's own field order (spec point 6).
        let input = doc! { "_id": 1, "a": 1, "b": 2, "c": 3 };
        let out = apply_projection_to_doc(input, &doc! { "c": 1, "a": 1 });
        let keys: Vec<&str> = out.keys().map(String::as_str).collect();
        assert_eq!(keys, vec!["_id", "a", "c"]);
    }

    #[test]
    fn collision_parent_then_child_is_last_spec_wins() {
        // MongoDB errors on `{"a": 1, "a.b": 1}`; mqlite resolves it
        // last-spec-wins, so the deeper `a.b` spec governs.
        let input = doc! { "_id": 1, "a": { "b": 2, "c": 3 } };
        let out = apply_projection_to_doc(input, &doc! { "a": 1, "a.b": 1 });
        assert_eq!(out, doc! { "_id": 1, "a": { "b": 2 } });
    }

    #[test]
    fn collision_child_then_parent_is_last_spec_wins() {
        // Reverse order: the later whole-`a` spec wins, keeping all of `a`.
        let input = doc! { "_id": 1, "a": { "b": 2, "c": 3 } };
        let out = apply_projection_to_doc(input, &doc! { "a.b": 1, "a": 1 });
        assert_eq!(out, doc! { "_id": 1, "a": { "b": 2, "c": 3 } });
    }
}
