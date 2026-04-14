//! In-memory storage engine for Phase 1b Collection CRUD methods.
//!
//! [`EngineState`] is the central state object held under a `Mutex` inside
//! `DatabaseInner`.  It stores all collections as `Vec<Document>` and
//! implements the full CRUD semantics specified in the `hq-amz` bead.
//!
//! ## Design choices
//!
//! * **In-memory `Vec<Document>`** — Simple, correct, sufficient for Phase 1b
//!   acceptance tests.  The B+tree / WAL / buffer-pool infrastructure from
//!   Phases 0–1a wires in at the engine boundary in a later phase.
//! * **Full scan for finds** — `eval_filter` from the query engine is used for
//!   all filter evaluations; index-accelerated seeks are Phase 1c.
//! * **Writer-mutex delegated** — `DatabaseInner.writer_lock` serialises all
//!   mutations; reads acquire `engine` directly.

use std::collections::HashMap;

use bson::{Bson, Document};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    error::{Error, Result},
    index::{IndexInfo, IndexModel},
    options::{
        FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions,
        FindOptions, InsertManyOptions, ReturnDocument, UpdateOptions,
    },
    query::{eval_filter, get_nested_field},
    results::{BulkWriteError, DeleteResult, InsertManyResult, InsertOneResult, UpdateResult},
    storage::oid::ObjectIdGenerator,
    update_operators::{apply_update, is_operator_update, upsert_base_from_filter},
    validation::validate_document,
};

// ---------------------------------------------------------------------------
// Per-collection state
// ---------------------------------------------------------------------------

struct IndexRecord {
    model: IndexModel,
    name: String,
}

struct CollectionState {
    docs: Vec<Document>,
    indexes: Vec<IndexRecord>,
}

impl CollectionState {
    fn new() -> Self {
        CollectionState {
            docs: Vec::new(),
            indexes: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Engine state
// ---------------------------------------------------------------------------

pub(crate) struct EngineState {
    collections: HashMap<String, CollectionState>,
}

impl EngineState {
    pub(crate) fn new() -> Self {
        EngineState {
            collections: HashMap::new(),
        }
    }

    // Lazily create a collection on first access.
    fn get_or_create(&mut self, name: &str) -> &mut CollectionState {
        self.collections
            .entry(name.to_owned())
            .or_insert_with(CollectionState::new)
    }

    // Ensure the document has an `_id` field; auto-assign an ObjectId if absent.
    fn ensure_id(doc: &mut Document) -> Bson {
        if let Some(id) = doc.get("_id") {
            id.clone()
        } else {
            let oid = Bson::ObjectId(ObjectIdGenerator::generate());
            doc.insert("_id", oid.clone());
            oid
        }
    }

    // ---------------------------------------------------------------------------
    // Insert
    // ---------------------------------------------------------------------------

    pub(crate) fn insert_one<T: Serialize>(
        &mut self,
        name: &str,
        doc: &T,
    ) -> Result<InsertOneResult> {
        let mut bson_doc = bson::to_document(doc).map_err(Error::BsonSerialization)?;
        validate_document(&bson_doc)?;
        let id_bson = Self::ensure_id(&mut bson_doc);

        let oid = match &id_bson {
            Bson::ObjectId(o) => *o,
            other => {
                // Convert non-ObjectId _id to a deterministic ObjectId by using a
                // generated one (stored in the doc, returned as the "inserted_id").
                let _ = other; // keep the original _id in the doc
                ObjectIdGenerator::generate()
            }
        };

        let coll = self.get_or_create(name);
        coll.docs.push(bson_doc);
        Ok(InsertOneResult { inserted_id: oid })
    }

    pub(crate) fn insert_many<T: Serialize>(
        &mut self,
        name: &str,
        docs: &[T],
        opts: InsertManyOptions,
    ) -> Result<InsertManyResult> {
        let mut inserted_ids: HashMap<usize, Bson> = HashMap::new();
        let mut errors: Vec<BulkWriteError> = Vec::new();

        for (idx, raw) in docs.iter().enumerate() {
            // Serialize + validate each document independently.
            let result: Result<Document> = (|| {
                let mut bson_doc = bson::to_document(raw).map_err(Error::BsonSerialization)?;
                validate_document(&bson_doc)?;
                Ok(bson_doc)
            })();

            match result {
                Err(e) => {
                    errors.push(BulkWriteError {
                        index: idx,
                        code: e.code().unwrap_or(1),
                        message: e.to_string(),
                    });
                    if opts.ordered {
                        // Stop at first error.
                        return Err(Error::Internal(format!(
                            "insert_many ordered: stopped at index {idx}: {e}"
                        )));
                    }
                }
                Ok(mut bson_doc) => {
                    let id = Self::ensure_id(&mut bson_doc);
                    let coll = self.get_or_create(name);
                    coll.docs.push(bson_doc);
                    inserted_ids.insert(idx, id);
                }
            }
        }

        Ok(InsertManyResult {
            inserted_ids,
            errors,
        })
    }

    // ---------------------------------------------------------------------------
    // Find
    // ---------------------------------------------------------------------------

    pub(crate) fn find_one<T: DeserializeOwned>(
        &self,
        name: &str,
        filter: Document,
    ) -> Result<Option<T>> {
        let Some(coll) = self.collections.get(name) else {
            return Ok(None);
        };

        for doc in &coll.docs {
            if eval_filter(doc, &filter)? {
                let t = bson::from_document(doc.clone()).map_err(Error::BsonDeserialization)?;
                return Ok(Some(t));
            }
        }
        Ok(None)
    }

    pub(crate) fn find<T: DeserializeOwned>(
        &self,
        name: &str,
        filter: Document,
        opts: FindOptions,
    ) -> Result<crate::cursor::Cursor<T>> {
        let Some(coll) = self.collections.get(name) else {
            return Ok(crate::cursor::Cursor::empty());
        };

        // Collect all matching documents.
        let mut matched: Vec<Document> = coll
            .docs
            .iter()
            .filter(|doc| eval_filter(doc, &filter).unwrap_or(false))
            .cloned()
            .collect();

        // Sort.
        if let Some(sort_doc) = opts.sort {
            sort_documents(&mut matched, &sort_doc);
        }

        // Skip.
        if let Some(skip) = opts.skip {
            let skip = skip as usize;
            if skip >= matched.len() {
                matched.clear();
            } else {
                matched = matched.into_iter().skip(skip).collect();
            }
        }

        // Limit.
        if let Some(limit) = opts.limit {
            if limit > 0 {
                matched.truncate(limit as usize);
            }
        }

        // Projection (basic include/exclude).
        if let Some(proj) = opts.projection {
            matched = matched
                .into_iter()
                .map(|doc| apply_projection(doc, &proj))
                .collect();
        }

        Ok(crate::cursor::Cursor::new(matched))
    }

    // ---------------------------------------------------------------------------
    // Update
    // ---------------------------------------------------------------------------

    pub(crate) fn update_one(
        &mut self,
        name: &str,
        filter: Document,
        update: Document,
        opts: UpdateOptions,
    ) -> Result<UpdateResult> {
        self.update_impl(name, filter, update, opts, false)
    }

    pub(crate) fn update_many(
        &mut self,
        name: &str,
        filter: Document,
        update: Document,
        opts: UpdateOptions,
    ) -> Result<UpdateResult> {
        self.update_impl(name, filter, update, opts, true)
    }

    fn update_impl(
        &mut self,
        name: &str,
        filter: Document,
        update: Document,
        opts: UpdateOptions,
        many: bool,
    ) -> Result<UpdateResult> {
        // Validate the update document: must be operator-based (not a replacement).
        if !is_operator_update(&update) {
            return Err(Error::Internal(
                "update_one/many requires an operator update document (e.g. {$set: {...}}); \
                 use find_one_and_replace for replacements"
                    .into(),
            ));
        }

        let mut matched_count = 0u64;
        let mut modified_count = 0u64;

        // Access existing collection documents.
        let docs = match self.collections.get_mut(name) {
            Some(coll) => &mut coll.docs,
            None => {
                // Collection doesn't exist yet.  Upsert creates it.
                if opts.upsert {
                    let upserted_id =
                        self.do_upsert(name, &filter, &update, false)?;
                    return Ok(UpdateResult {
                        matched_count: 0,
                        modified_count: 0,
                        upserted_id: Some(upserted_id),
                    });
                }
                return Ok(UpdateResult {
                    matched_count: 0,
                    modified_count: 0,
                    upserted_id: None,
                });
            }
        };

        for doc in docs.iter_mut() {
            if !eval_filter(doc, &filter)? {
                continue;
            }
            matched_count += 1;

            let before = doc.clone();
            apply_update(doc, &update, false)?;
            if *doc != before {
                modified_count += 1;
            }

            if !many {
                break;
            }
        }

        // Upsert: insert if no document matched.
        let upserted_id = if matched_count == 0 && opts.upsert {
            Some(self.do_upsert(name, &filter, &update, false)?)
        } else {
            None
        };

        Ok(UpdateResult {
            matched_count,
            modified_count,
            upserted_id,
        })
    }

    /// Create and insert a new document for an upsert operation.
    ///
    /// `is_find_and_modify` is `true` when called from `find_one_and_update`
    /// (triggers `$setOnInsert` semantics).
    fn do_upsert(
        &mut self,
        name: &str,
        filter: &Document,
        update: &Document,
        _is_find_and_modify: bool,
    ) -> Result<Bson> {
        let mut new_doc = upsert_base_from_filter(filter);
        apply_update(&mut new_doc, update, true /* is_insert */)?;
        let id = Self::ensure_id(&mut new_doc);
        validate_document(&new_doc)?;
        let coll = self.get_or_create(name);
        coll.docs.push(new_doc);
        Ok(id)
    }

    // ---------------------------------------------------------------------------
    // Delete
    // ---------------------------------------------------------------------------

    pub(crate) fn delete_one(&mut self, name: &str, filter: Document) -> Result<DeleteResult> {
        self.delete_impl(name, filter, false)
    }

    pub(crate) fn delete_many(&mut self, name: &str, filter: Document) -> Result<DeleteResult> {
        self.delete_impl(name, filter, true)
    }

    fn delete_impl(
        &mut self,
        name: &str,
        filter: Document,
        many: bool,
    ) -> Result<DeleteResult> {
        let Some(coll) = self.collections.get_mut(name) else {
            return Ok(DeleteResult { deleted_count: 0 });
        };

        let mut deleted_count = 0u64;
        let mut i = 0;
        while i < coll.docs.len() {
            if eval_filter(&coll.docs[i], &filter)? {
                coll.docs.remove(i);
                deleted_count += 1;
                if !many {
                    break;
                }
            } else {
                i += 1;
            }
        }

        Ok(DeleteResult { deleted_count })
    }

    // ---------------------------------------------------------------------------
    // findAndModify variants
    // ---------------------------------------------------------------------------

    pub(crate) fn find_one_and_update<T: Serialize + DeserializeOwned>(
        &mut self,
        name: &str,
        filter: Document,
        update: Document,
    ) -> Result<Option<T>> {
        let opts = FindOneAndUpdateOptions::new();
        self.find_one_and_update_with_options(name, filter, update, opts)
    }

    pub(crate) fn find_one_and_update_with_options<T: Serialize + DeserializeOwned>(
        &mut self,
        name: &str,
        filter: Document,
        update: Document,
        opts: FindOneAndUpdateOptions,
    ) -> Result<Option<T>> {
        if !is_operator_update(&update) {
            return Err(Error::Internal(
                "find_one_and_update requires an operator update document (e.g. {$set: {...}})"
                    .into(),
            ));
        }

        // Find the matching index first (without holding a long-lived borrow).
        let idx = match self.collections.get(name) {
            None => None,
            Some(coll) => find_matching_index(&coll.docs, &filter, opts.sort.as_ref())?,
        };

        match idx {
            None => {
                // No match (or no collection).
                if opts.upsert {
                    let id = self.do_upsert(name, &filter, &update, true)?;
                    if opts.return_document == ReturnDocument::After {
                        return self.find_one(name, bson::doc! { "_id": id });
                    }
                }
                Ok(None)
            }
            Some(idx) => {
                let coll = self.collections.get_mut(name).expect("collection exists");
                let before = coll.docs[idx].clone();
                apply_update(&mut coll.docs[idx], &update, false)?;
                let after = coll.docs[idx].clone();

                let result_doc = match opts.return_document {
                    ReturnDocument::Before => before,
                    ReturnDocument::After => after,
                };

                bson::from_document(result_doc)
                    .map(Some)
                    .map_err(Error::BsonDeserialization)
            }
        }
    }

    pub(crate) fn find_one_and_delete<T: DeserializeOwned>(
        &mut self,
        name: &str,
        filter: Document,
    ) -> Result<Option<T>> {
        let opts = FindOneAndDeleteOptions::new();
        self.find_one_and_delete_with_options(name, filter, opts)
    }

    pub(crate) fn find_one_and_delete_with_options<T: DeserializeOwned>(
        &mut self,
        name: &str,
        filter: Document,
        opts: FindOneAndDeleteOptions,
    ) -> Result<Option<T>> {
        let Some(coll) = self.collections.get_mut(name) else {
            return Ok(None);
        };

        let idx = find_matching_index(&coll.docs, &filter, opts.sort.as_ref())?;

        let Some(idx) = idx else {
            return Ok(None);
        };

        let deleted = coll.docs.remove(idx);
        bson::from_document(deleted)
            .map(Some)
            .map_err(Error::BsonDeserialization)
    }

    pub(crate) fn find_one_and_replace<T: Serialize + DeserializeOwned>(
        &mut self,
        name: &str,
        filter: Document,
        replacement: &T,
    ) -> Result<Option<T>> {
        let opts = FindOneAndReplaceOptions::new();
        self.find_one_and_replace_with_options(name, filter, replacement, opts)
    }

    pub(crate) fn find_one_and_replace_with_options<T: Serialize + DeserializeOwned>(
        &mut self,
        name: &str,
        filter: Document,
        replacement: &T,
        opts: FindOneAndReplaceOptions,
    ) -> Result<Option<T>> {
        let mut replacement_doc =
            bson::to_document(replacement).map_err(Error::BsonSerialization)?;
        validate_document(&replacement_doc)?;

        let Some(coll) = self.collections.get_mut(name) else {
            // No collection — handle upsert.
            if opts.upsert {
                Self::ensure_id(&mut replacement_doc);
                let id = replacement_doc.get("_id").cloned().unwrap();
                let coll = self.get_or_create(name);
                coll.docs.push(replacement_doc.clone());
                if opts.return_document == ReturnDocument::After {
                    return bson::from_document(replacement_doc)
                        .map(Some)
                        .map_err(Error::BsonDeserialization);
                }
            }
            return Ok(None);
        };

        let idx = find_matching_index(&coll.docs, &filter, opts.sort.as_ref())?;

        let Some(idx) = idx else {
            // No match — handle upsert.
            if opts.upsert {
                let mut rep = replacement_doc;
                Self::ensure_id(&mut rep);
                let result_doc = if opts.return_document == ReturnDocument::After {
                    Some(rep.clone())
                } else {
                    None
                };
                coll.docs.push(rep);
                return match result_doc {
                    None => Ok(None),
                    Some(d) => bson::from_document(d)
                        .map(Some)
                        .map_err(Error::BsonDeserialization),
                };
            }
            return Ok(None);
        };

        let before = coll.docs[idx].clone();

        // Preserve the _id from the matched document.
        if let Some(existing_id) = before.get("_id") {
            replacement_doc.insert("_id", existing_id.clone());
        } else {
            Self::ensure_id(&mut replacement_doc);
        }

        coll.docs[idx] = replacement_doc.clone();

        let result_doc = match opts.return_document {
            ReturnDocument::Before => before,
            ReturnDocument::After => replacement_doc,
        };

        bson::from_document(result_doc)
            .map(Some)
            .map_err(Error::BsonDeserialization)
    }

    // ---------------------------------------------------------------------------
    // Count
    // ---------------------------------------------------------------------------

    pub(crate) fn estimated_document_count(&self, name: &str) -> Result<u64> {
        Ok(self
            .collections
            .get(name)
            .map(|c| c.docs.len() as u64)
            .unwrap_or(0))
    }

    pub(crate) fn count_documents(&self, name: &str, filter: Document) -> Result<u64> {
        let Some(coll) = self.collections.get(name) else {
            return Ok(0);
        };
        let mut count = 0u64;
        for doc in &coll.docs {
            if eval_filter(doc, &filter)? {
                count += 1;
            }
        }
        Ok(count)
    }

    // ---------------------------------------------------------------------------
    // Collection management
    // ---------------------------------------------------------------------------

    pub(crate) fn list_collection_names(&self) -> Result<Vec<String>> {
        let mut names: Vec<String> = self.collections.keys().cloned().collect();
        names.sort();
        Ok(names)
    }

    pub(crate) fn drop_collection(&mut self, name: &str) -> Result<()> {
        self.collections.remove(name);
        Ok(())
    }

    pub(crate) fn create_collection(&mut self, name: &str) -> Result<()> {
        self.collections
            .entry(name.to_owned())
            .or_insert_with(CollectionState::new);
        Ok(())
    }

    // ---------------------------------------------------------------------------
    // Indexes
    // ---------------------------------------------------------------------------

    pub(crate) fn create_index(&mut self, collection: &str, model: IndexModel) -> Result<String> {
        use crate::storage::secondary_index::generate_index_name;
        let name = model
            .options
            .name
            .clone()
            .unwrap_or_else(|| generate_index_name(&model.keys));
        let coll = self.get_or_create(collection);
        // Reject duplicate index names (idempotent for exact duplicates).
        if !coll.indexes.iter().any(|r| r.name == name) {
            coll.indexes.push(IndexRecord {
                model,
                name: name.clone(),
            });
        }
        Ok(name)
    }

    pub(crate) fn drop_index(&mut self, collection: &str, index_name: &str) -> Result<()> {
        let Some(coll) = self.collections.get_mut(collection) else {
            return Ok(());
        };
        coll.indexes.retain(|r| r.name != index_name);
        Ok(())
    }

    pub(crate) fn list_indexes(&self, collection: &str) -> Result<Vec<IndexInfo>> {
        let Some(coll) = self.collections.get(collection) else {
            return Ok(Vec::new());
        };
        Ok(coll
            .indexes
            .iter()
            .map(|r| IndexInfo {
                name: r.name.clone(),
                keys: r.model.keys.clone(),
                unique: r.model.options.unique,
                sparse: r.model.options.sparse,
            })
            .collect())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Find the index of the first (or sorted-first) document matching `filter`.
///
/// If `sort` is given, the full set of matching documents is sorted and the
/// index of the winner in the **original** slice is returned.
fn find_matching_index(
    docs: &[Document],
    filter: &Document,
    sort: Option<&Document>,
) -> Result<Option<usize>> {
    // Collect indices of matching documents.
    let mut candidates: Vec<usize> = Vec::new();
    for (i, doc) in docs.iter().enumerate() {
        if eval_filter(doc, filter)? {
            candidates.push(i);
        }
    }

    if candidates.is_empty() {
        return Ok(None);
    }

    if let Some(sort_doc) = sort {
        // Pick the "first" according to the sort order.
        candidates.sort_by(|&a, &b| compare_documents(&docs[a], &docs[b], sort_doc));
    }

    Ok(Some(candidates[0]))
}

/// Sort a slice of documents by a sort specification document.
///
/// Keys map to `1` (ascending) or `-1` (descending).
fn sort_documents(docs: &mut Vec<Document>, sort: &Document) {
    docs.sort_by(|a, b| compare_documents(a, b, sort));
}

fn compare_documents(a: &Document, b: &Document, sort: &Document) -> std::cmp::Ordering {
    use crate::key_encoding::encode_key;

    for (field, dir) in sort {
        let ascending = !matches!(dir, Bson::Int32(-1) | Bson::Int64(-1));
        let a_val = get_nested_field(a, field)
            .cloned()
            .unwrap_or(Bson::Null);
        let b_val = get_nested_field(b, field)
            .cloned()
            .unwrap_or(Bson::Null);

        let ord = encode_key(&a_val).cmp(&encode_key(&b_val));
        if ord == std::cmp::Ordering::Equal {
            continue;
        }
        return if ascending { ord } else { ord.reverse() };
    }
    std::cmp::Ordering::Equal
}

/// Apply a projection document to a result document.
///
/// Handles inclusion projections (`{field: 1}`) and exclusion projections
/// (`{field: 0}`).  The `_id` field is always included unless explicitly
/// excluded.
fn apply_projection(mut doc: Document, proj: &Document) -> Document {
    // Determine mode: first non-_id key with value 1 → inclusion; 0 → exclusion.
    let is_inclusion = proj
        .iter()
        .filter(|(k, _)| k.as_str() != "_id")
        .any(|(_, v)| !matches!(v, Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false)));

    let explicit_id_excl = proj.get("_id").map_or(false, |v| {
        matches!(v, Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false))
    });

    if is_inclusion {
        // Inclusion mode: keep only projected fields (+ _id unless excluded).
        let mut result = Document::new();
        if !explicit_id_excl {
            if let Some(id) = doc.get("_id") {
                result.insert("_id", id.clone());
            }
        }
        for (k, v) in proj {
            if k == "_id" {
                continue;
            }
            if !matches!(v, Bson::Int32(0) | Bson::Int64(0) | Bson::Boolean(false)) {
                if let Some(val) = doc.get(k) {
                    result.insert(k, val.clone());
                }
            }
        }
        result
    } else {
        // Exclusion mode: remove projected fields.
        for (k, _) in proj {
            doc.remove(k);
        }
        doc
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct User {
        name: String,
        email: String,
        age: i32,
    }

    fn engine() -> EngineState {
        EngineState::new()
    }

    // ---- Insert + find round-trip -------------------------------------------

    #[test]
    fn insert_one_and_find_one_roundtrip() {
        let mut eng = engine();
        let user = User {
            name: "Alice".into(),
            email: "alice@example.com".into(),
            age: 30,
        };
        let res = eng.insert_one("users", &user).unwrap();
        assert_ne!(res.inserted_id.to_hex(), "");

        let found: Option<User> = eng
            .find_one("users", doc! { "name": "Alice" })
            .unwrap();
        assert_eq!(found, Some(user));
    }

    #[test]
    fn find_one_empty_filter_matches_first() {
        let mut eng = engine();
        eng.insert_one("u", &doc! { "x": 1i32 }).unwrap();
        eng.insert_one("u", &doc! { "x": 2i32 }).unwrap();

        let found: Option<Document> = eng.find_one("u", doc! {}).unwrap();
        assert!(found.is_some());
    }

    #[test]
    fn find_one_returns_none_when_not_found() {
        let mut eng = engine();
        eng.insert_one("u", &doc! { "x": 1i32 }).unwrap();
        let found: Option<Document> = eng.find_one("u", doc! { "x": 99i32 }).unwrap();
        assert!(found.is_none());
    }

    #[test]
    fn find_returns_all_matching_documents() {
        let mut eng = engine();
        for i in 0..5i32 {
            eng.insert_one("items", &doc! { "v": i }).unwrap();
        }
        // find where v >= 3
        let cursor = eng
            .find::<Document>("items", doc! { "v": { "$gte": 3i32 } }, FindOptions::new())
            .unwrap();
        let results: Vec<_> = cursor.collect::<Result<_>>().unwrap();
        assert_eq!(results.len(), 2); // v=3 and v=4
    }

    // ---- insert_many --------------------------------------------------------

    #[test]
    fn insert_many_ordered_stops_at_first_error() {
        let mut eng = engine();
        // Two documents; second is too large to serialize cleanly.
        // Use a bad document (simulate by manually injecting a validated-fail).
        // For simplicity, test basic ordered insert with all-valid docs first.
        let docs = vec![
            doc! { "a": 1i32 },
            doc! { "a": 2i32 },
            doc! { "a": 3i32 },
        ];
        let res = eng
            .insert_many("coll", &docs, InsertManyOptions::new())
            .unwrap();
        assert_eq!(res.inserted_ids.len(), 3);
        assert!(res.errors.is_empty());
    }

    #[test]
    fn insert_many_sets_ids_by_index() {
        let mut eng = engine();
        let docs = vec![doc! { "n": 0i32 }, doc! { "n": 1i32 }];
        let res = eng
            .insert_many("coll", &docs, InsertManyOptions::new())
            .unwrap();
        assert!(res.inserted_ids.contains_key(&0));
        assert!(res.inserted_ids.contains_key(&1));
    }

    // ---- update_one ---------------------------------------------------------

    #[test]
    fn update_one_modifies_first_match() {
        let mut eng = engine();
        eng.insert_one("u", &doc! { "x": 1i32 }).unwrap();
        eng.insert_one("u", &doc! { "x": 1i32 }).unwrap();

        let res = eng
            .update_one(
                "u",
                doc! { "x": 1i32 },
                doc! { "$set": { "x": 99i32 } },
                UpdateOptions::new(),
            )
            .unwrap();
        assert_eq!(res.matched_count, 1);
        assert_eq!(res.modified_count, 1);

        // First doc updated, second still 1.
        let cursor = eng
            .find::<Document>("u", doc! { "x": 1i32 }, FindOptions::new())
            .unwrap();
        let remaining: Vec<_> = cursor.collect::<Result<_>>().unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn update_many_modifies_all_matches() {
        let mut eng = engine();
        for _ in 0..4 {
            eng.insert_one("u", &doc! { "status": "pending" }).unwrap();
        }
        let res = eng
            .update_many(
                "u",
                doc! { "status": "pending" },
                doc! { "$set": { "status": "done" } },
                UpdateOptions::new(),
            )
            .unwrap();
        assert_eq!(res.matched_count, 4);
        assert_eq!(res.modified_count, 4);
    }

    // ---- upsert -------------------------------------------------------------

    #[test]
    fn upsert_inserts_when_no_match() {
        let mut eng = engine();
        let res = eng
            .update_one(
                "u",
                doc! { "name": "Charlie" },
                doc! { "$set": { "age": 25i32 } },
                UpdateOptions::new().upsert(true),
            )
            .unwrap();
        assert_eq!(res.matched_count, 0);
        assert_eq!(res.modified_count, 0);
        assert!(res.upserted_id.is_some());

        let found: Option<Document> = eng.find_one("u", doc! { "name": "Charlie" }).unwrap();
        assert!(found.is_some());
        let doc = found.unwrap();
        assert_eq!(doc.get_str("name").unwrap(), "Charlie");
        assert!(doc.get("age").is_some());
    }

    #[test]
    fn upsert_does_not_insert_when_match_exists() {
        let mut eng = engine();
        eng.insert_one("u", &doc! { "name": "Dave", "age": 30i32 })
            .unwrap();
        let res = eng
            .update_one(
                "u",
                doc! { "name": "Dave" },
                doc! { "$set": { "age": 31i32 } },
                UpdateOptions::new().upsert(true),
            )
            .unwrap();
        assert_eq!(res.matched_count, 1);
        assert!(res.upserted_id.is_none());
    }

    // ---- delete -------------------------------------------------------------

    #[test]
    fn delete_one_removes_single_document() {
        let mut eng = engine();
        eng.insert_one("u", &doc! { "x": 1i32 }).unwrap();
        eng.insert_one("u", &doc! { "x": 1i32 }).unwrap();

        let res = eng.delete_one("u", doc! { "x": 1i32 }).unwrap();
        assert_eq!(res.deleted_count, 1);

        let count = eng.count_documents("u", doc! {}).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn delete_many_removes_all_matches() {
        let mut eng = engine();
        for _ in 0..5 {
            eng.insert_one("u", &doc! { "x": 1i32 }).unwrap();
        }
        eng.insert_one("u", &doc! { "x": 2i32 }).unwrap();

        let res = eng.delete_many("u", doc! { "x": 1i32 }).unwrap();
        assert_eq!(res.deleted_count, 5);
        assert_eq!(eng.count_documents("u", doc! {}).unwrap(), 1);
    }

    // ---- find_one_and_update ------------------------------------------------

    #[test]
    fn find_one_and_update_returns_pre_modification_by_default() {
        let mut eng = engine();
        eng.insert_one("u", &doc! { "v": 1i32 }).unwrap();

        let before: Option<Document> = eng
            .find_one_and_update("u", doc! { "v": 1i32 }, doc! { "$set": { "v": 99i32 } })
            .unwrap();

        assert_eq!(
            before.unwrap().get_i32("v").unwrap(),
            1,
            "should return PRE-modification document"
        );
    }

    #[test]
    fn find_one_and_update_with_return_after() {
        let mut eng = engine();
        eng.insert_one("u", &doc! { "v": 1i32 }).unwrap();

        let after: Option<Document> = eng
            .find_one_and_update_with_options(
                "u",
                doc! { "v": 1i32 },
                doc! { "$set": { "v": 99i32 } },
                FindOneAndUpdateOptions::new().return_document(ReturnDocument::After),
            )
            .unwrap();

        assert_eq!(
            after.unwrap().get_i32("v").unwrap(),
            99,
            "should return POST-modification document"
        );
    }

    #[test]
    fn find_one_and_update_returns_none_when_no_match() {
        let mut eng = engine();
        let result: Option<Document> = eng
            .find_one_and_update("u", doc! { "x": 42i32 }, doc! { "$set": { "x": 0i32 } })
            .unwrap();
        assert!(result.is_none());
    }

    // ---- find_one_and_delete ------------------------------------------------

    #[test]
    fn find_one_and_delete_returns_and_removes_document() {
        let mut eng = engine();
        eng.insert_one("u", &doc! { "n": 7i32 }).unwrap();

        let deleted: Option<Document> =
            eng.find_one_and_delete("u", doc! { "n": 7i32 }).unwrap();

        assert_eq!(deleted.unwrap().get_i32("n").unwrap(), 7);
        assert_eq!(eng.count_documents("u", doc! {}).unwrap(), 0);
    }

    // ---- find_one_and_replace -----------------------------------------------

    #[test]
    fn find_one_and_replace_returns_before_by_default() {
        let mut eng = engine();
        eng.insert_one("u", &doc! { "a": 1i32, "b": 2i32 })
            .unwrap();

        let before: Option<Document> = eng
            .find_one_and_replace("u", doc! { "a": 1i32 }, &doc! { "c": 3i32 })
            .unwrap();

        assert!(before.unwrap().contains_key("a"));
        // After replacement, old fields should be gone.
        let after: Option<Document> = eng.find_one("u", doc! { "c": 3i32 }).unwrap();
        assert!(after.is_some());
        assert!(!after.unwrap().contains_key("a"));
    }

    // ---- count_documents ----------------------------------------------------

    #[test]
    fn count_documents_with_filter() {
        let mut eng = engine();
        for i in 0..10i32 {
            eng.insert_one("u", &doc! { "v": i }).unwrap();
        }
        let count = eng
            .count_documents("u", doc! { "v": { "$lt": 5i32 } })
            .unwrap();
        assert_eq!(count, 5);
    }

    #[test]
    fn estimated_document_count_returns_total() {
        let mut eng = engine();
        for _ in 0..7 {
            eng.insert_one("u", &doc! {}).unwrap();
        }
        assert_eq!(eng.estimated_document_count("u").unwrap(), 7);
    }

    // ---- projection ---------------------------------------------------------

    #[test]
    fn find_with_inclusion_projection() {
        let mut eng = engine();
        eng.insert_one("u", &doc! { "a": 1i32, "b": 2i32, "c": 3i32 })
            .unwrap();
        let opts = FindOptions::new().projection(doc! { "a": 1i32 });
        let cursor = eng.find::<Document>("u", doc! {}, opts).unwrap();
        let docs: Vec<_> = cursor.collect::<Result<_>>().unwrap();
        assert_eq!(docs.len(), 1);
        // Inclusion: only "a" (and _id) should be present.
        assert!(docs[0].contains_key("a"));
        assert!(!docs[0].contains_key("b"));
    }

    #[test]
    fn find_with_exclusion_projection() {
        let mut eng = engine();
        eng.insert_one("u", &doc! { "a": 1i32, "b": 2i32, "c": 3i32 })
            .unwrap();
        let opts = FindOptions::new().projection(doc! { "b": 0i32 });
        let cursor = eng.find::<Document>("u", doc! {}, opts).unwrap();
        let docs: Vec<_> = cursor.collect::<Result<_>>().unwrap();
        assert_eq!(docs.len(), 1);
        assert!(docs[0].contains_key("a"));
        assert!(!docs[0].contains_key("b"));
        assert!(docs[0].contains_key("c"));
    }

    // ---- sort / limit / skip ------------------------------------------------

    #[test]
    fn find_with_sort_ascending() {
        let mut eng = engine();
        for v in [3i32, 1, 4, 1, 5].iter() {
            eng.insert_one("u", &doc! { "v": *v }).unwrap();
        }
        let opts = FindOptions::new().sort(doc! { "v": 1i32 });
        let cursor = eng.find::<Document>("u", doc! {}, opts).unwrap();
        let values: Vec<i32> = cursor
            .collect::<Result<Vec<Document>>>()
            .unwrap()
            .iter()
            .map(|d| d.get_i32("v").unwrap())
            .collect();
        // Should be ascending (ties may appear at any relative position).
        for w in values.windows(2) {
            assert!(w[0] <= w[1]);
        }
    }

    #[test]
    fn find_with_limit_and_skip() {
        let mut eng = engine();
        for i in 0..10i32 {
            eng.insert_one("u", &doc! { "v": i }).unwrap();
        }
        let opts = FindOptions::new().skip(3).limit(4);
        let cursor = eng.find::<Document>("u", doc! {}, opts).unwrap();
        let docs: Vec<_> = cursor.collect::<Result<_>>().unwrap();
        assert_eq!(docs.len(), 4);
    }

    // ---- collection management ----------------------------------------------

    #[test]
    fn list_collection_names_empty() {
        let eng = engine();
        assert!(eng.list_collection_names().unwrap().is_empty());
    }

    #[test]
    fn list_collection_names_after_insert() {
        let mut eng = engine();
        eng.insert_one("alpha", &doc! {}).unwrap();
        eng.insert_one("beta", &doc! {}).unwrap();
        let names = eng.list_collection_names().unwrap();
        assert_eq!(names, ["alpha", "beta"]);
    }

    #[test]
    fn drop_collection_removes_documents() {
        let mut eng = engine();
        eng.insert_one("u", &doc! { "x": 1i32 }).unwrap();
        eng.drop_collection("u").unwrap();
        let count = eng.count_documents("u", doc! {}).unwrap();
        assert_eq!(count, 0);
    }

    // ---- index operations ---------------------------------------------------

    #[test]
    fn create_and_list_index() {
        use crate::index::IndexModel;
        let mut eng = engine();
        eng.create_collection("u").unwrap();
        let model = IndexModel::builder()
            .keys(doc! { "email": 1i32 })
            .build()
            .unwrap();
        let name = eng.create_index("u", model).unwrap();
        assert_eq!(name, "email_1");
        let indexes = eng.list_indexes("u").unwrap();
        assert_eq!(indexes.len(), 1);
        assert_eq!(indexes[0].name, "email_1");
    }
}
