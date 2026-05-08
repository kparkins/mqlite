//! `ClientInner` CRUD methods + checkpoint/backup.
//!
//! Extracted from [`super`] to keep `client.rs` under the length budget. All
//! storage operations are routed through `self.engine` (a `Box<dyn StorageEngine>`).

use std::path::Path;

use bson::{Bson, Document};
use serde::{de::DeserializeOwned, Serialize};

use crate::storage::lock::FileLock;

use crate::{
    cursor::Cursor,
    error::{Error, Result},
    index::{IndexInfo, IndexModel},
    options::{
        FindOneAndDeleteOptions, FindOneAndReplaceOptions, FindOneAndUpdateOptions, FindOptions,
        InsertManyOptions, UpdateOptions,
    },
    results::{DeleteResult, InsertManyResult, InsertOneResult, UpdateResult},
};

use super::{path::reject_symlink, ClientInner};

impl ClientInner {
    pub(crate) fn insert_one<T: serde::Serialize>(
        &self,
        name: &str,
        doc: &T,
    ) -> Result<InsertOneResult> {
        #[cfg(feature = "tracing")]
        tracing::debug!(target: "mqlite", collection = name, doc_count = 1u64, "mqlite::insert");

        let bson_doc = bson::to_document(doc).map_err(Error::BsonSerialization)?;
        // The storage engine owns write admission and publication ordering.
        let id = self.engine.insert(name, bson_doc)?;
        let oid = match id {
            Bson::ObjectId(o) => o,
            // For non-ObjectId _id values, generate a surrogate ObjectId to
            // satisfy the `InsertOneResult` type.  The document retains its
            // original `_id`.  This is a pre-existing limitation.
            _ => crate::storage::oid::ObjectIdGenerator::generate(),
        };
        Ok(InsertOneResult { inserted_id: oid })
    }

    pub(crate) fn insert_many<T: serde::Serialize>(
        &self,
        name: &str,
        docs: &[T],
        opts: InsertManyOptions,
    ) -> Result<InsertManyResult> {
        use crate::results::BulkWriteError;
        use std::collections::HashMap;

        #[cfg(feature = "tracing")]
        tracing::debug!(
            target: "mqlite",
            collection = name,
            doc_count = docs.len() as u64,
            "mqlite::insert"
        );
        let mut inserted_ids: HashMap<usize, Bson> = HashMap::with_capacity(docs.len());
        let mut errors: Vec<BulkWriteError> = Vec::with_capacity(docs.len());

        for (i, doc) in docs.iter().enumerate() {
            let bson_doc = match bson::to_document(doc).map_err(Error::BsonSerialization) {
                Ok(d) => d,
                Err(e) => {
                    errors.push(BulkWriteError {
                        index: i,
                        code: e.code().unwrap_or(1),
                        message: e.to_string(),
                    });
                    if opts.ordered {
                        break;
                    }
                    continue;
                }
            };
            match self.engine.insert(name, bson_doc) {
                Ok(id) => {
                    inserted_ids.insert(i, id);
                }
                Err(e) => {
                    errors.push(BulkWriteError {
                        index: i,
                        code: e.code().unwrap_or(1),
                        message: e.to_string(),
                    });
                    if opts.ordered {
                        break;
                    }
                }
            }
        }

        Ok(InsertManyResult {
            inserted_ids,
            errors,
        })
    }

    pub(crate) fn find_one<T: DeserializeOwned>(
        &self,
        name: &str,
        filter: Document,
    ) -> Result<Option<T>> {
        #[cfg(feature = "tracing")]
        {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            for k in filter.keys() {
                k.hash(&mut h);
            }
            tracing::debug!(
                target: "mqlite",
                collection = name,
                filter_hash = h.finish(),
                doc_count = 0u64,
                "mqlite::find"
            );
        }
        match self.engine.find_one(name, &filter)? {
            None => Ok(None),
            Some(doc) => bson::from_document(doc)
                .map(Some)
                .map_err(Error::BsonDeserialization),
        }
    }

    pub(crate) fn find<T: DeserializeOwned>(
        &self,
        name: &str,
        filter: Document,
        opts: FindOptions,
    ) -> Result<Cursor<T>> {
        #[cfg(feature = "tracing")]
        {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            for k in filter.keys() {
                k.hash(&mut h);
            }
            tracing::debug!(
                target: "mqlite",
                collection = name,
                filter_hash = h.finish(),
                doc_count = 0u64,
                "mqlite::find"
            );
        }
        let (docs, explain) = self.engine.find(name, &filter, &opts)?;
        Ok(Cursor::new(docs, explain))
    }

    pub(crate) fn update_one(
        &self,
        name: &str,
        filter: Document,
        update: Document,
        opts: UpdateOptions,
    ) -> Result<UpdateResult> {
        self.engine.update(name, &filter, &update, &opts, false)
    }

    pub(crate) fn update_many(
        &self,
        name: &str,
        filter: Document,
        update: Document,
        opts: UpdateOptions,
    ) -> Result<UpdateResult> {
        self.engine.update(name, &filter, &update, &opts, true)
    }

    pub(crate) fn delete_one(&self, name: &str, filter: Document) -> Result<DeleteResult> {
        self.engine.delete(name, &filter, false)
    }

    pub(crate) fn delete_many(&self, name: &str, filter: Document) -> Result<DeleteResult> {
        self.engine.delete(name, &filter, true)
    }

    pub(crate) fn find_one_and_update_with_options<T: DeserializeOwned>(
        &self,
        name: &str,
        filter: Document,
        update: Document,
        opts: FindOneAndUpdateOptions,
    ) -> Result<Option<T>> {
        match self
            .engine
            .find_one_and_update(name, &filter, &update, &opts)?
        {
            None => Ok(None),
            Some(doc) => bson::from_document(doc)
                .map(Some)
                .map_err(Error::BsonDeserialization),
        }
    }

    pub(crate) fn find_one_and_delete_with_options<T: DeserializeOwned>(
        &self,
        name: &str,
        filter: Document,
        opts: FindOneAndDeleteOptions,
    ) -> Result<Option<T>> {
        match self.engine.find_one_and_delete(name, &filter, &opts)? {
            None => Ok(None),
            Some(doc) => bson::from_document(doc)
                .map(Some)
                .map_err(Error::BsonDeserialization),
        }
    }

    pub(crate) fn find_one_and_replace_with_options<T: Serialize + DeserializeOwned>(
        &self,
        name: &str,
        filter: Document,
        replacement: &T,
        opts: FindOneAndReplaceOptions,
    ) -> Result<Option<T>> {
        let replacement_doc = bson::to_document(replacement).map_err(Error::BsonSerialization)?;
        match self
            .engine
            .find_one_and_replace(name, &filter, &replacement_doc, &opts)?
        {
            None => Ok(None),
            Some(doc) => bson::from_document(doc)
                .map(Some)
                .map_err(Error::BsonDeserialization),
        }
    }

    pub(crate) fn estimated_document_count(&self, name: &str) -> Result<u64> {
        self.engine.count(name, &Document::new())
    }

    pub(crate) fn count_documents(&self, name: &str, filter: Document) -> Result<u64> {
        self.engine.count(name, &filter)
    }

    pub(crate) fn create_index(&self, name: &str, model: IndexModel) -> Result<String> {
        self.engine.create_index(name, &model)
    }

    pub(crate) fn drop_index(&self, name: &str, index_name: &str) -> Result<()> {
        self.engine.drop_index(name, index_name)
    }

    pub(crate) fn list_indexes(&self, name: &str) -> Result<Vec<IndexInfo>> {
        self.engine.list_indexes(name)
    }

    pub(crate) fn list_collection_names(&self) -> Result<Vec<String>> {
        self.engine.list_namespaces()
    }

    pub(crate) fn drop_collection(&self, name: &str) -> Result<()> {
        self.engine.drop_namespace(name)
    }

    pub(crate) fn create_collection(&self, name: &str) -> Result<()> {
        self.engine.create_namespace(name)
    }

    pub(crate) fn checkpoint(&self) -> Result<()> {
        if self.path.is_none() {
            return Ok(());
        }

        self.engine.checkpoint()
    }

    pub(crate) fn backup(&self, dest: &Path) -> Result<()> {
        let src_path = self
            .path
            .as_deref()
            .ok_or_else(|| Error::Internal("backup: no source path available".into()))?;

        // Security: reject symlinks at the destination path.
        reject_symlink(dest)?;

        // Reject backup-to-self: canonicalize both paths if dest already
        // exists.  If dest does not exist yet, it cannot be the same file.
        if dest.exists() {
            let dest_canon = std::fs::canonicalize(dest).unwrap_or_default();
            let src_canon = std::fs::canonicalize(src_path).unwrap_or_default();
            if !dest_canon.as_os_str().is_empty()
                && !src_canon.as_os_str().is_empty()
                && dest_canon == src_canon
            {
                return Err(Error::Internal(
                    "backup: destination is the same file as the source".into(),
                ));
            }
        }

        // Checkpoint: flush dirty buffer-pool pages to the journal, then move all
        // journal frames into the main file.  After this, the main file contains
        // the complete committed state and is safe to copy.
        self.engine.checkpoint()?;

        // Determine the byte length of the database file.
        let file_size = std::fs::metadata(src_path)?.len();

        // Copy the database file to dest using the *existing* file_lock fd
        // for reads.  We must NOT open a new file descriptor to the source
        // while the advisory lock is held: POSIX guarantees that closing ANY
        // fd to a file releases ALL advisory locks the process holds on that
        // file (the "POSIX advisory lock footgun").
        let mut dest_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(dest)
            .map_err(Error::Io)?;

        // Create the destination file with restricted permissions (0600) on
        // Unix, matching the behaviour of Client::open for new database files.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            dest_file
                .set_permissions(std::fs::Permissions::from_mode(0o600))
                .map_err(Error::Io)?;
        }

        // Stream the source file contents in 64 KB chunks through the lock fd.
        use std::io::Write;
        const CHUNK: usize = 64 * 1024;
        let mut buf = vec![0u8; CHUNK];
        let mut offset: u64 = 0;

        while offset < file_size {
            let remaining = (file_size - offset) as usize;
            let read_len = remaining.min(CHUNK);
            let chunk = &mut buf[..read_len];

            self.file_lock.read_exact_at(offset, chunk)?;
            dest_file.write_all(chunk).map_err(Error::Io)?;

            offset += read_len as u64;
        }

        // Flush the destination file's data to the OS page cache.
        dest_file.flush().map_err(Error::Io)?;

        Ok(())
    }
}
