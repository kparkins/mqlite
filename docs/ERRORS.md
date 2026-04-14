# mqlite Error Guide

All mqlite operations return `mqlite::Result<T>`, which is `Result<T, mqlite::Error>`.

## Error Variants

### `Error::Io`

An OS-level I/O error (permissions, disk full, file not found).

**Common causes:**
- Path to database directory does not exist
- Process lacks write permission on the database file

**Recovery:** Check file system permissions and available disk space.

---

### `Error::WriterBusy`

Another writer process already holds the exclusive lock on the database file.

mqlite uses a single-writer model: only one process may write at a time.

**Recovery options:**

```rust
use mqlite::{Database, OpenOptions};
use std::time::Duration;

// Option 1: set a busy timeout (blocks until lock is available or timeout)
let db = Database::open_with_options(
    "myapp.mqlite",
    OpenOptions::new().busy_timeout(Duration::from_secs(5)),
)?;

// Option 2: custom busy handler
let db = Database::open_with_options(
    "myapp.mqlite",
    OpenOptions::new().busy_handler(|attempts| attempts < 10),
)?;

// Option 3: open read-only (does not need exclusive lock)
let db = Database::open_with_options(
    "myapp.mqlite",
    OpenOptions::new().read_only(true),
)?;
# Ok::<(), mqlite::Error>(())
```

---

### `Error::DuplicateKey`

A write violated a unique index constraint.

**MongoDB error code:** 11000

**Recovery:** Either use a different key value or use `upsert` if you want to
replace an existing document.

---

### `Error::CorruptDatabase`

The database file header is invalid, truncated, or has a bad checksum.

**Recovery options:**
- Open in read-only mode to access the last checkpointed state
- Restore from a backup
- Phase 2 will add `Database::repair()`

---

### `Error::SymlinkRejected`

The database path points to a symlink. mqlite refuses to follow symlinks to
prevent symlink attacks (see security.md threat #12).

**MongoDB error code:** 2 (BAD_VALUE)

**Recovery:** Open the real file path directly.

---

### `Error::DiskFull`

A write failed because the disk is full.

The error includes:
- `required_bytes` — bytes needed
- `available_bytes` — bytes available

**Recovery:** Free disk space, then retry.

---

### `Error::UnsupportedOperator`

A query filter or update used an operator not supported in Phase 1.

**MongoDB error code:** 9

See the [Compatibility Matrix](COMPATIBILITY.md) for the full list of supported operators.

---

### `Error::UnsupportedIndexOption`

`create_index` was called with an unsupported index type (TTL, text, geospatial, hashed).

**MongoDB error code:** 67 (CannotCreateIndex)

Supported types: single-field, compound, unique, sparse, multikey.

---

### `Error::DocumentTooLarge`

The document exceeds the 16MB BSON size limit.

**MongoDB error code:** 10334

---

### `Error::DocumentValidationFailure`

The document failed structural validation (nesting too deep, too many fields, invalid field names).

**MongoDB error code:** 121

---

### `Error::CursorNotFound`

The referenced cursor has expired or does not exist (wire protocol only).

**MongoDB error code:** 43

---

### `Error::CollectionNotFound`

The referenced collection does not exist.

**MongoDB error code:** 26

---

### `Error::InvalidWireMessage`

The wire protocol received a malformed message (wrong magic, size exceeds limit, unsupported opcode).

**MongoDB error code:** 48 (IllegalOperation)

---

### `Error::Internal`

An internal invariant was violated. This should not happen in correct usage.

**MongoDB error code:** 1

If you encounter this, please file a bug report.

## MongoDB Error Codes

mqlite maps errors to MongoDB error codes for wire protocol compatibility:

| Code | Constant | Variant |
|------|----------|---------|
| 1 | `INTERNAL_ERROR` | `Error::Internal` |
| 2 | `BAD_VALUE` | `Error::SymlinkRejected` |
| 9 | `UNSUPPORTED_OPERATOR` | `Error::UnsupportedOperator` |
| 26 | `NAMESPACE_NOT_FOUND` | `Error::CollectionNotFound` |
| 43 | `CURSOR_NOT_FOUND` | `Error::CursorNotFound` |
| 48 | `ILLEGAL_OP` | `Error::InvalidWireMessage` |
| 67 | `CANNOT_CREATE_INDEX` | `Error::UnsupportedIndexOption` |
| 115 | `UNSUPPORTED_FORMAT` | — |
| 121 | `DOCUMENT_VALIDATION_FAILURE` | `Error::DocumentValidationFailure` |
| 10334 | `DOCUMENT_TOO_LARGE` | `Error::DocumentTooLarge` |
| 11000 | `DUPLICATE_KEY` | `Error::DuplicateKey` |
