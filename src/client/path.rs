//! Path and header helpers shared by `Client::open*` and `ClientInner::backup`.

use std::path::{Path, PathBuf};

use crate::{
    error::{Error, Result},
    storage::header::{FileHeader, HEADER_PAGE_SIZE},
};

/// Check whether `path` is a symlink and return an error if so.
///
/// Uses `symlink_metadata()` which does **not** follow symlinks (unlike `metadata()`).
/// If the path does not exist yet, this is not an error — a new file will be created.
///
/// # Security
/// Symlink following at `Client::open()` time could allow an attacker who controls
/// the filesystem path to redirect the database open to an arbitrary file (e.g.,
/// `/etc/passwd`).  See mqlite security.md threat #12.
pub(super) fn reject_symlink(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => Err(Error::SymlinkRejected {
            path: path.to_owned(),
        }),
        // Exists and is a regular file or directory — OK.
        Ok(_) => Ok(()),
        // Path does not exist yet (will be created as a new database) — OK.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        // Any other IO error (permission denied, etc.) — propagate.
        Err(e) => Err(Error::Io(e)),
    }
}

/// Returns the expected journal file path for a given database path.
///
/// Journal files use the naming convention `<db-path>-journal`.
pub(super) fn journal_path(db_path: &Path) -> PathBuf {
    let mut s = db_path.as_os_str().to_owned();
    s.push("-journal");
    PathBuf::from(s)
}

/// Read and validate the page-0 [`FileHeader`] from the backing file via the
/// lock file descriptor.
pub(super) fn read_and_validate_header(
    lock: &dyn crate::storage::lock::FileLock,
    path: &Path,
) -> Result<FileHeader> {
    let mut buf = [0u8; HEADER_PAGE_SIZE];
    lock.read_exact_at(0, &mut buf)?;
    let header = FileHeader::from_bytes(&buf).map_err(|e| enrich_path(e, path))?;
    header.validate().map_err(|e| enrich_path(e, path))?;
    Ok(header)
}

/// Write a fresh [`FileHeader`] as page 0 via the lock file descriptor.
pub(super) fn write_initial_header(lock: &dyn crate::storage::lock::FileLock) -> Result<()> {
    let header = FileHeader::new_now();
    let bytes = header.to_bytes();
    lock.write_at(0, &bytes)
}

/// Attach the real on-disk path to a [`Error::CorruptDatabase`] whose `path`
/// field was left empty by the parser (which doesn't know the path).
fn enrich_path(e: Error, path: &Path) -> Error {
    match e {
        Error::CorruptDatabase {
            path: ref p,
            ref detail,
            recoverable,
        } if p == std::path::Path::new("") => Error::CorruptDatabase {
            path: path.to_owned(),
            detail: detail.clone(),
            recoverable,
        },
        other => other,
    }
}

/// Create (or open) a database file with restricted permissions (`0600`).
pub(super) fn create_db_file_secure(path: &Path) -> Result<std::fs::File> {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }

    Ok(file)
}
