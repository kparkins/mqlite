//! Counter/gauge generator macros shared by the `metrics` submodules.
//!
//! Three shapes cover every repetitive triple in this subsystem:
//!
//!   define_counter!(STATIC, record_fn, snapshot_fn, reset_fn, doc)
//!     — fetch_add(1) record, load snapshot, store(0) reset.
//!
//!   define_batch_counter!(STATIC, record_fn, snapshot_fn, reset_fn, param, doc)
//!     — fetch_add(param) if param > 0, load snapshot, store(0) reset.
//!
//!   define_gauge!(STATIC, set_fn, snapshot_fn, reset_fn, param, doc)
//!     — store(param) set, load snapshot, store(0) reset.
//!
//! Each macro accepts a leading `$(#[$meta:meta])*` token tree so that
//! doc-comment attributes pass through to the generated static and functions.

macro_rules! define_counter {
    (
        $(#[$smeta:meta])*
        $static:ident,
        $(#[$rmeta:meta])* $record:ident,
        $(#[$snmeta:meta])* $snapshot:ident,
        $(#[$rsmeta:meta])* $reset:ident $(,)?
    ) => {
        $(#[$smeta])*
        pub static $static: AtomicU64 = AtomicU64::new(0);

        $(#[$rmeta])*
        pub fn $record() {
            $static.fetch_add(1, Ordering::Relaxed);
        }

        $(#[$snmeta])*
        pub fn $snapshot() -> u64 {
            $static.load(Ordering::Relaxed)
        }

        $(#[$rsmeta])*
        pub fn $reset() {
            $static.store(0, Ordering::Relaxed);
        }
    };
}

macro_rules! define_batch_counter {
    (
        $(#[$smeta:meta])*
        $static:ident,
        $(#[$rmeta:meta])* $record:ident ($param:ident : u64),
        $(#[$snmeta:meta])* $snapshot:ident,
        $(#[$rsmeta:meta])* $reset:ident $(,)?
    ) => {
        $(#[$smeta])*
        pub static $static: AtomicU64 = AtomicU64::new(0);

        $(#[$rmeta])*
        pub fn $record($param: u64) {
            if $param > 0 {
                $static.fetch_add($param, Ordering::Relaxed);
            }
        }

        $(#[$snmeta])*
        pub fn $snapshot() -> u64 {
            $static.load(Ordering::Relaxed)
        }

        $(#[$rsmeta])*
        pub fn $reset() {
            $static.store(0, Ordering::Relaxed);
        }
    };
}

macro_rules! define_gauge {
    (
        $(#[$smeta:meta])*
        $static:ident,
        $(#[$smeta2:meta])* $set:ident ($param:ident : u64),
        $(#[$snmeta:meta])* $snapshot:ident,
        $(#[$rsmeta:meta])* $reset:ident $(,)?
    ) => {
        $(#[$smeta])*
        pub static $static: AtomicU64 = AtomicU64::new(0);

        $(#[$smeta2])*
        pub fn $set($param: u64) {
            $static.store($param, Ordering::Relaxed);
        }

        $(#[$snmeta])*
        pub fn $snapshot() -> u64 {
            $static.load(Ordering::Relaxed)
        }

        $(#[$rsmeta])*
        pub fn $reset() {
            $static.store(0, Ordering::Relaxed);
        }
    };
}

pub(crate) use define_batch_counter;
pub(crate) use define_counter;
pub(crate) use define_gauge;
