// Bug-suspect #4 (deep-refactor-2026-06-10): `ReservedLogRecord` has no Drop.
//
// HYPOTHESIS (suspect 25(b) in the ranked plan): if a `ReservedLogRecord`
// that owns a real log-manager slot is dropped WITHOUT `write_and_mark()` or
// `poison_slot()`, the slot is left permanently `Reserved` in the slot map.
// The contiguous ready frontier (`mark_written`'s forward walk) only advances
// over `Written` slots, so a `Reserved` hole below a later-written record can
// never be consumed — `ready_lsn` is wedged, and `wait_ready` / `ensure_sync`
// for any LSN at or beyond that hole blocks forever with no error.
//
// This is a FRAGILITY with a real, demonstrable wedge: the type permits the
// dangerous state. Production commit paths hand-route every reserved slot to
// `write_and_mark` or `poison_slot`, but nothing mechanical enforces it. This
// test pins the mechanism so the missing `Drop` backstop is added.
//
// VERDICT pending the assertion below:
//   - If `ensure_sync` HANGS (watchdog times out) → the wedge is real; the
//     fix is a poisoning `Drop` on `ReservedLogRecord`.
//   - If `ensure_sync` RETURNS an `EngineFatal` promptly → the backstop is in
//     place and this test becomes a pinning regression guard.

#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc
)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex as StdMutex};
    use std::time::Duration;

    use crate::error::Error;
    use crate::journal::log_manager::{PositionedLogFile, PositionedLogIo};
    use crate::journal::wire::LogRecordDraft;
    use crate::journal::{JournalManager, LogManager};
    use crate::mvcc::timestamp::Ts;

    /// In-memory positioned log I/O that always succeeds. Sync is a no-op so the
    /// only thing that can keep `ensure_sync` from returning is the ready-frontier
    /// wedge under test, not a sync stall.
    struct MemLogIo {
        data: StdMutex<Vec<u8>>,
        sync_calls: AtomicUsize,
    }

    impl MemLogIo {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                data: StdMutex::new(Vec::new()),
                sync_calls: AtomicUsize::new(0),
            })
        }
    }

    impl PositionedLogIo for Arc<MemLogIo> {
        fn write_at(&self, offset: u64, data: &[u8]) -> std::io::Result<usize> {
            let mut buf = self.data.lock().unwrap();
            let start = offset as usize;
            let end = start + data.len();
            if end > buf.len() {
                buf.resize(end, 0);
            }
            buf[start..end].copy_from_slice(data);
            Ok(data.len())
        }

        fn sync_data(&self) -> std::io::Result<()> {
            self.sync_calls.fetch_add(1, Ordering::AcqRel);
            Ok(())
        }
    }

    fn log_manager() -> Arc<LogManager> {
        let io = MemLogIo::new();
        Arc::new(LogManager::from_positioned_file(
            PositionedLogFile::from_io(Box::new(io)),
            0,
        ))
    }

    fn crud_draft(publish_seq: u64, fill: u8) -> LogRecordDraft {
        // A minimal, well-formed CRUD draft. The inner payload bytes need not be
        // a decodable logical/chain frame for this test — `reserve` only needs
        // the encoded length, and `write_and_mark` only writes the finalized
        // bytes; we never run recovery here.
        LogRecordDraft::crud(
            1,
            publish_seq,
            Ts {
                physical_ms: 1,
                logical: 0,
            },
            vec![fill; 4],
            vec![fill; 4],
        )
    }

    #[test]
    fn dropped_reserved_record_must_not_wedge_ready_frontier_forever() {
        let manager = log_manager();

        // Reserve a slot and DROP it without write_and_mark / poison_slot.
        // This models a reserved-but-abandoned record (e.g. an early return or
        // panic on a future code path that forgets to finalize the slot).
        let abandoned_end_lsn = {
            let reserved = JournalManager::reserve_log_record_on(&manager, crud_draft(10, 0xAA))
                .expect("reserve abandoned slot");
            assert!(
                reserved.is_journaled(),
                "slot must own a real log-manager slot"
            );
            reserved.end_lsn()
            // `reserved` drops here without being written or poisoned.
        };

        // The ready frontier sits below the abandoned slot: nothing has been
        // marked written, so `ready_lsn` is still the initial seed.
        assert!(
            manager.ready_lsn() < abandoned_end_lsn,
            "ready frontier must sit below the abandoned reservation (wedge setup)"
        );

        // Demand durability through the abandoned slot's end LSN on a watchdog
        // thread.
        //
        //   Unfixed (no Drop backstop): the slot stays `Reserved` forever, the
        //   contiguous ready frontier can never reach `abandoned_end_lsn`, and
        //   `ensure_sync` blocks indefinitely → the recv times out → this test
        //   fails, confirming the wedge.
        //
        //   Fixed (poisoning Drop): the abandoned slot poisoned the manager on
        //   drop, so `ensure_sync`'s `wait_ready` surfaces `EngineFatal`
        //   promptly instead of hanging.
        let (tx, rx) = mpsc::channel();
        let worker_manager = Arc::clone(&manager);
        std::thread::spawn(move || {
            let result = worker_manager.ensure_sync(abandoned_end_lsn);
            // Ignore send error: if the receiver already gave up (timeout), the
            // main thread has moved on.
            let _ = tx.send(result);
        });

        match rx.recv_timeout(Duration::from_secs(3)) {
            Ok(result) => match result {
                Err(Error::EngineFatal { .. }) => {
                    // Correct post-fix behavior: the abandoned reservation is an
                    // observable fatal error, not a silent durability wedge.
                }
                other => panic!(
                    "ensure_sync through an abandoned reserved slot must fail EngineFatal \
                     (an Ok would mean the frontier advanced past an unwritten hole — a \
                     durability violation); got {other:?}"
                ),
            },
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // The wedge is real: ensure_sync never returned. The worker
                // thread is still parked on the ready frontier and cannot be
                // joined (it would block forever), so we leak it and fail loudly.
                panic!(
                    "BUG-SUSPECT-4 CONFIRMED: dropping a journaled ReservedLogRecord without \
                     write_and_mark/poison_slot left the ready frontier wedged below \
                     {abandoned_end_lsn}; ensure_sync blocked forever. ReservedLogRecord needs a \
                     poisoning Drop."
                );
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("watchdog channel disconnected unexpectedly");
            }
        }
    }
}
