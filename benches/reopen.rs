//! Reopen latency under two scenarios.
//!
//! Group 1 – reopen-after-journal: seed N docs with FullSync, then
//! `std::mem::forget` the client (prevents the Drop checkpoint, leaving the
//! journal non-empty on disk).  Each timed iteration clones the seeded
//! db+journal into a fresh tempdir via `iter_batched` so `Client::open`
//! actually replays a non-empty journal on every measurement.
//!
//! Group 2 – reopen-after-emergency-checkpoint: same workload but use a tiny
//! `journal_max_size` (256 KiB) and `journal_auto_checkpoint(50)` so the
//! engine triggers emergency checkpoints internally before we reopen.  Same
//! iter_batched clone strategy.
//!
//! Each scenario records: journal byte size, legacy page frame count replayed,
//! and ChainCommit frame count replayed, using the recovery-metrics counters
//! from `mqlite::mvcc::metrics`.  These are captured via a throwaway
//! `peek_reopen_frames` that `mem::forget`s its client so the seeded journal
//! survives for the timed loop.
//!
//! Run:
//!   cargo bench --bench reopen -- --save-baseline current

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]
#![allow(missing_docs)]

use std::time::Duration;

use bson::doc;
use bson::Document;
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use mqlite::{Client, DurabilityMode, OpenOptions};
use tempfile::TempDir;

const SEED_DOCS: usize = 200;
const PAYLOAD_BYTES: usize = 512;

fn metadata(
    scenario: &str,
    journal_bytes: u64,
    legacy_page_frames: u64,
    chain_commit_frames: u64,
) -> String {
    let rustc = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_else(|_| "unknown".to_owned());

    let cpu_count = num_cpus();
    let arch = std::env::consts::ARCH;
    let os = std::env::consts::OS;
    let durability = "FullSync";

    format!(
        "scenario={scenario} journal_bytes={journal_bytes} \
         legacy_page_frames={legacy_page_frames} chain_commit_frames={chain_commit_frames} \
         durability={durability} rustc=\"{rustc}\" cpu_count={cpu_count} \
         arch={arch} os={os}"
    )
}

fn num_cpus() -> usize {
    std::process::Command::new("sh")
        .arg("-c")
        .arg("nproc 2>/dev/null || sysctl -n hw.logicalcpu 2>/dev/null || echo 1")
        .output()
        .ok()
        .and_then(|o| String::from_utf8_lossy(&o.stdout).trim().parse().ok())
        .unwrap_or(1)
}

/// Seed `count` documents into a fresh database at `path`.
///
/// Uses `std::mem::forget` to prevent the Drop checkpoint so the journal
/// remains non-empty on disk (leaving ChainCommit + legacy page frames for
/// the subsequent reopen to replay).
fn seed_database(path: &std::path::Path, opts: OpenOptions, count: usize) {
    let client = Client::open_with_options(path, opts).expect("open for seeding must succeed");
    let col = client
        .database("bench")
        .collection::<Document>("reopen_col");
    let payload = "x".repeat(PAYLOAD_BYTES);
    for i in 0..count as i32 {
        col.insert_one(&doc! { "_id": i, "payload": &payload })
            .expect("seed insert must succeed");
    }
    // Intentionally leak the client so Drop does not run the checkpoint.
    // The journal remains on disk with all commit frames intact.
    // Safety: the TempDir is removed at bench end, freeing all OS resources.
    std::mem::forget(client);
}

/// Return the byte size of the journal file at `<db_path>-journal`.
fn journal_size(db_path: &std::path::Path) -> u64 {
    let mut journal_path = db_path.as_os_str().to_owned();
    journal_path.push("-journal");
    std::fs::metadata(std::path::Path::new(&journal_path))
        .map(|m| m.len())
        .unwrap_or(0)
}

/// Reset recovery counters, open the database, capture the frame counts the
/// recovery loop replayed, then `std::mem::forget` the client so the journal
/// remains on disk for later reopens.  Returns `(legacy_page_frames,
/// chain_commit_frames)`.
///
/// NOTE: do NOT call `client.close()` here — close() runs a checkpoint which
/// empties the journal, which would defeat the point of measuring reopen.
fn peek_reopen_frames(path: &std::path::Path) -> (u64, u64) {
    mqlite::mvcc::metrics::reset_recovery_legacy_page_frames();
    mqlite::mvcc::metrics::reset_recovery_chain_commit_frames();
    let opts = OpenOptions::new().durability(DurabilityMode::FullSync);
    let client = Client::open_with_options(path, opts).expect("reopen must succeed");
    let legacy = mqlite::mvcc::metrics::recovery_legacy_page_frames_snapshot();
    let chain = mqlite::mvcc::metrics::recovery_chain_commit_frames_snapshot();
    // Safety: intentionally leak; subsequent bench iterations will open on
    // fresh copies of the seeded db+journal files, so the leaked client
    // never collides with the timed loop.
    std::mem::forget(client);
    (legacy, chain)
}

/// Copy the seeded db + its `-journal` sidecar into a fresh tempdir and
/// return the destination db path.  Each bench iteration uses a pristine
/// copy so the timed `Client::open` actually replays a non-empty journal.
fn clone_seeded(template_db: &std::path::Path) -> (TempDir, std::path::PathBuf) {
    let dir = TempDir::new().expect("tempdir for iteration");
    let dest_db = dir.path().join("reopen_iter.mqlite");
    std::fs::copy(template_db, &dest_db).expect("copy seeded db");
    let mut template_journal = template_db.as_os_str().to_owned();
    template_journal.push("-journal");
    let mut dest_journal = dest_db.as_os_str().to_owned();
    dest_journal.push("-journal");
    // The template journal may not exist if the engine already checkpointed;
    // copy only if present so this helper handles both paths.
    if std::path::Path::new(&template_journal).exists() {
        std::fs::copy(&template_journal, &dest_journal).expect("copy seeded journal");
    }
    (dir, dest_db)
}

fn bench_reopen(c: &mut Criterion) {
    // -----------------------------------------------------------------
    // Group 1: reopen-after-journal
    // -----------------------------------------------------------------
    {
        let mut group = c.benchmark_group("reopen-after-journal");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));
        group.warm_up_time(Duration::from_secs(2));

        let id = BenchmarkId::from_parameter("seed=200docs_FullSync");

        // Seed once; std::mem::forget leaves journal non-empty.
        let dir = TempDir::new().expect("tempdir");
        let db_path = dir.path().join("reopen_journal.mqlite");
        let seed_opts = OpenOptions::new().durability(DurabilityMode::FullSync);
        seed_database(&db_path, seed_opts, SEED_DOCS);

        let jsize = journal_size(&db_path);
        // Peek at the recovered frame counts on a throwaway reopen; the
        // client is forgotten (not closed) so the seeded journal remains on
        // disk to drive subsequent bench iterations.
        let (legacy_frames, chain_frames) = peek_reopen_frames(&db_path);
        eprintln!(
            "[reopen] {}",
            metadata("reopen-after-journal", jsize, legacy_frames, chain_frames)
        );

        group.bench_with_input(id, &db_path, |b, path| {
            b.iter_batched(
                || clone_seeded(path),
                |(_dir, dest_db)| {
                    // Each iteration opens a fresh copy of the seeded
                    // db+journal, so recovery actually replays a non-empty
                    // journal on every measurement (not a clean reopen).
                    let opts = OpenOptions::new().durability(DurabilityMode::FullSync);
                    let client =
                        Client::open_with_options(&dest_db, opts).expect("reopen must succeed");
                    let col = client
                        .database("bench")
                        .collection::<Document>("reopen_col");
                    let _ = col.count_documents(doc! {}).expect("count must succeed");
                    std::mem::forget(client);
                },
                BatchSize::SmallInput,
            );
        });

        group.finish();
    }

    // -----------------------------------------------------------------
    // Group 2: reopen-after-emergency-checkpoint
    // -----------------------------------------------------------------
    {
        let mut group = c.benchmark_group("reopen-after-emergency-checkpoint");
        group.sample_size(10);
        group.measurement_time(Duration::from_secs(10));
        group.warm_up_time(Duration::from_secs(2));

        let id = BenchmarkId::from_parameter("seed=200docs_emergency_ckpt");

        // Use a small journal_max_size so emergency checkpoints fire.
        let dir = TempDir::new().expect("tempdir");
        let db_path = dir.path().join("reopen_emcp.mqlite");
        let seed_opts = OpenOptions::new()
            .durability(DurabilityMode::FullSync)
            .journal_max_size(256 * 1024) // 256 KB
            .journal_auto_checkpoint(50);
        seed_database(&db_path, seed_opts, SEED_DOCS);

        let jsize = journal_size(&db_path);
        let (legacy_frames, chain_frames) = peek_reopen_frames(&db_path);
        eprintln!(
            "[reopen] {}",
            metadata(
                "reopen-after-emergency-checkpoint",
                jsize,
                legacy_frames,
                chain_frames
            )
        );

        group.bench_with_input(id, &db_path, |b, path| {
            b.iter_batched(
                || clone_seeded(path),
                |(_dir, dest_db)| {
                    let opts = OpenOptions::new().durability(DurabilityMode::FullSync);
                    let client =
                        Client::open_with_options(&dest_db, opts).expect("reopen must succeed");
                    let col = client
                        .database("bench")
                        .collection::<Document>("reopen_col");
                    let _ = col.count_documents(doc! {}).expect("count must succeed");
                    std::mem::forget(client);
                },
                BatchSize::SmallInput,
            );
        });

        group.finish();
    }
}

criterion_group!(benches, bench_reopen);
criterion_main!(benches);
