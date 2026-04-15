//! G7 Performance benchmarks for mqlite.
//!
//! All benchmarks use **file-backed** databases (via [`tempfile::TempDir`]).
//! In-memory databases are intentionally avoided so that results reflect
//! real I/O costs and are comparable across platforms.
//!
//! ## G7 target benchmarks
//!
//! | Benchmark | Target |
//! |-----------|--------|
//! | `g7/point_lookup_cached` | < 10 µs |
//! | `g7/point_lookup_uncached` | < 1 ms |
//! | `g7/indexed_range_scan_100` | < 5 ms |
//! | `g7/insert_full_sync` | < 2 ms |
//! | `g7/bulk_10k_interval` | < 500 ms |
//!
//! The CI pipeline compares results against the stored baseline and fails if
//! any benchmark regresses by more than 2× (200%).
//!
//! See `.benchmarks/REFERENCE_HARDWARE.md` for the CI runner specification.

use bson::{doc, Document};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use mqlite::{Client, DurabilityMode, IndexModel, OpenOptions};
use std::path::Path;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a document with varied field values.
fn make_doc(i: u64) -> Document {
    doc! {
        "x": i as i64,
        "y": format!("value_{i}"),
        "z": (i % 100) as i64,
    }
}

/// Open a **file-backed** database at `path` with default options and
/// pre-populate it with `n` documents.
fn file_db_with_docs(path: &Path, n: usize) -> Client {
    let client = Client::open(path).expect("file-backed open");
    let col = client.database("bench").collection::<Document>("bench");
    for i in 0..n as u64 {
        col.insert_one(&make_doc(i)).expect("insert");
    }
    client
}

/// Create a `TempDir` + pre-populated file-backed database.
///
/// Returns `(TempDir, Client)`.  The `TempDir` must be kept alive for the
/// lifetime of `Client` so the underlying file is not removed prematurely.
fn setup_file_db(n: usize) -> (TempDir, Client) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("bench.mqlite");
    let client = file_db_with_docs(&path, n);
    (dir, client)
}

// ---------------------------------------------------------------------------
// 1. insert_one — batches of 1 K, 10 K, and 100 K documents
//
// Measures aggregate throughput (docs/second) for inserting N documents into
// a fresh file-backed collection.  Uses `DurabilityMode::None` to focus on
// engine overhead rather than fsync latency.
// ---------------------------------------------------------------------------

fn bench_insert_one(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_one");

    for &n in &[1_000_usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let docs: Vec<Document> = (0..n as u64).map(make_doc).collect();
                    let dir = TempDir::new().expect("tempdir");
                    let path = dir.path().join("bench.mqlite");
                    let client = Client::open_with_options(
                        &path,
                        OpenOptions::new().durability(DurabilityMode::None),
                    )
                    .expect("open");
                    (dir, client, docs)
                },
                |(_dir, client, docs)| {
                    let col = client.database("bench").collection::<Document>("bench");
                    for d in &docs {
                        col.insert_one(d).expect("insert");
                    }
                    (client, col)
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 2. find_one — single point lookup by `_id`
//
// Pre-populates a 10 K-document file-backed collection, resolves the `_id`
// of the middle document once during setup, then measures repeated find_one
// calls for that specific `_id`.  The database stays open between iterations
// so the OS page cache is warm.
// ---------------------------------------------------------------------------

fn bench_find_one_by_id(c: &mut Criterion) {
    let (_dir, client) = setup_file_db(10_000);
    let col = client.database("bench").collection::<Document>("bench");

    // Resolve the _id of the document with x == 5_000 once.
    let mid = col
        .find_one(doc! { "x": 5_000_i64 })
        .expect("query")
        .expect("exists");
    let id = mid.get("_id").expect("_id present").clone();

    c.bench_function("find_one/by_id", |b| {
        b.iter(|| col.find_one(doc! { "_id": id.clone() }).expect("query"));
    });
}

// ---------------------------------------------------------------------------
// 3. find with filter — index scan vs collection scan
//
// Both variants query for `z == 42` against a 10 K-document file-backed
// collection.  The "index_scan" variant builds an index on `z` before the
// benchmark loop; the "collection_scan" variant deliberately omits it.
// ---------------------------------------------------------------------------

fn bench_find_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("find");

    // Collection scan (no index on "z").
    {
        let (_dir, client) = setup_file_db(10_000);
        let col = client.database("bench").collection::<Document>("bench");
        group.bench_function("collection_scan", |b| {
            b.iter(|| col.find(doc! { "z": 42_i64 }).expect("query").count());
        });
    }

    // Index scan (ascending index on "z").
    {
        let (_dir, client) = setup_file_db(10_000);
        let col = client.database("bench").collection::<Document>("bench");
        col.create_index(
            IndexModel::builder()
                .keys(doc! { "z": 1 })
                .build()
                .expect("model"),
        )
        .expect("create_index");
        group.bench_function("index_scan", |b| {
            b.iter(|| col.find(doc! { "z": 42_i64 }).expect("query").count());
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 4. find with sort + limit — top-N query
//
// Fetches the top 10 documents sorted by `x` descending from a 10 K
// file-backed collection.  Exercises the in-engine sort + limit pipeline.
// ---------------------------------------------------------------------------

fn bench_find_sort_limit(c: &mut Criterion) {
    let (_dir, client) = setup_file_db(10_000);
    let col = client.database("bench").collection::<Document>("bench");

    c.bench_function("find/sort_limit_10", |b| {
        b.iter(|| {
            col.find_with_options(
                doc! {},
                mqlite::FindOptions::new()
                    .sort(doc! { "x": -1_i32 })
                    .limit(10),
            )
            .expect("query")
            .count()
        });
    });
}

// ---------------------------------------------------------------------------
// 5. update_one — single document update
//
// Repeatedly updates the document with `x == 5_000` in a 10 K file-backed
// collection.  The filter always matches exactly one document, isolating
// single-document write latency.
// ---------------------------------------------------------------------------

fn bench_update_one(c: &mut Criterion) {
    let (_dir, client) = setup_file_db(10_000);
    let col = client.database("bench").collection::<Document>("bench");

    c.bench_function("update_one", |b| {
        b.iter(|| {
            col.update_one(doc! { "x": 5_000_i64 }, doc! { "$set": { "y": "updated" } })
                .expect("update")
        });
    });
}

// ---------------------------------------------------------------------------
// 6. update_many — bulk update
//
// Updates all ~100 documents with `z == 42` in a 10 K file-backed collection.
// Exercises the full multi-document write path.
// ---------------------------------------------------------------------------

fn bench_update_many(c: &mut Criterion) {
    let (_dir, client) = setup_file_db(10_000);
    let col = client.database("bench").collection::<Document>("bench");

    c.bench_function("update_many", |b| {
        b.iter(|| {
            col.update_many(
                doc! { "z": 42_i64 },
                doc! { "$set": { "y": "bulk_updated" } },
            )
            .expect("update")
        });
    });
}

// ---------------------------------------------------------------------------
// 7. delete_one — single document delete
//
// Uses iter_batched so each iteration starts with a fresh 10 K file-backed
// collection, ensuring the target document is always present and the
// measurement is consistent across iterations.
// ---------------------------------------------------------------------------

fn bench_delete_one(c: &mut Criterion) {
    c.bench_function("delete_one", |b| {
        b.iter_batched(
            || setup_file_db(10_000),
            |(_dir, client)| {
                let col = client.database("bench").collection::<Document>("bench");
                col.delete_one(doc! { "x": 5_000_i64 }).expect("delete")
            },
            BatchSize::LargeInput,
        );
    });
}

// ---------------------------------------------------------------------------
// 8. Cursor iteration — iterate large result set
//
// Fetches all 10 K documents from a file-backed collection and drives the
// cursor to exhaustion.  Measures iterator overhead plus deserialisation.
// ---------------------------------------------------------------------------

fn bench_cursor_iteration(c: &mut Criterion) {
    let (_dir, client) = setup_file_db(10_000);
    let col = client.database("bench").collection::<Document>("bench");

    c.bench_function("cursor/iterate_10k", |b| {
        b.iter(|| {
            let mut count = 0_u64;
            for _result in col.find(doc! {}).expect("query") {
                count += 1;
            }
            count
        });
    });
}

// ---------------------------------------------------------------------------
// 9. create_index — on an existing file-backed collection
//
// Measures the time to build a single-field ascending index from scratch
// on file-backed collections of 1 K and 10 K documents.  Uses iter_batched
// so each iteration starts without the index already present.
// ---------------------------------------------------------------------------

fn bench_create_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("create_index");

    for &n in &[1_000_usize, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || setup_file_db(n),
                |(_dir, client)| {
                    let col = client.database("bench").collection::<Document>("bench");
                    col.create_index(
                        IndexModel::builder()
                            .keys(doc! { "z": 1 })
                            .build()
                            .expect("model"),
                    )
                    .expect("create_index");
                    (client, col)
                },
                BatchSize::LargeInput,
            );
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// 10. checkpoint — WAL flush performance
//
// Opens a file-backed database with DurabilityMode::None (no fsync), writes
// 1 K documents to accumulate WAL entries, then benchmarks the checkpoint
// (WAL-to-main-file flush) operation.
//
// Uses iter_batched so each iteration starts with a fresh WAL containing
// exactly 1 K writes.
// ---------------------------------------------------------------------------

fn bench_checkpoint(c: &mut Criterion) {
    c.bench_function("checkpoint/after_1k_writes", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().expect("tempdir");
                let path = dir.path().join("bench.mqlite");
                let client = Client::open_with_options(
                    &path,
                    OpenOptions::new().durability(DurabilityMode::None),
                )
                .expect("open");
                let col = client.database("bench").collection::<Document>("bench");
                for i in 0..1_000_u64 {
                    col.insert_one(&make_doc(i)).expect("insert");
                }
                (client, dir) // keep `dir` alive so the file is not deleted
            },
            |(client, _dir)| client.checkpoint().expect("checkpoint"),
            BatchSize::LargeInput,
        );
    });
}

// ---------------------------------------------------------------------------
// G7 target benchmarks
//
// These five benchmarks correspond directly to the G7 performance targets
// defined in docs/specs/phase1-reconciliation.md §3.4.  Each benchmark is
// named under the "g7/" group for easy CI filtering.
//
// Targets (reference hardware — see .benchmarks/REFERENCE_HARDWARE.md):
//
//   g7/point_lookup_cached    < 10 µs
//   g7/point_lookup_uncached  < 1 ms
//   g7/indexed_range_scan_100 < 5 ms
//   g7/insert_full_sync       < 2 ms
//   g7/bulk_10k_interval      < 500 ms
// ---------------------------------------------------------------------------

/// G7-1 — Point lookup by `_id`, **warm cache** (< 10 µs target).
///
/// The database stays open and the file is fully loaded into the OS page
/// cache before measurement begins.  Criterion drives the loop until the
/// measurement is statistically stable, so by the time the first timing
/// sample is taken the cache is hot.
fn bench_g7_point_lookup_cached(c: &mut Criterion) {
    let mut group = c.benchmark_group("g7");

    let (_dir, client) = setup_file_db(10_000);
    let col = client.database("bench").collection::<Document>("bench");

    // Warm up: run one lookup to pull the target page into the OS cache.
    let mid = col
        .find_one(doc! { "x": 5_000_i64 })
        .expect("query")
        .expect("exists");
    let id = mid.get("_id").expect("_id present").clone();

    group.bench_function("point_lookup_cached", |b| {
        b.iter(|| col.find_one(doc! { "_id": id.clone() }).expect("query"));
    });

    group.finish();
}

/// G7-2 — Point lookup by `_id`, **cold database** (< 1 ms target).
///
/// Each iteration opens a fresh `Client` against the same file.  This forces
/// the engine to re-read its header and any un-checkpointed WAL before
/// servicing the lookup, simulating a cold-start scenario.
fn bench_g7_point_lookup_uncached(c: &mut Criterion) {
    let mut group = c.benchmark_group("g7");

    // Write the 10 K documents once; reuse the same file across iterations.
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("bench.mqlite");
    {
        let client = file_db_with_docs(&path, 10_000);
        // Checkpoint so the WAL is empty — each fresh open won't replay a WAL.
        client.checkpoint().expect("checkpoint");
    }

    // Resolve the target _id using a warm connection (not measured).
    let id = {
        let client = Client::open(&path).expect("probe open");
        let col = client.database("bench").collection::<Document>("bench");
        let doc = col
            .find_one(doc! { "x": 5_000_i64 })
            .expect("query")
            .expect("exists");
        doc.get("_id").expect("_id present").clone()
    };

    group.bench_function("point_lookup_uncached", |b| {
        b.iter_batched(
            || Client::open(&path).expect("cold open"),
            |client| {
                let col = client.database("bench").collection::<Document>("bench");
                col.find_one(doc! { "_id": id.clone() }).expect("query")
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

/// G7-3 — Indexed range scan returning ~100 documents (< 5 ms target).
///
/// A 10 K collection has ~100 documents where `z == 42` (1 % of the
/// collection, since `z = i % 100`).  An ascending index on `z` is built
/// once during setup; each iteration scans through all matching documents
/// via the index.
fn bench_g7_indexed_range_scan_100(c: &mut Criterion) {
    let mut group = c.benchmark_group("g7");

    let (_dir, client) = setup_file_db(10_000);
    let col = client.database("bench").collection::<Document>("bench");

    col.create_index(
        IndexModel::builder()
            .keys(doc! { "z": 1 })
            .build()
            .expect("model"),
    )
    .expect("create_index");

    group.bench_function("indexed_range_scan_100", |b| {
        b.iter(|| {
            // Consume the entire cursor to measure scan + deserialisation cost.
            col.find(doc! { "z": 42_i64 })
                .expect("query")
                .count()
        });
    });

    group.finish();
}

/// G7-4 — Single `insert_one` with `FullSync` durability (< 2 ms target).
///
/// `DurabilityMode::FullSync` calls `fsync()` (or equivalent) after every
/// committed write, giving the strongest durability guarantee at the cost of
/// a full disk flush per write.  This benchmark measures the end-to-end
/// latency of a single insert under those conditions.
///
/// Uses `iter_batched` so each iteration inserts into a fresh database,
/// preventing accumulated WAL size from skewing later measurements.
fn bench_g7_insert_full_sync(c: &mut Criterion) {
    let mut group = c.benchmark_group("g7");

    group.bench_function("insert_full_sync", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().expect("tempdir");
                let path = dir.path().join("bench.mqlite");
                let client = Client::open_with_options(
                    &path,
                    OpenOptions::new().durability(DurabilityMode::FullSync),
                )
                .expect("open");
                (dir, client)
            },
            |(_dir, client)| {
                let col = client.database("bench").collection::<Document>("bench");
                col.insert_one(&make_doc(0)).expect("insert")
            },
            BatchSize::PerIteration,
        );
    });

    group.finish();
}

/// G7-5 — Bulk insert of 10 K documents with `Interval(100 ms)` durability
/// (< 500 ms target).
///
/// `DurabilityMode::Interval(100ms)` is the default mode — it fsyncs the WAL
/// at most every 100 ms, balancing durability and throughput.  This benchmark
/// measures the wall-clock time to insert 10 K documents end-to-end.
///
/// Uses `iter_batched` so each iteration starts with a fresh database.
fn bench_g7_bulk_10k_interval(c: &mut Criterion) {
    let mut group = c.benchmark_group("g7");
    group.throughput(Throughput::Elements(10_000));

    let docs: Vec<Document> = (0..10_000_u64).map(make_doc).collect();

    group.bench_function("bulk_10k_interval", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().expect("tempdir");
                let path = dir.path().join("bench.mqlite");
                let client = Client::open_with_options(
                    &path,
                    OpenOptions::new()
                        .durability(DurabilityMode::Interval(std::time::Duration::from_millis(100))),
                )
                .expect("open");
                (dir, client)
            },
            |(_dir, client)| {
                let col = client.database("bench").collection::<Document>("bench");
                for d in &docs {
                    col.insert_one(d).expect("insert");
                }
                client
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Criterion plumbing
// ---------------------------------------------------------------------------

criterion_group!(
    benches,
    bench_insert_one,
    bench_find_one_by_id,
    bench_find_filter,
    bench_find_sort_limit,
    bench_update_one,
    bench_update_many,
    bench_delete_one,
    bench_cursor_iteration,
    bench_create_index,
    bench_checkpoint,
    bench_g7_point_lookup_cached,
    bench_g7_point_lookup_uncached,
    bench_g7_indexed_range_scan_100,
    bench_g7_insert_full_sync,
    bench_g7_bulk_10k_interval,
);
criterion_main!(benches);
