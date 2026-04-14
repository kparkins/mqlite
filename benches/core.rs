//! G7 Performance benchmarks for mqlite.
//!
//! Covers all PRD G7 target operations with criterion for CI regression detection.
//! The CI pipeline compares results against the baseline and fails if any
//! operation regresses by more than 2× (200%).
//!
//! See `.benchmarks/REFERENCE_HARDWARE.md` for the reference CI runner specification.

use bson::{doc, Document};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};
use mqlite::{Database, DurabilityMode, IndexModel, OpenOptions};
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

/// Open an in-memory database pre-populated with `n` documents.
fn db_with_docs(n: usize) -> Database {
    let db = Database::open_in_memory().expect("in-memory open");
    let col = db.collection::<Document>("bench");
    for i in 0..n as u64 {
        col.insert_one(&make_doc(i)).expect("insert");
    }
    db
}

// ---------------------------------------------------------------------------
// 1. insert_one — batches of 1 K, 10 K, and 100 K documents
//
// Measures the aggregate time to insert N documents into a fresh in-memory
// collection. Throughput is expressed as documents/second via
// `Throughput::Elements`.
// ---------------------------------------------------------------------------

fn bench_insert_one(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_one");

    for &n in &[1_000_usize, 10_000, 100_000] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || {
                    let docs: Vec<Document> = (0..n as u64).map(make_doc).collect();
                    (Database::open_in_memory().expect("open"), docs)
                },
                |(db, docs)| {
                    let col = db.collection::<Document>("bench");
                    for d in &docs {
                        col.insert_one(d).expect("insert");
                    }
                    // Return db to prevent the optimizer from dropping it early.
                    (db, col)
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
// Pre-populates a 10 K-document collection, resolves the `_id` of the
// middle document once during setup, then measures repeated find_one calls
// for that specific `_id`.
// ---------------------------------------------------------------------------

fn bench_find_one_by_id(c: &mut Criterion) {
    let db = db_with_docs(10_000);
    let col = db.collection::<Document>("bench");

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
// Both variants query for `z == 42` against a 10 K-document collection.
// The "index_scan" variant builds an index on `z` before the benchmark loop;
// the "collection_scan" variant deliberately omits it.
// ---------------------------------------------------------------------------

fn bench_find_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("find");

    // Collection scan (no index on "z").
    {
        let db = db_with_docs(10_000);
        let col = db.collection::<Document>("bench");
        group.bench_function("collection_scan", |b| {
            b.iter(|| col.find(doc! { "z": 42_i64 }).expect("query").count());
        });
    }

    // Index scan (ascending index on "z").
    {
        let db = db_with_docs(10_000);
        let col = db.collection::<Document>("bench");
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
// collection.  Exercises the in-engine sort + limit pipeline.
// ---------------------------------------------------------------------------

fn bench_find_sort_limit(c: &mut Criterion) {
    let db = db_with_docs(10_000);
    let col = db.collection::<Document>("bench");

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
// Repeatedly updates the document with `x == 5_000` in a 10 K collection.
// The filter always matches exactly one document, so the benchmark isolates
// single-document write latency.
// ---------------------------------------------------------------------------

fn bench_update_one(c: &mut Criterion) {
    let db = db_with_docs(10_000);
    let col = db.collection::<Document>("bench");

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
// Updates all ~100 documents with `z == 42` in a 10 K collection.
// Exercises the full multi-document write path.
// ---------------------------------------------------------------------------

fn bench_update_many(c: &mut Criterion) {
    let db = db_with_docs(10_000);
    let col = db.collection::<Document>("bench");

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
// Uses iter_batched so each iteration starts with a fresh 10 K collection,
// ensuring the target document is always present and the measurement is
// consistent across iterations.
// ---------------------------------------------------------------------------

fn bench_delete_one(c: &mut Criterion) {
    c.bench_function("delete_one", |b| {
        b.iter_batched(
            || db_with_docs(10_000),
            |db| {
                let col = db.collection::<Document>("bench");
                col.delete_one(doc! { "x": 5_000_i64 }).expect("delete")
            },
            BatchSize::LargeInput,
        );
    });
}

// ---------------------------------------------------------------------------
// 8. Cursor iteration — iterate large result set
//
// Fetches all 10 K documents from an in-memory collection and drives the
// cursor to exhaustion.  Measures iterator overhead plus deserialisation.
// ---------------------------------------------------------------------------

fn bench_cursor_iteration(c: &mut Criterion) {
    let db = db_with_docs(10_000);
    let col = db.collection::<Document>("bench");

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
// 9. create_index — on an existing collection
//
// Measures the time to build a single-field ascending index from scratch
// on collections of 1 K and 10 K documents.  Uses iter_batched so each
// iteration starts without the index already present.
// ---------------------------------------------------------------------------

fn bench_create_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("create_index");

    for &n in &[1_000_usize, 10_000] {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter_batched(
                || db_with_docs(n),
                |db| {
                    let col = db.collection::<Document>("bench");
                    col.create_index(
                        IndexModel::builder()
                            .keys(doc! { "z": 1 })
                            .build()
                            .expect("model"),
                    )
                    .expect("create_index");
                    (db, col)
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
                let db = Database::open_with_options(
                    &path,
                    OpenOptions::new().durability(DurabilityMode::None),
                )
                .expect("open");
                let col = db.collection::<Document>("bench");
                for i in 0..1_000_u64 {
                    col.insert_one(&make_doc(i)).expect("insert");
                }
                (db, dir) // keep `dir` alive so the file is not deleted
            },
            |(db, _dir)| db.checkpoint().expect("checkpoint"),
            BatchSize::LargeInput,
        );
    });
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
);
criterion_main!(benches);
