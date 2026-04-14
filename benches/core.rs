//! Core benchmarks for mqlite.
//!
//! These benchmarks cover the operations that are already implemented in Phase 0
//! (database handles and collection instantiation). Storage engine operations will
//! be benchmarked once the Phase 1 implementation lands.

use criterion::{criterion_group, criterion_main, Criterion};
use mqlite::Database;

fn bench_open_in_memory(c: &mut Criterion) {
    c.bench_function("Database::open_in_memory", |b| {
        b.iter(|| {
            let db = Database::open_in_memory().expect("in-memory DB should always open");
            // Access the collection handle to avoid the database being optimized away.
            let _col = db.collection::<bson::Document>("bench_collection");
        })
    });
}

fn bench_collection_handle(c: &mut Criterion) {
    let db = Database::open_in_memory().expect("in-memory DB should always open");
    c.bench_function("Database::collection (clone handle)", |b| {
        b.iter(|| {
            let _col = db.collection::<bson::Document>("bench_collection");
        })
    });
}

criterion_group!(benches, bench_open_in_memory, bench_collection_handle);
criterion_main!(benches);
