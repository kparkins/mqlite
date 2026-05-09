//! Secondary index build on a non-trivial collection.
//!
//! Measures the end-to-end time of `create_index` on a collection seeded with
//! 10 000 documents.  Index is non-unique on the "category" field (int).
//!
//! Setup (seed inserts) is done once outside the timed region; only
//! `create_index` is measured.  Each iteration drops and recreates the index
//! to keep the measurement stable across samples.
//!
//! Run:
//!   cargo bench --bench index_build -- --save-baseline current

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
use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use mqlite::IndexModel;
use tempfile::TempDir;

mod common;

const DOC_COUNT: usize = 10_000;
const PAYLOAD_BYTES: usize = 64;
const PAYLOAD_CLASS: &str = "~64B";
const INDEX_SPEC: &str = "category:1 non-unique";
const INDEX_NAME: &str = "category_1";

fn metadata() -> String {
    format!(
        "doc_count={DOC_COUNT} payload_class={PAYLOAD_CLASS} actual_bytes={PAYLOAD_BYTES} \
         index_spec=\"{INDEX_SPEC}\" durability={} {}",
        common::INTERVAL_100MS_LABEL,
        common::host_metadata()
    )
}

fn category_index_model() -> IndexModel {
    IndexModel::builder().keys(doc! { "category": 1 }).build()
}

fn bench_index_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("index_build");
    group.sample_size(10);
    // Index build on 10k docs can take a few seconds; allow enough room.
    group.measurement_time(Duration::from_secs(30));
    group.warm_up_time(Duration::from_secs(5));

    eprintln!("[index_build] {}", metadata());

    // Seed the collection once outside the Criterion timing loop.
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("bench.mqlite");
    let client = common::open_client(&path, common::interval_100ms());
    let col = client.database("bench").collection::<Document>("big_col");

    let payload = "x".repeat(PAYLOAD_BYTES);
    // Batch-insert in chunks to avoid holding a huge allocation.
    let chunk_size = 500;
    let mut inserted = 0i32;
    while inserted < DOC_COUNT as i32 {
        let end = (inserted + chunk_size).min(DOC_COUNT as i32);
        let docs: Vec<Document> = (inserted..end)
            .map(|i| {
                doc! {
                    "_id": i,
                    "category": i % 100,
                    "payload": &payload,
                }
            })
            .collect();
        col.insert_many(&docs)
            .ordered(false)
            .run()
            .expect("bulk insert must succeed");
        inserted = end;
    }

    let created_name = col
        .create_index(category_index_model())
        .expect("initial create_index must succeed");
    assert_eq!(created_name, INDEX_NAME);

    group.bench_function("create_index_10k", |b| {
        b.iter_batched(
            || {
                col.drop_index(INDEX_NAME)
                    .expect("drop_index must prepare a cold build");
            },
            |_| {
                col.create_index(category_index_model())
                    .expect("create_index must succeed");
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_index_build);
criterion_main!(benches);
