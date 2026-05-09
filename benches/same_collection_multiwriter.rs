//! Same-collection multi-writer benchmark harness.
//!
//! Exercises one pre-split collection with 1/2/4/8/16 concurrent root-neutral
//! update writers. Writers use disjoint key bands so the workload targets
//! separate leaf ranges after the pre-split seed step.
//!
//! Run:
//!   cargo bench --bench same_collection_multiwriter -- --save-baseline current

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test and bench targets use assertion-style panics and setup unwraps"
)]
#![allow(missing_docs)]

mod common;

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use bson::{doc, Document};
use criterion::{
    criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use mqlite::{Client, DurabilityMode, OpenOptions};
use tempfile::TempDir;

use common::non_empty_command_output;

const DATABASE_NAME: &str = "multiwriter_bench";
const COLLECTION_NAME: &str = "multiwriter";
const EXPECTED_NAMESPACE_ID: i64 = 1;
const OPS_PER_WRITER: i32 = 1;
const KEY_BAND_WIDTH: i64 = 1_000_000_000;
const KEY_STRIDE: i64 = 1_000;
const PRE_SPLIT_ANCHORS_PER_WRITER: i32 = 16;
const SEED_PAYLOAD_BYTES: usize = 4_096;
const SEED_CHUNK_SIZE: usize = 64;
const WRITER_COUNTS: &[usize] = &[1, 2, 4, 8, 16];
const PAYLOAD_CLASSES: &[PayloadClass] = &[
    PayloadClass {
        label: "256B",
        bytes: 256,
    },
    PayloadClass {
        label: "4KiB",
        bytes: 4_096,
    },
    PayloadClass {
        label: "32KiB",
        bytes: 32_768,
    },
];

#[derive(Clone, Copy)]
struct PayloadClass {
    label: &'static str,
    bytes: usize,
}

#[derive(Clone)]
struct DurabilityCase {
    label: &'static str,
    mode: DurabilityMode,
}

fn durability_cases() -> [DurabilityCase; 2] {
    [
        DurabilityCase {
            label: "Interval(100ms)",
            mode: DurabilityMode::Interval(Duration::from_millis(100)),
        },
        DurabilityCase {
            label: "FullSync",
            mode: DurabilityMode::FullSync,
        },
    ]
}

fn metadata(writer_count: usize, payload: PayloadClass, durability: &str) -> String {
    let rustc = non_empty_command_output("rustc", &["--version"]);
    let git_commit = non_empty_command_output("git", &["rev-parse", "HEAD"]);
    let cpu_model = cpu_model().replace('"', "'");
    let core_count = core_count();
    let arch = std::env::consts::ARCH;
    let os = std::env::consts::OS;

    format!(
        "writers={writer_count} namespace_id={EXPECTED_NAMESPACE_ID} \
         payload_class={} actual_bytes={} durability={} rustc=\"{}\" \
         machine_shape=\"cpu_model={};core_count={};arch={};os={}\" git_commit={}",
        payload.label,
        payload.bytes,
        durability,
        rustc,
        cpu_model,
        core_count,
        arch,
        os,
        git_commit
    )
}

fn cpu_model() -> String {
    non_empty_command_output(
        "sh",
        &[
            "-c",
            "sysctl -n machdep.cpu.brand_string 2>/dev/null || \
             awk -F: '/model name/ {gsub(/^[ \\t]+/, \"\", $2); print $2; exit}' \
             /proc/cpuinfo 2>/dev/null || echo unknown",
        ],
    )
}

fn core_count() -> usize {
    std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
}

fn open_client(dir: &TempDir, mode: DurabilityMode) -> Client {
    let path = dir.path().join("same-collection-multiwriter.mqlite");
    let opts = OpenOptions::new()
        .durability(mode)
        .busy_timeout(Duration::from_secs(30));
    Client::open_with_options(&path, opts).expect("open must succeed")
}

fn seed_pre_split_collection(client: &Client, writer_count: usize) {
    let collection = client
        .database(DATABASE_NAME)
        .collection::<Document>(COLLECTION_NAME);
    let payload = "s".repeat(SEED_PAYLOAD_BYTES);
    let mut docs = Vec::with_capacity(SEED_CHUNK_SIZE);

    for writer_id in 0..writer_count {
        let band_start = writer_key_band(writer_id);
        for anchor in 0..PRE_SPLIT_ANCHORS_PER_WRITER {
            docs.push(doc! {
                "_id": band_start + i64::from(anchor) * KEY_STRIDE,
                "seed": true,
                "writer_band": writer_id as i32,
                "payload": &payload,
            });

            if docs.len() == SEED_CHUNK_SIZE {
                collection
                    .insert_many(&docs)
                    .ordered(false)
                    .run()
                    .expect("pre-split seed insert must succeed");
                docs.clear();
            }
        }
    }

    if !docs.is_empty() {
        collection
            .insert_many(&docs)
            .ordered(false)
            .run()
            .expect("pre-split seed insert must succeed");
    }
}

fn writer_key_band(writer_id: usize) -> i64 {
    writer_id as i64 * KEY_BAND_WIDTH
}

fn writer_key(writer_id: usize, batch: i64, op_index: i32) -> i64 {
    let anchor = (batch * i64::from(OPS_PER_WRITER) + i64::from(op_index))
        .rem_euclid(i64::from(PRE_SPLIT_ANCHORS_PER_WRITER));
    writer_key_band(writer_id) + anchor * KEY_STRIDE
}

fn run_writer_batch(client: Client, writer_count: usize, payload: Arc<String>, batch: i64) {
    let barrier = Arc::new(Barrier::new(writer_count));
    let handles: Vec<_> = (0..writer_count)
        .map(|writer_id| {
            let client = client.clone();
            let barrier = Arc::clone(&barrier);
            let payload = Arc::clone(&payload);
            thread::spawn(move || {
                barrier.wait();
                let collection = client
                    .database(DATABASE_NAME)
                    .collection::<Document>(COLLECTION_NAME);
                for op_index in 0..OPS_PER_WRITER {
                    collection
                        .update_one(
                            doc! { "_id": writer_key(writer_id, batch, op_index) },
                            doc! {
                                "$set": {
                                    "writer": writer_id as i32,
                                    "seq": batch,
                                    "payload": payload.as_str(),
                                }
                            },
                        )
                        .run()
                        .expect("same-collection multiwriter update must succeed");
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("writer thread panicked");
    }
}

fn bench_same_collection_multiwriter(c: &mut Criterion) {
    let mut group = c.benchmark_group("same_collection_multiwriter");
    group.sample_size(10);
    group.measurement_time(Duration::from_millis(100));
    group.warm_up_time(Duration::from_millis(50));
    group.sampling_mode(SamplingMode::Flat);

    for durability in durability_cases() {
        for payload in PAYLOAD_CLASSES {
            for &writer_count in WRITER_COUNTS {
                let total_ops = (OPS_PER_WRITER as usize * writer_count) as u64;
                group.throughput(Throughput::Elements(total_ops));

                eprintln!(
                    "[same_collection_multiwriter] {}",
                    metadata(writer_count, *payload, durability.label)
                );

                let id = BenchmarkId::from_parameter(format!(
                    "durability={}_payload={}_writers={}",
                    durability.label, payload.label, writer_count
                ));

                group.bench_with_input(id, &writer_count, |b, &wc| {
                    let dir = TempDir::new().expect("tempdir");
                    let client = open_client(&dir, durability.mode.clone());
                    seed_pre_split_collection(&client, wc);
                    let payload = Arc::new("x".repeat(payload.bytes));
                    let next_batch = AtomicI64::new(0);

                    b.iter(|| {
                        let batch = next_batch.fetch_add(1, Ordering::Relaxed);
                        run_writer_batch(client.clone(), wc, Arc::clone(&payload), batch);
                    });
                });
            }
        }
    }

    group.finish();
}

criterion_group!(benches, bench_same_collection_multiwriter);
criterion_main!(benches);
