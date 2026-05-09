//! Phase 8 LSN group-commit benchmark gate.
//!
//! Run:
//!   cargo bench --bench group_commit_lsn

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "bench target uses assertion-style panics and setup unwraps"
)]
#![allow(missing_docs)]

use std::fs;
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bson::{doc, Document};
use mqlite::{Client, DurabilityMode, OpenOptions};
use tempfile::TempDir;

const ARTIFACT_PATH: &str = ".omc/artifacts/phase8-bench.json";
const DATABASE_NAME: &str = "phase8_bench";
const COLLECTION_NAME: &str = "docs";
const WRITER_COUNTS: &[usize] = &[1, 2, 4, 8];
const COMMITS_PER_WRITER: usize = 8;
const PAYLOAD_BYTES: usize = 256;
const FULLSYNC_FOUR_WRITER_THRESHOLD: f64 = 0.50;

#[derive(Debug)]
struct BenchCase {
    writer_count: usize,
    commits: usize,
    fsyncs: u64,
    fsyncs_per_commit: f64,
    p50_latency_us: u128,
    single_writer_p50_latency_us: u128,
}

fn open_client(dir: &TempDir) -> Client {
    let path = dir.path().join("group-commit-lsn.mqlite");
    let opts = OpenOptions::new()
        .durability(DurabilityMode::FullSync)
        .busy_timeout(Duration::from_secs(30));
    Client::open_with_options(&path, opts).expect("open bench database")
}

fn p50_latency_us(latencies: &mut [u128]) -> u128 {
    latencies.sort_unstable();
    latencies[latencies.len() / 2]
}

fn run_case(writer_count: usize, single_writer_p50_latency_us: u128) -> BenchCase {
    let dir = TempDir::new().expect("tempdir");
    let client = open_client(&dir);
    client
        .database(DATABASE_NAME)
        .create_collection(COLLECTION_NAME)
        .expect("pre-create benchmark collection");
    client.__reset_journal_sync_observations();

    let start = Arc::new(Barrier::new(writer_count));
    let latencies = Arc::new(Mutex::new(Vec::with_capacity(
        writer_count * COMMITS_PER_WRITER,
    )));
    let payload = Arc::new("x".repeat(PAYLOAD_BYTES));

    let handles = (0..writer_count)
        .map(|writer_id| {
            let worker = client.clone();
            let start = Arc::clone(&start);
            let latencies = Arc::clone(&latencies);
            let payload = Arc::clone(&payload);
            thread::spawn(move || {
                let collection = worker
                    .database(DATABASE_NAME)
                    .collection::<Document>(COLLECTION_NAME);
                for seq in 0..COMMITS_PER_WRITER {
                    start.wait();
                    let op_start = Instant::now();
                    collection
                        .insert_one(&doc! {
                            "_id": ((writer_id * COMMITS_PER_WRITER) + seq) as i32,
                            "writer": writer_id as i32,
                            "seq": seq as i32,
                            "payload": payload.as_str(),
                        })
                        .expect("FullSync insert must succeed");
                    latencies
                        .lock()
                        .expect("latency mutex poisoned")
                        .push(op_start.elapsed().as_micros());
                }
            })
        })
        .collect::<Vec<_>>();

    for handle in handles {
        handle.join().expect("writer thread joined");
    }

    let commits = writer_count * COMMITS_PER_WRITER;
    let observed = client.__journal_sync_os_boundaries();
    let docs = client
        .database(DATABASE_NAME)
        .collection::<Document>(COLLECTION_NAME)
        .count_documents(doc! {})
        .expect("count bench docs");
    assert_eq!(docs, commits as u64);

    let mut latencies = latencies.lock().expect("latency mutex poisoned");
    assert_eq!(latencies.len(), commits);
    let p50_latency_us = p50_latency_us(&mut latencies);

    BenchCase {
        writer_count,
        commits,
        fsyncs: observed,
        fsyncs_per_commit: observed as f64 / commits as f64,
        p50_latency_us,
        single_writer_p50_latency_us,
    }
}

fn json_for_cases(cases: &[BenchCase], threshold_met: bool) -> String {
    let single = cases
        .iter()
        .find(|case| case.writer_count == 1)
        .expect("single-writer case exists");
    let mut json = String::from("{\n");
    json.push_str("  \"bench\": \"group_commit_lsn\",\n");
    json.push_str("  \"durability\": \"FullSync\",\n");
    json.push_str(&format!(
        "  \"commits_per_writer\": {COMMITS_PER_WRITER},\n"
    ));
    json.push_str(&format!(
        "  \"single_writer_fsyncs_per_commit\": {:.6},\n",
        single.fsyncs_per_commit
    ));
    json.push_str(&format!(
        "  \"single_writer_p50_latency_us\": {},\n",
        single.p50_latency_us
    ));
    json.push_str(&format!(
        "  \"four_writer_threshold_ratio\": {:.2},\n",
        FULLSYNC_FOUR_WRITER_THRESHOLD
    ));
    json.push_str(&format!(
        "  \"four_writer_threshold_met\": {},\n",
        if threshold_met { "true" } else { "false" }
    ));
    json.push_str("  \"cases\": [\n");
    for (idx, case) in cases.iter().enumerate() {
        let comma = if idx + 1 == cases.len() { "" } else { "," };
        json.push_str(&format!(
            "    {{\"writers\": {}, \"commits\": {}, \"fsyncs\": {}, \
             \"fsyncs_per_commit\": {:.6}, \"p50_latency_us\": {}, \
             \"single_writer_p50_latency_us\": {}, \
             \"single_writer_p50_latency_ratio\": {:.6}}}{}\n",
            case.writer_count,
            case.commits,
            case.fsyncs,
            case.fsyncs_per_commit,
            case.p50_latency_us,
            case.single_writer_p50_latency_us,
            case.p50_latency_us as f64 / case.single_writer_p50_latency_us as f64,
            comma
        ));
    }
    json.push_str("  ]\n");
    json.push_str("}\n");
    json
}

fn main() {
    let mut cases = Vec::with_capacity(WRITER_COUNTS.len());
    let mut single_writer_p50_latency_us = 0;
    for &writer_count in WRITER_COUNTS {
        let case = run_case(writer_count, single_writer_p50_latency_us);
        if writer_count == 1 {
            single_writer_p50_latency_us = case.p50_latency_us;
            cases.push(BenchCase {
                single_writer_p50_latency_us,
                ..case
            });
        } else {
            cases.push(BenchCase {
                single_writer_p50_latency_us,
                ..case
            });
        }
    }

    let single = cases
        .iter()
        .find(|case| case.writer_count == 1)
        .expect("single-writer case exists");
    let four = cases
        .iter()
        .find(|case| case.writer_count == 4)
        .expect("four-writer case exists");
    let threshold = single.fsyncs_per_commit * FULLSYNC_FOUR_WRITER_THRESHOLD;
    let threshold_met = four.fsyncs_per_commit <= threshold;
    let json = json_for_cases(&cases, threshold_met);

    fs::create_dir_all(".omc/artifacts").expect("create Phase 8 artifact directory");
    fs::write(ARTIFACT_PATH, json).expect("write Phase 8 benchmark artifact");
    eprintln!("[group_commit_lsn] wrote {ARTIFACT_PATH}");
    eprintln!(
        "[group_commit_lsn] single_writer_fsyncs_per_commit={:.6} \
         four_writer_fsyncs_per_commit={:.6} threshold={:.6}",
        single.fsyncs_per_commit, four.fsyncs_per_commit, threshold
    );

    assert!(
        threshold_met,
        "FullSync four-writer fsyncs_per_commit {:.6} exceeds {:.2}x \
         single-writer value {:.6}",
        four.fsyncs_per_commit, FULLSYNC_FOUR_WRITER_THRESHOLD, single.fsyncs_per_commit
    );
}
