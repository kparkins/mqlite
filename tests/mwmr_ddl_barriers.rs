//! Phase 5 §10.17 / §10.17.1 / §10.21 — DDL/CRUD barrier tests for US-006.
//!
//! Locks down the metadata-guard protocol:
//!
//!  * `run_write_commit_envelope` holds exactly one `metadata.read()` acquisition,
//!    bracketing only the id-capture + `NsWriterRegistry::admit` scope, and
//!    the guard is dropped before the body call (AC #2, AC #5 source gate).
//!  * Ordinary CRUD never advances `PublishedEpoch.catalog_generation`; only
//!    DDL paths reserve a fresh generation via
//!    `SharedState.next_catalog_gen.fetch_add(...)` and stamp it through the
//!    publish closure (AC #1, AC #3, AC #5).
//!  * The captured-identity gate at S3.5 revalidates the target
//!    namespace/index identity when global catalog generation advances. It
//!    only returns `Error::WriteConflict { CatalogGenerationChanged }` when
//!    the target identity changed; unrelated DDL must not break MWMR writes
//!    (AC #4).
//!  * Concurrent DDL on a different namespace cannot deadlock with an
//!    in-flight CRUD writer paused inside its body (AC #6 — anti-deadlock
//!    contract; the post-durable poison contract proper is owned by US-036
//!    and tested in `tests/mwmr_crash_recovery.rs`).
//!  * Concurrent DDL on the SAME namespace either drains the admitted
//!    writer or beats it to `metadata.write()`; both interleavings
//!    terminate without a metadata/registry cycle (AC #7).

#![cfg(feature = "test-hooks")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    reason = "test target uses assertion-style panics and setup unwraps"
)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use bson::{doc, Bson, Document};
use mqlite::error::{EngineFatalReason, Error, WriteConflictReason};
use mqlite::mvcc::{ReadView, Ts};
use mqlite::{Client, IndexModel};

const DB: &str = "p5ddl";
const COLL_A: &str = "alpha";
const COLL_B: &str = "beta";
const NS_A: &str = "p5ddl.alpha";
const FIRST_NAMESPACE_ID: i64 = 1;
const SETTLE_DEADLINE: Duration = Duration::from_secs(5);
const SHORT_SLEEP: Duration = Duration::from_millis(50);
const TAG_INDEX: &str = "tag_1";

fn open_with_collection_a() -> (tempfile::TempDir, Client) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("us006.mqlite");
    let client = Client::open(&path).unwrap();
    client.database(DB).create_collection(COLL_A).unwrap();
    // Seed a doc so the published epoch advances past the bootstrap and
    // subsequent CRUD takes the `run_write_commit_envelope` path (not the
    // bootstrap-and-retry path inside `run_write`).
    client
        .database(DB)
        .collection::<Document>(COLL_A)
        .insert_one(&doc! { "_id": 0i32, "v": "seed" })
        .unwrap();
    (dir, client)
}

fn tag_index_model() -> IndexModel {
    IndexModel::builder().keys(doc! { "tag": 1 }).build()
}

fn wait_for_flag(flag: &AtomicBool, description: &str) {
    let deadline = Instant::now() + SETTLE_DEADLINE;
    while !flag.load(Ordering::Acquire) {
        assert!(
            Instant::now() < deadline,
            "{description} did not finish within {SETTLE_DEADLINE:?}",
        );
        thread::sleep(Duration::from_millis(5));
    }
}

fn flag_set_within(flag: &AtomicBool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while !flag.load(Ordering::Acquire) {
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(5));
    }
    true
}

// ---------------------------------------------------------------------------
// AC #5 source gate: exactly one `metadata.read()` in `run_write_commit_envelope`.
// ---------------------------------------------------------------------------

/// AC #5 / §10.17 step 3 — `run_write_commit_envelope` holds exactly one
/// `metadata.read()` acquisition and drops it before the body call.
///
/// Repository-wide single-line greps cannot prove this property because
/// the call may span lines (`self.metadata` then `.read()` on the next).
/// This gate parses the file directly, isolates the function body, and
/// counts `.read()` invocations on the metadata `RwLock`. Any second
/// acquisition — or a `metadata.read()` after the id-capture scope drops
/// — fails the gate. §10.21 CV-5 forbids the publish closure from
/// reacquiring the read guard.
#[test]
fn test_run_write_commit_envelope_holds_exactly_one_metadata_read() {
    let project_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = project_root.join("src/storage/paged_engine.rs");
    let body = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));

    let function_body = extract_function_body(&body, "run_write_commit_envelope")
        .expect("run_write_commit_envelope locatable");

    // Strip line comments so doc references like
    // "the single `self.metadata.read()` call" do not count.
    let code_only = strip_line_comments(&function_body);

    // Count `.read()` calls following `self.metadata` (possibly across a
    // line break). The single-line form would be `self.metadata.read()`;
    // the multiline form is `self.metadata\n    .read()`.
    let read_count = count_metadata_read_calls(&code_only);
    assert_eq!(
        read_count, 1,
        "run_write_commit_envelope must hold exactly one self.metadata.read() acquisition (§10.17 step 3, US-006 AC #5); found {read_count}",
    );

    // Forbid `metadata.write()` inside `run_write_commit_envelope` outright —
    // DDL escalation is owned by `run_ddl`.
    let write_count = count_metadata_write_calls(&code_only);
    assert_eq!(
        write_count, 0,
        "run_write_commit_envelope must not acquire self.metadata.write() ({write_count} found); only run_ddl may take metadata.write",
    );
}

/// Find the top-level `fn run_write_commit_envelope<F, R>(&self, ...)` body and
/// return everything between the opening brace of the function and the
/// matching close. Returns `None` if the function is not present.
fn extract_function_body(source: &str, function_name: &str) -> Option<String> {
    // Match the function signature start. The signature spans multiple
    // lines so we search for the first `fn run_write_commit_envelope` token.
    let sig_start = source.find(&format!("fn {function_name}"))?;
    // Find the opening `{` after the signature. Skip the where-clause.
    let mut depth: i32 = 0;
    let mut started = false;
    let mut body_start = 0usize;
    let mut body_end = 0usize;
    for (idx, ch) in source[sig_start..].char_indices() {
        let abs = sig_start + idx;
        match ch {
            '{' => {
                depth += 1;
                if !started {
                    started = true;
                    body_start = abs + 1;
                }
            }
            '}' => {
                depth -= 1;
                if started && depth == 0 {
                    body_end = abs;
                    break;
                }
            }
            _ => {}
        }
    }
    if !started || body_end <= body_start {
        return None;
    }
    Some(source[body_start..body_end].to_owned())
}

// ---------------------------------------------------------------------------
// US-030 — bootstrap admits the allocated namespace id directly.
// ---------------------------------------------------------------------------

/// US-030 / Phase 6 US-003 — a first write to a missing namespace carries the
/// allocated durable `ns_id` out of bootstrap, then proceeds without making the
/// namespace registry ordinary CRUD's serialization authority. The test holds
/// the writer at body entry, proves the namespace id is visible, and verifies
/// that a separate test admission can take the lane while CRUD is paused.
#[test]
fn test_bootstrap_write_admits_allocated_ns_id_without_name_reresolve() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("us030.mqlite");
    let client = Client::open(&path).unwrap();
    let gen_before = client.__published_catalog_gen();

    let mut hook = client.__install_write_body_entry_hook(NS_A);
    let writer_client = client.clone();
    let writer = thread::spawn(move || -> Result<(), Error> {
        writer_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .insert_one(&doc! { "_id": 30i32, "v": "bootstrap" })
            .map(|_| ())
    });

    hook.wait_until_entered()
        .expect("bootstrap writer reached body-entry hook");
    let gen_after_bootstrap = client.__published_catalog_gen();
    assert!(
        gen_after_bootstrap > gen_before,
        "bootstrap namespace creation must publish a new catalog generation",
    );
    let ns_id = client
        .__us036_namespace_id(NS_A)
        .expect("engine not poisoned after bootstrap")
        .expect("bootstrap namespace id must be visible");

    let lane_ticket = client
        .__us036_admit_writer(ns_id, 0)
        .expect("ordinary CRUD must not occupy the namespace DDL admission lane");
    drop(lane_ticket);

    hook.release()
        .expect("writer was waiting on release channel");
    writer
        .join()
        .expect("writer thread joined")
        .expect("bootstrap insert succeeds");

    let project_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let engine_path = project_root.join("src/storage/paged_engine.rs");
    let doc_ops_path = project_root.join("src/storage/paged_engine/doc_ops.rs");
    let engine_source = std::fs::read_to_string(&engine_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", engine_path.display()));
    let doc_ops_source = std::fs::read_to_string(&doc_ops_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", doc_ops_path.display()));
    let combined = format!("{engine_source}\n{doc_ops_source}");
    assert!(
        !combined.contains("id_for_name(ns)"),
        "US-030 forbids recovering the bootstrap id through id_for_name(ns)",
    );
    assert!(
        engine_source.contains("let ns_id = self.bootstrap_namespace(ns)?;"),
        "run_write must keep the id returned by bootstrap_namespace",
    );
    assert!(
        !engine_source.contains("ns_writers.admit"),
        "ordinary CRUD must not admit through NsWriterRegistry in Phase 6",
    );
    let create_namespace_body = extract_function_body(&engine_source, "create_namespace")
        .expect("create_namespace function body must be locatable");
    assert!(
        !create_namespace_body.contains("close_and_drain"),
        "standalone create_namespace must not drain an existing namespace lane",
    );
}

/// US-030 — standalone namespace creation is not idempotent. A duplicate
/// create must fail under `metadata.write()` without publishing a fresh
/// catalog generation or draining a namespace writer lane.
#[test]
fn test_standalone_create_namespace_rejects_existing_name_without_drain() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("us030_duplicate_create.mqlite");
    let client = Client::open(&path).unwrap();

    client.database(DB).create_collection(COLL_A).unwrap();
    let gen_after_create = client.__published_catalog_gen();
    let err = client
        .database(DB)
        .create_collection(COLL_A)
        .expect_err("duplicate standalone create_namespace must fail");
    assert!(matches!(err, Error::DuplicateKey { .. }));
    assert_eq!(
        client.__published_catalog_gen(),
        gen_after_create,
        "duplicate create must not publish a new catalog generation",
    );

    let project_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let engine_path = project_root.join("src/storage/paged_engine.rs");
    let engine_source = std::fs::read_to_string(&engine_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", engine_path.display()));
    let create_namespace_body = extract_function_body(&engine_source, "create_namespace")
        .expect("create_namespace function body must be locatable");
    assert!(
        !create_namespace_body.contains("return Ok(())"),
        "standalone create_namespace must fail instead of no-oping on an existing name",
    );
    assert!(
        !create_namespace_body.contains("close_and_drain"),
        "standalone create_namespace must not drain an existing namespace lane",
    );
}

/// US-030 / Phase 6 US-003 — a closed DDL admission lane does not block
/// ordinary CRUD bootstrap. The namespace remains visible and the row is
/// inserted through the logical write path.
#[test]
fn test_bootstrap_closed_admission_lane_does_not_block_crud_insert() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("us030_admission_busy.mqlite");
    let client = Client::open(&path).unwrap();

    let ticket = client
        .__us036_admit_writer(FIRST_NAMESPACE_ID, 0)
        .expect("seed first namespace-id lane");
    drop(ticket);
    client
        .__us036_close_and_drain(FIRST_NAMESPACE_ID, 0)
        .expect("close idle first namespace-id lane");

    let coll = client.database(DB).collection::<Document>(COLL_A);
    coll.insert_one(&doc! { "_id": 31i32, "v": "no-partial" })
        .expect("closed DDL admission lane must not reject ordinary CRUD bootstrap");
    assert_eq!(
        client
            .__us036_namespace_id(NS_A)
            .expect("engine not poisoned after admission failure"),
        Some(FIRST_NAMESPACE_ID),
        "bootstrap publish must leave the namespace visible",
    );
    assert_eq!(
        coll.count_documents(doc! {}).unwrap(),
        1,
        "ordinary CRUD bootstrap must insert the row without registry admission",
    );
}

fn strip_line_comments(source: &str) -> String {
    let mut out = String::with_capacity(source.len());
    for line in source.lines() {
        let stripped = line.find("//").map(|idx| &line[..idx]).unwrap_or(line);
        out.push_str(stripped);
        out.push('\n');
    }
    out
}

fn count_metadata_read_calls(code: &str) -> usize {
    count_metadata_method_calls(code, "read")
}

fn count_metadata_write_calls(code: &str) -> usize {
    count_metadata_method_calls(code, "write")
}

/// US-004 — structural page writes route through the dedicated batch owner,
/// not ordinary CRUD rollback helpers.
#[test]
fn test_us004_structural_callers_use_page_batch_owner() {
    let project_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let owner_path = project_root.join("src/storage/structural_page_batch.rs");
    let engine_path = project_root.join("src/storage/paged_engine.rs");
    let index_build_path = project_root.join("src/storage/paged_engine/index_build.rs");
    let index_maint_path = project_root.join("src/storage/paged_engine/index_maint.rs");
    let snapshot_path = project_root.join("src/storage/paged_engine/snapshot_ops.rs");

    let owner = std::fs::read_to_string(&owner_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", owner_path.display()));
    assert!(
        owner.contains("pub(crate) struct StructuralPageBatch")
            && owner.contains("pub(crate) struct AllocatorLifetimeBatch")
            && owner.contains("lifetime: AllocatorLifetimeBatch")
            && owner.contains("pub(crate) fn commit_lsn_fenced(")
            && owner.contains("pub(crate) fn abort(")
            && owner.contains("self.lifetime.abort(handle)?"),
        "US-004/US-006 require structural and allocator-lifetime owners with explicit durable commit and abort semantics",
    );

    let engine = std::fs::read_to_string(&engine_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", engine_path.display()));
    let index_build = std::fs::read_to_string(&index_build_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", index_build_path.display()));
    let index_maint = std::fs::read_to_string(&index_maint_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", index_maint_path.display()));
    let snapshot = std::fs::read_to_string(&snapshot_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", snapshot_path.display()));

    for (label, source, function, required) in [
        (
            "bootstrap_namespace",
            engine.as_str(),
            "bootstrap_namespace",
            &["sync_catalog_root_structural", "new_structural_store"][..],
        ),
        (
            "create_namespace",
            engine.as_str(),
            "create_namespace",
            &["sync_catalog_root_structural", "new_structural_store"],
        ),
        (
            "drop_namespace",
            engine.as_str(),
            "drop_namespace",
            &[
                "StructuralPageBatch::new",
                "free_tree_pages_exclusive(&mut batch",
                "sync_catalog_root_structural",
                "commit_catalog_batch_to_log",
                "batch.abort",
                "PostDurableDdlPublishFailure",
            ],
        ),
        (
            "run_namespace_create_ddl",
            engine.as_str(),
            "run_namespace_create_ddl",
            &[
                "StructuralPageBatch::new",
                "commit_catalog_batch_to_log",
                "batch.abort",
                "PostDurableDdlPublishFailure",
            ],
        ),
        (
            "free_tree_pages_exclusive",
            engine.as_str(),
            "free_tree_pages_exclusive",
            &["new_structural_store", "sort_by_key", "pin_for_write_sized"],
        ),
        (
            "create_index_reserve",
            index_build.as_str(),
            "create_index_reserve",
            &[
                "StructuralPageBatch::new",
                "sync_catalog_root_structural",
                "commit_catalog_batch_to_log",
                "batch.abort",
            ],
        ),
        (
            "create_index_build_inner",
            index_build.as_str(),
            "create_index_build_inner",
            &[
                "StructuralPageBatch::new",
                "new_structural_store",
                "sync_catalog_root_structural",
                "commit_catalog_batch_to_log",
                "batch.abort",
            ],
        ),
        (
            "create_index_commit",
            index_build.as_str(),
            "create_index_commit",
            &[
                "StructuralPageBatch::new",
                "sync_catalog_root_structural",
                "commit_catalog_batch_to_log",
                "batch.abort",
                "PostDurableDdlPublishFailure",
            ],
        ),
        (
            "create_index_cleanup",
            index_build.as_str(),
            "create_index_cleanup",
            &[
                "StructuralPageBatch::new",
                "free_index_pages_exclusive(&mut batch",
                "sync_catalog_root_structural",
                "commit_catalog_batch_to_log",
                "batch.abort",
                "PostDurableDdlPublishFailure",
            ],
        ),
        (
            "free_index_pages_exclusive",
            index_build.as_str(),
            "free_index_pages_exclusive",
            &["new_structural_store", "sort_by_key", "pin_for_write_sized"],
        ),
        (
            "drop_index",
            index_maint.as_str(),
            "drop_index",
            &[
                "StructuralPageBatch::new",
                "free_index_pages_exclusive(&mut batch",
                "sync_catalog_root_structural",
                "commit_catalog_batch_to_log",
                "batch.abort",
                "PostDurableDdlPublishFailure",
            ],
        ),
        (
            "materialize_ready_secondary_deltas_for_checkpoint",
            index_maint.as_str(),
            "materialize_ready_secondary_deltas_for_checkpoint",
            &["new_structural_store"],
        ),
        (
            "materialize_primary_deltas_for_checkpoint",
            index_maint.as_str(),
            "materialize_primary_deltas_for_checkpoint",
            &["new_structural_store"],
        ),
        (
            "checkpoint_after_reconcile_plan",
            snapshot.as_str(),
            "checkpoint_after_reconcile_plan",
            &[
                "StructuralPageBatch::new",
                "materialize_primary_deltas_for_checkpoint",
                "materialize_ready_secondary_deltas_for_checkpoint",
                "batch.commit_lsn_fenced",
                "batch.abort",
            ],
        ),
    ] {
        assert_structural_owner_body(label, source, function, required);
    }
}

/// US-005 — header/catalog-root mutation has its own final owner. The old
/// retired helper names are deleted rather than left as hidden compatibility
/// surfaces.
#[test]
fn test_us005_header_catalog_root_owner_replaces_retired_helpers() {
    let project_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let owner_path = project_root.join("src/storage/structural_page_batch.rs");
    let production_paths: Vec<PathBuf> = vec![
        owner_path.clone(),
        project_root.join("src/storage/paged_engine.rs"),
        project_root.join("src/storage/paged_engine/index_build.rs"),
        project_root.join("src/storage/paged_engine/index_maint.rs"),
        project_root.join("src/storage/paged_engine/publish.rs"),
        project_root.join("src/storage/paged_engine/snapshot_ops.rs"),
    ];

    let owner = std::fs::read_to_string(&owner_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", owner_path.display()));
    assert!(
        owner.contains("pub(crate) struct HeaderCatalogRootBatch")
            && owner.contains("header: HeaderCatalogRootBatch")
            && owner.contains("impl HeaderCatalogRootBatch")
            && owner.contains("pub(crate) fn update_header")
            && owner.contains("fn abort")
            && owner.contains("self.header.abort(handle)"),
        "US-005 requires an explicit header/catalog-root owner composed by \
         StructuralPageBatch",
    );

    for path in production_paths {
        let source = std::fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
        for token in [
            "TxnOverlay",
            "new_txn_store",
            "sync_catalog_root_overlay",
            "txn_update_header",
        ] {
            assert!(
                !source.contains(token),
                "{} must not contain deleted US-005 helper `{token}`",
                path.display(),
            );
        }
    }
}

/// US-004 — a post-durable DDL poison does not make reopen depend on legacy
/// page-frame replay. The durable structural catalog/index state is recovered
/// into a fresh unpoisoned engine.
#[test]
fn test_us004_post_durable_ddl_poison_reopen_recovers_structural_commit() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("us004_post_durable_ddl_poison.mqlite");

    {
        let client = Client::open(&path).unwrap();
        let db = client.database(DB);
        db.create_collection(COLL_A).unwrap();
        let coll = db.collection::<Document>(COLL_A);
        coll.insert_one(&doc! { "_id": 4004i32, "tag": "structural" })
            .unwrap();
        coll.create_index(tag_index_model())
            .expect("structural create_index succeeds before poison");

        let reason = EngineFatalReason::PostDurableDdlPublishFailure;
        client.__us036_poison_engine(reason.clone());
        assert_eq!(client.__us036_poisoned_reason(), Some(reason.clone()));
        let err = coll
            .list_indexes()
            .expect_err("poisoned engine must fail closed before reopen");
        assert!(matches!(err, Error::EngineFatal { reason: got } if got == reason));
    }

    let reopened = Client::open(&path).expect("reopen after post-durable DDL poison");
    assert_eq!(
        reopened.__us036_poisoned_reason(),
        None,
        "reopen must construct an unpoisoned engine",
    );
    let db = reopened.database(DB);
    let names = db
        .list_collection_names()
        .expect("list collections after reopen");
    assert!(
        names.iter().any(|name| name == COLL_A),
        "structural namespace commit must survive reopen; names={names:?}",
    );
    let indexes = db
        .collection::<Document>(COLL_A)
        .list_indexes()
        .expect("list indexes after reopen");
    assert!(
        indexes.iter().any(|index| index.name == TAG_INDEX),
        "structural index commit must survive reopen; indexes={indexes:?}",
    );
}

fn assert_structural_owner_body(label: &str, source: &str, function_name: &str, required: &[&str]) {
    let body = extract_function_body(source, function_name)
        .unwrap_or_else(|| panic!("{label} function body must be locatable"));
    let code = strip_line_comments(&body);

    for token in required {
        assert!(
            code.contains(token),
            "{label} must contain required structural-owner token `{token}`",
        );
    }

    for token in [
        "TxnOverlay::new",
        "new_txn_store",
        "sync_catalog_root_overlay",
        "rollback_overlay",
    ] {
        assert!(
            !code.contains(token),
            "{label} must not depend on ordinary CRUD helper `{token}`",
        );
    }
}

fn assert_free_helper_latches_before_free(free_body: &str, label: &str) {
    let collect = free_body
        .find("collect_pages_by_size")
        .unwrap_or_else(|| panic!("{label} must collect the complete tree page set"));
    let sort = free_body
        .find("sort_by_key")
        .unwrap_or_else(|| panic!("{label} must sort pages by page_id"));
    let latch = free_body
        .find("pin_for_write_sized")
        .unwrap_or_else(|| panic!("{label} must acquire explicit-size exclusive latches"));
    let free_internal = free_body
        .find("free_internal")
        .unwrap_or_else(|| panic!("{label} must free internal pages while latches are held"));
    let free_leaf = free_body
        .find("free_leaf")
        .unwrap_or_else(|| panic!("{label} must free leaf pages while latches are held"));
    let drop_latches = free_body
        .find("drop(latches)")
        .unwrap_or_else(|| panic!("{label} must drop latches after freeing pages"));

    assert!(
        collect < sort
            && sort < latch
            && latch < free_internal
            && latch < free_leaf
            && free_internal < drop_latches
            && free_leaf < drop_latches,
        "{label} must collect, sort, latch in ascending page_id order, then free before dropping latches",
    );
}

/// Count acquisitions of the metadata `RwLock` written either as
/// `self.metadata.read()` (single-line) or split across lines as
/// `self\n    .metadata\n    .read()` — the form §10.17 step 3
/// single-line greps cannot detect. Field-only references such as
/// `self.metadata_state` or `&self.metadata_state` are rejected.
fn count_metadata_method_calls(code: &str, method: &'static str) -> usize {
    let needle = ".metadata";
    let suffix = format!(".{method}()");
    let mut hits = 0usize;
    let bytes = code.as_bytes();
    let mut cursor = 0usize;
    while let Some(rel) = code[cursor..].find(needle) {
        let pos = cursor + rel;
        let after = pos + needle.len();
        // Reject `.metadata_state` / `.metadata_foo` — the field
        // name must end at the search point.
        let next_byte = bytes.get(after).copied().unwrap_or(b' ');
        if next_byte.is_ascii_alphanumeric() || next_byte == b'_' {
            cursor = after;
            continue;
        }
        // Skip whitespace (including newlines) between `.metadata`
        // and the `.<method>()` token. The receiver before `.metadata`
        // is overwhelmingly `self` here; we don't enforce that because
        // `run_write_commit_envelope` body has only one valid receiver.
        let rest = &code[after..];
        let ws_len = rest
            .char_indices()
            .find(|(_, c)| !c.is_whitespace())
            .map(|(i, _)| i)
            .unwrap_or(rest.len());
        let tail = &rest[ws_len..];
        if tail.starts_with(&suffix) {
            hits += 1;
        }
        cursor = after;
    }
    hits
}

// ---------------------------------------------------------------------------
// AC #4 captured-identity gate tolerates unrelated DDL generation advances.
// ---------------------------------------------------------------------------

/// AC #4 / §10.17.1 — a global catalog generation advance is only a dirty
/// signal. If the writer's target namespace/index identity still matches,
/// unrelated DDL on another namespace must not force a pre-durable rollback.
#[test]
fn test_unrelated_catalog_generation_advance_does_not_conflict() {
    let (_dir, client) = open_with_collection_a();
    let gen_before = client.__published_catalog_gen();

    // Pause the next write body on COLL_A. The hook fires AFTER the S1
    // metadata-read scope (catalog_gen captured, ticket admitted) and
    // BEFORE the closure body runs — exactly the window that lets a
    // concurrent DDL on a DIFFERENT namespace bump the published generation.
    let mut hook = client.__install_write_body_entry_hook(NS_A);

    let writer_client = client.clone();
    let writer = thread::spawn(move || -> Result<(), Error> {
        writer_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .insert_one(&doc! { "_id": 1i32, "v": "after-ddl" })
            .map(|_| ())
    });

    // Wait until the writer reaches the body-entry hook (i.e., it has
    // captured catalog_gen and admitted its ticket).
    hook.wait_until_entered()
        .expect("writer reached body-entry hook");

    let ddl_done = Arc::new(AtomicBool::new(false));
    let ddl_done_flag = Arc::clone(&ddl_done);
    let ddl_client = client.clone();
    let ddl = thread::spawn(move || -> Result<(), Error> {
        let res = ddl_client.database(DB).create_collection(COLL_B);
        ddl_done_flag.store(true, Ordering::Release);
        res
    });
    assert!(
        !flag_set_within(&ddl_done, SHORT_SLEEP),
        "Phase 6 DDL must wait behind the CRUD metadata guard while the writer is paused",
    );
    assert_eq!(
        client.__published_catalog_gen(),
        gen_before,
        "blocked DDL must not advance catalog generation before the writer releases",
    );

    hook.release()
        .expect("writer was waiting on release channel");

    let res = writer.join().expect("writer thread joined");
    res.expect("writer must commit before unrelated DDL takes metadata.write");

    wait_for_flag(&ddl_done, "DDL after writer release");
    ddl.join()
        .expect("DDL thread joined")
        .expect("DDL on a different namespace must proceed after the writer releases");

    let gen_after_ddl = client.__published_catalog_gen();
    assert!(
        gen_after_ddl > gen_before,
        "DDL must advance published catalog_generation: before={gen_before}, after={gen_after_ddl}",
    );

    // The paused writer committed, and later CRUD on COLL_A still succeeds.
    client
        .database(DB)
        .collection::<Document>(COLL_A)
        .insert_one(&doc! { "_id": 2i32, "v": "post-conflict" })
        .expect("post-conflict CRUD on the same namespace must succeed");
    let count = client
        .database(DB)
        .collection::<Document>(COLL_A)
        .count_documents(doc! {})
        .unwrap();
    assert_eq!(
        count, 3,
        "count must reflect the seed + paused writer + post-DDL insert",
    );
}

// ---------------------------------------------------------------------------
// AC #1 / AC #5 — ordinary CRUD must NOT advance the DDL identity counter.
// ---------------------------------------------------------------------------

/// AC #5 / §10.21 CV-5 — ordinary CRUD's publish closure inherits the
/// prior published `catalog_generation` and never advances the DDL
/// identity counter. Mirrors the contract verified at the source level
/// for the publish closure (`reserved_catalog_gen=None`).
#[test]
fn test_crud_publish_does_not_advance_published_catalog_generation() {
    let (_dir, client) = open_with_collection_a();
    let gen_after_setup = client.__published_catalog_gen();

    let coll = client.database(DB).collection::<Document>(COLL_A);
    for id in 1..=5 {
        coll.insert_one(&doc! { "_id": id, "v": format!("crud-{id}") })
            .unwrap();
    }
    assert_eq!(
        client.__published_catalog_gen(),
        gen_after_setup,
        "ordinary CRUD inserts must not bump PublishedEpoch.catalog_generation",
    );

    coll.update_one(doc! { "_id": 1i32 }, doc! { "$set": { "v": "updated" } })
        .run()
        .unwrap();
    assert_eq!(
        client.__published_catalog_gen(),
        gen_after_setup,
        "CRUD updates must not bump PublishedEpoch.catalog_generation",
    );

    coll.delete_one(doc! { "_id": 2i32 }).unwrap();
    assert_eq!(
        client.__published_catalog_gen(),
        gen_after_setup,
        "CRUD deletes must not bump PublishedEpoch.catalog_generation",
    );

    // DDL, on the other hand, must advance the counter via the
    // `next_catalog_gen` reservation.
    client.database(DB).create_collection(COLL_B).unwrap();
    let gen_after_ddl = client.__published_catalog_gen();
    assert!(
        gen_after_ddl > gen_after_setup,
        "DDL must advance PublishedEpoch.catalog_generation: setup={gen_after_setup}, ddl={gen_after_ddl}",
    );
}

// ---------------------------------------------------------------------------
// AC #6 — DDL on a different namespace does not deadlock with an
// in-flight CRUD writer paused inside its body.
// ---------------------------------------------------------------------------

/// AC #6 / Phase 6 US-003 — a CRUD writer paused at body entry holds the
/// metadata read guard. A concurrent DDL on a different namespace must wait
/// for that guard, then publish after the writer releases; this prevents a
/// metadata/registry deadlock without using the registry as CRUD authority.
#[test]
fn test_ddl_during_writer_body_does_not_deadlock_or_post_durable_write_conflict() {
    let (_dir, client) = open_with_collection_a();

    let mut hook = client.__install_write_body_entry_hook(NS_A);
    let writer_client = client.clone();
    let writer_done = Arc::new(AtomicBool::new(false));
    let writer_done_flag = Arc::clone(&writer_done);
    let writer = thread::spawn(move || -> Result<(), Error> {
        let res = writer_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .insert_one(&doc! { "_id": 9i32, "v": "concurrent-ddl" })
            .map(|_| ());
        writer_done_flag.store(true, Ordering::Release);
        res
    });

    hook.wait_until_entered()
        .expect("writer reached body-entry hook");

    assert!(
        !writer_done.load(Ordering::Acquire),
        "writer should be paused at the body-entry hook",
    );
    let ddl_done = Arc::new(AtomicBool::new(false));
    let ddl_done_flag = Arc::clone(&ddl_done);
    let ddl_client = client.clone();
    let ddl = thread::spawn(move || -> Result<(), Error> {
        let res = ddl_client.database(DB).create_collection(COLL_B);
        ddl_done_flag.store(true, Ordering::Release);
        res
    });
    assert!(
        !flag_set_within(&ddl_done, SHORT_SLEEP),
        "DDL must wait behind the paused writer's metadata guard",
    );

    let ddl_release_start = Instant::now();
    hook.release()
        .expect("writer was waiting on release channel");

    let join_deadline = Instant::now() + SETTLE_DEADLINE;
    while !writer_done.load(Ordering::Acquire) {
        assert!(
            Instant::now() < join_deadline,
            "writer thread did not finish within {SETTLE_DEADLINE:?}",
        );
        thread::sleep(Duration::from_millis(5));
    }
    let res = writer.join().expect("writer thread joined");
    res.expect("writer must commit before waiting DDL publishes");

    wait_for_flag(&ddl_done, "DDL after writer release");
    ddl.join()
        .expect("DDL thread joined")
        .expect("DDL on a different namespace must proceed without deadlock");
    let ddl_elapsed = ddl_release_start.elapsed();
    assert!(
        ddl_elapsed < SETTLE_DEADLINE,
        "DDL took {ddl_elapsed:?} after writer release, exceeding {SETTLE_DEADLINE:?}",
    );
}

// ---------------------------------------------------------------------------
// AC #7 — same-namespace DDL/CRUD interleaving terminates without a
// metadata/registry cycle.
// ---------------------------------------------------------------------------

/// AC #7 / §10.27 — when a DDL on the SAME namespace races an
/// admitted CRUD writer, either the DDL waits for the writer to
/// drain or the writer fails with `CatalogGenerationChanged`. Both
/// orderings terminate without a deadlock between
/// `NsWriterRegistry` and the `metadata` `RwLock`.
#[test]
fn test_same_namespace_ddl_writer_interleaving_terminates() {
    // Repeat the race a handful of times to flush both interleavings:
    // the DDL beating the writer's gate, and the writer winning the
    // S3.5 revalidation.
    for iter in 0..4 {
        let (_dir, client) = open_with_collection_a();
        let writer_client = client.clone();
        let writer_id = 1000 + iter;
        let writer = thread::spawn(move || -> Result<(), Error> {
            writer_client
                .database(DB)
                .collection::<Document>(COLL_A)
                .insert_one(&doc! { "_id": writer_id, "v": "racing-ddl" })
                .map(|_| ())
        });

        // Race a same-namespace DDL: drop the namespace.
        // `drop_collection` runs through `run_ddl`, takes
        // `metadata.write()`, and (per §10.1.3 / §10.8.3) calls
        // `close_and_drain` to drain admitted writers before the
        // catalog mutation. The drain plus the writer's S3.5 gate
        // give two valid terminations: writer succeeds + DDL drops,
        // or writer fails with CatalogGenerationChanged + DDL drops.
        let drop_res = client.database(DB).drop_collection(COLL_A);

        let writer_res = writer.join().expect("writer thread joined");

        // The DDL itself must complete; the lane drain timeout would
        // surface as `WriterBusy`, but with default `busy_timeout`
        // values the drain has ample headroom.
        drop_res.expect("DDL must terminate within the busy timeout");

        match writer_res {
            Ok(()) => {}
            Err(Error::WriteConflict {
                reason: WriteConflictReason::CatalogGenerationChanged,
            }) => {}
            Err(Error::WriterBusy) => {}
            // The CRUD body may also observe the namespace as
            // dropped between admit and the body — that path returns
            // `Ok` with zero affected on update/delete, but for
            // `insert_one` the catalog lookup at body time can
            // surface a namespace-not-found error. Accept it.
            Err(other) => {
                let msg = format!("{other:?}");
                assert!(
                    msg.contains("Namespace")
                        || msg.contains("namespace")
                        || msg.contains("collection"),
                    "writer must succeed or return a recoverable conflict, got {other:?}",
                );
            }
        }
    }
}

// ---------------------------------------------------------------------------
// AC #2 / §10.27 — sanity check: the writer ticket admitted under the
// metadata-read scope serializes a same-namespace `close_and_drain`.
// This corroborates that admit BEFORE the metadata-read scope (or
// after the scope drops) is unnecessary because the ticket already
// pins the writer in the lane until body+publish complete.
// ---------------------------------------------------------------------------

/// AC #2 — the test-only US-036 admit hook, exercised here against
/// the same registry the production CRUD path uses, demonstrates that
/// `close_and_drain` blocks until a held ticket drops. This is the
/// drain-ordering invariant the §10.17 metadata-read id-capture scope
/// relies on: by the time DDL takes `metadata.write()`, all admitted
/// writers will eventually surface and drop their tickets, so
/// `close_and_drain` cannot starve.
#[test]
fn test_admitted_writer_ticket_blocks_close_and_drain() {
    let (_dir, client) = open_with_collection_a();
    let ns_id = client
        .__us036_namespace_id(NS_A)
        .expect("engine not poisoned at setup")
        .expect("durable ns_id resolved from published catalog");

    let ticket = client
        .__us036_admit_writer(ns_id, 5_000)
        .expect("admit writer ticket");

    let drain_client = client.clone();
    let drain = thread::spawn(move || drain_client.__us036_close_and_drain(ns_id, 5_000));

    // Give the drain thread time to enter its wait loop.
    thread::sleep(SHORT_SLEEP);
    assert!(
        !drain.is_finished(),
        "close_and_drain must block while a ticket is held",
    );

    drop(ticket);

    let res = drain
        .join()
        .expect("drain thread joined")
        .expect("close_and_drain completes after the ticket drops");
    let _ = res;
}

// ---------------------------------------------------------------------------
// US-013 — create-index Building publish + reopened scan window.
// ---------------------------------------------------------------------------

/// US-013 — `create_index_reserve` closes the namespace writer registry
/// and waits for already-admitted writers before it publishes the
/// Building catalog entry. Once that writer drops its ticket, the DDL
/// completes and reopens admissions before the build scan.
#[test]
fn test_create_index_barrier_drains_writers() {
    let (_dir, client) = open_with_collection_a();
    let mut writer_hook = client.__install_write_body_entry_hook(NS_A);

    let writer_client = client.clone();
    let writer = thread::spawn(move || -> Result<(), Error> {
        writer_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .insert_one(&doc! { "_id": 1301i32, "tag": "held" })
            .map(|_| ())
    });

    writer_hook
        .wait_until_entered()
        .expect("writer reached body-entry hook");

    let ddl_done = Arc::new(AtomicBool::new(false));
    let ddl_done_flag = Arc::clone(&ddl_done);
    let ddl_client = client.clone();
    let ddl = thread::spawn(move || -> Result<(), Error> {
        let res = ddl_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .create_index(tag_index_model())
            .map(|_| ());
        ddl_done_flag.store(true, Ordering::Release);
        res
    });

    thread::sleep(SHORT_SLEEP);
    assert!(
        !ddl_done.load(Ordering::Acquire),
        "create_index_reserve must drain the already-admitted writer before publishing Building",
    );

    writer_hook
        .release()
        .expect("writer was waiting on release channel");
    writer
        .join()
        .expect("writer thread joined")
        .expect("held writer succeeds before create_index publishes");

    wait_for_flag(&ddl_done, "create_index after writer release");
    ddl.join()
        .expect("create_index thread joined")
        .expect("create_index succeeds after drain");
}

/// US-013 — an admitted writer does not need to reacquire
/// `metadata.read()` after a same-namespace create-index DDL has acquired
/// `metadata.write()` and begun draining the writer registry. The writer
/// can finish, the DDL drain can observe the ticket drop, and both threads
/// terminate without a metadata/registry cycle.
#[test]
fn test_ddl_writer_pre_admit_id_capture_interleaving_does_not_deadlock() {
    let (_dir, client) = open_with_collection_a();
    let mut writer_hook = client.__install_write_body_entry_hook(NS_A);

    let writer_client = client.clone();
    let writer_done = Arc::new(AtomicBool::new(false));
    let writer_done_flag = Arc::clone(&writer_done);
    let writer = thread::spawn(move || -> Result<(), Error> {
        let res = writer_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .insert_one(&doc! { "_id": 1302i32, "tag": "deadlock-proof" })
            .map(|_| ());
        writer_done_flag.store(true, Ordering::Release);
        res
    });

    writer_hook
        .wait_until_entered()
        .expect("writer reached body-entry hook");

    let ddl_done = Arc::new(AtomicBool::new(false));
    let ddl_done_flag = Arc::clone(&ddl_done);
    let ddl_client = client.clone();
    let ddl = thread::spawn(move || -> Result<(), Error> {
        let res = ddl_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .create_index(tag_index_model())
            .map(|_| ());
        ddl_done_flag.store(true, Ordering::Release);
        res
    });

    thread::sleep(SHORT_SLEEP);
    assert!(
        !ddl_done.load(Ordering::Acquire),
        "DDL should be waiting for the admitted writer, not racing ahead",
    );

    writer_hook
        .release()
        .expect("writer was waiting on release channel");

    wait_for_flag(&writer_done, "writer after DDL begins drain");
    wait_for_flag(&ddl_done, "create_index after writer drain");

    writer
        .join()
        .expect("writer thread joined")
        .expect("writer succeeds without metadata/registry deadlock");
    ddl.join()
        .expect("create_index thread joined")
        .expect("create_index succeeds after draining writer");
}

/// US-013 — after the Building publish, `create_index_build` runs outside
/// `metadata.write()` and outside a closed writer-registry gate. A new
/// same-namespace writer can enter while the build scan is paused and
/// dual-write into the still-Building index.
#[test]
fn test_create_index_dual_write_building_window_contains_post_reopen_writes() {
    let (_dir, client) = open_with_collection_a();
    let gen_before = client.__published_catalog_gen();
    let mut build_hook = client.__install_create_index_build_hook(NS_A, TAG_INDEX);

    let build_client = client.clone();
    let build = thread::spawn(move || -> Result<(), Error> {
        build_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .create_index(tag_index_model())
            .map(|_| ())
    });

    build_hook
        .wait_until_entered()
        .expect("create_index reached the build scan hook");
    assert!(
        client.__published_catalog_gen() > gen_before,
        "Building publish must advance the published catalog generation before the scan",
    );

    let during_doc = doc! { "_id": 1303i32, "tag": "during-build" };
    let mut writer_hook = client.__install_write_body_entry_hook(NS_A);
    let writer_client = client.clone();
    let writer_doc = during_doc.clone();
    let writer = thread::spawn(move || -> Result<(), Error> {
        writer_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .insert_one(&writer_doc)
            .map(|_| ())
    });

    writer_hook
        .wait_until_entered_timeout(SETTLE_DEADLINE)
        .expect("post-Building writer must admit while build scan is paused");
    writer_hook
        .release()
        .expect("writer was waiting on release channel");
    writer
        .join()
        .expect("writer thread joined")
        .expect("post-Building writer succeeds");

    let states = client
        .__us009_secondary_chain_states(NS_A, TAG_INDEX, &during_doc, &Bson::Int32(1303))
        .expect("Building index secondary chain can be inspected");
    assert!(
        states.iter().any(|state| state == "Committed"),
        "post-reopen writer must dual-write a committed entry into the Building index; states={states:?}",
    );

    let collection = client.database(DB).collection::<Document>(COLL_A);
    let cursor = collection
        .find(doc! { "tag": "during-build" })
        .run()
        .expect("query during Building window succeeds");
    let explain = cursor.explain().expect("query explain available");
    let docs = cursor
        .collect::<mqlite::Result<Vec<_>>>()
        .expect("query during Building window collects results");
    assert_eq!(docs, vec![during_doc]);
    assert!(
        explain.full_scan,
        "query planner must not use the still-Building index: {explain:?}",
    );
    assert_eq!(
        explain.index_used, None,
        "query planner must keep the Building index read-invisible",
    );

    build_hook
        .release()
        .expect("build was waiting on release channel");
    build
        .join()
        .expect("create_index thread joined")
        .expect("create_index succeeds after post-reopen dual-write");
}

/// US-013 — a long build scan with admissions reopened can overlap CRUD
/// and a concurrent `drop_index` without wedging the create-index guard.
/// Drop-index's full guarded page-free contract is owned by US-023; this
/// regression only locks the US-013 requirement that the reopened build
/// window does not keep a stale closed writer gate.
#[test]
fn test_long_build_crud_drop_index_stack() {
    let (_dir, client) = open_with_collection_a();
    let mut build_hook = client.__install_create_index_build_hook(NS_A, TAG_INDEX);

    let build_client = client.clone();
    let build_done = Arc::new(AtomicBool::new(false));
    let build_done_flag = Arc::clone(&build_done);
    let build = thread::spawn(move || -> Result<(), Error> {
        let res = build_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .create_index(tag_index_model())
            .map(|_| ());
        build_done_flag.store(true, Ordering::Release);
        res
    });

    build_hook
        .wait_until_entered()
        .expect("create_index reached the build scan hook");

    client
        .database(DB)
        .collection::<Document>(COLL_A)
        .insert_one(&doc! { "_id": 1304i32, "tag": "overlap" })
        .expect("CRUD must admit while create-index build scan is paused");

    client
        .database(DB)
        .collection::<Document>(COLL_A)
        .drop_index(TAG_INDEX)
        .expect("drop_index can remove the Building entry during the paused scan");

    build_hook
        .release()
        .expect("build was waiting on release channel");
    wait_for_flag(&build_done, "create_index after concurrent drop_index");

    let _ = build.join().expect("create_index thread joined");
    let indexes = client
        .database(DB)
        .collection::<Document>(COLL_A)
        .list_indexes()
        .expect("list_indexes after overlap");
    assert!(
        indexes.iter().all(|index| index.name != TAG_INDEX),
        "Building entry must be absent after concurrent drop_index",
    );
    client
        .database(DB)
        .collection::<Document>(COLL_A)
        .insert_one(&doc! { "_id": 1305i32, "tag": "post-drop" })
        .expect("writer registry must be reopened after overlap");
}

/// US-023 — `drop_index` must close the namespace writer registry and
/// wait for already-admitted writers before it removes the target index.
/// Once that writer drops its ticket, the DDL completes and admissions
/// reopen through the RAII guard.
#[test]
fn test_drop_index_waits_for_writers() {
    let (_dir, client) = open_with_collection_a();
    client
        .database(DB)
        .collection::<Document>(COLL_A)
        .create_index(tag_index_model())
        .expect("create index before drop");

    let mut writer_hook = client.__install_write_body_entry_hook(NS_A);
    let writer_client = client.clone();
    let writer = thread::spawn(move || -> Result<(), Error> {
        writer_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .insert_one(&doc! { "_id": 2301i32, "tag": "held-before-drop" })
            .map(|_| ())
    });

    writer_hook
        .wait_until_entered()
        .expect("writer reached body-entry hook");

    let ddl_done = Arc::new(AtomicBool::new(false));
    let ddl_done_flag = Arc::clone(&ddl_done);
    let ddl_client = client.clone();
    let ddl = thread::spawn(move || -> Result<(), Error> {
        let res = ddl_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .drop_index(TAG_INDEX);
        ddl_done_flag.store(true, Ordering::Release);
        res
    });

    thread::sleep(SHORT_SLEEP);
    assert!(
        !ddl_done.load(Ordering::Acquire),
        "drop_index must drain the already-admitted writer before deleting the index",
    );

    writer_hook
        .release()
        .expect("writer was waiting on release channel");
    writer
        .join()
        .expect("writer thread joined")
        .expect("held writer succeeds before drop_index publishes");

    wait_for_flag(&ddl_done, "drop_index after writer release");
    ddl.join()
        .expect("drop_index thread joined")
        .expect("drop_index succeeds after drain");

    client
        .database(DB)
        .collection::<Document>(COLL_A)
        .insert_one(&doc! { "_id": 2302i32, "tag": "post-drop" })
        .expect("writer registry must reopen after drop_index");
}

/// US-023 / US-013 — a Building publish, a concurrent CRUD publish, and
/// the final Ready publish must advance visible timestamps monotonically.
#[test]
fn test_create_index_commit_ts_monotonic_vs_concurrent_crud() {
    let (_dir, client) = open_with_collection_a();
    let mut build_hook = client.__install_create_index_build_hook(NS_A, TAG_INDEX);

    let build_client = client.clone();
    let build = thread::spawn(move || -> Result<(), Error> {
        build_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .create_index(tag_index_model())
            .map(|_| ())
    });

    build_hook
        .wait_until_entered()
        .expect("create_index reached the build scan hook");
    let after_building_publish = client.__published_visible_ts();

    client
        .database(DB)
        .collection::<Document>(COLL_A)
        .insert_one(&doc! { "_id": 2303i32, "tag": "between-building-and-ready" })
        .expect("CRUD must publish while create-index build scan is paused");
    let after_crud_publish = client.__published_visible_ts();

    build_hook
        .release()
        .expect("build was waiting on release channel");
    build
        .join()
        .expect("create_index thread joined")
        .expect("create_index succeeds after concurrent CRUD");
    let after_ready_publish = client.__published_visible_ts();

    assert!(
        after_building_publish < after_crud_publish,
        "CRUD publish must advance after Building publish: {after_building_publish:?} < {after_crud_publish:?}",
    );
    assert!(
        after_crud_publish < after_ready_publish,
        "Ready publish must advance after concurrent CRUD: {after_crud_publish:?} < {after_ready_publish:?}",
    );
}

/// US-031 — the Ready flip is its own sequencer-published DDL event, not a
/// side effect hidden inside the long build scan.
#[test]
fn test_create_index_commit_flips_building_to_ready_with_new_catalog_generation() {
    let (_dir, client) = open_with_collection_a();
    let mut build_hook = client.__install_create_index_build_hook(NS_A, TAG_INDEX);

    let build_client = client.clone();
    let build = thread::spawn(move || -> Result<(), Error> {
        build_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .create_index(tag_index_model())
            .map(|_| ())
    });

    build_hook
        .wait_until_entered()
        .expect("create_index reached the build scan hook");
    let building_ts = client.__published_visible_ts();
    let building_frontier = client.__published_sequencer_frontier();
    let building_gen = client.__published_catalog_gen();

    build_hook
        .release()
        .expect("build was waiting on release channel");
    build
        .join()
        .expect("create_index thread joined")
        .expect("create_index succeeds");

    let ready_ts = client.__published_visible_ts();
    let ready_frontier = client.__published_sequencer_frontier();
    let ready_gen = client.__published_catalog_gen();
    assert!(
        building_ts < ready_ts,
        "Ready flip must allocate a fresh commit_ts after Building: {building_ts:?} < {ready_ts:?}",
    );
    assert!(
        building_frontier < ready_frontier,
        "Ready flip must advance the sequencer frontier after Building: {building_frontier:?} < {ready_frontier:?}",
    );
    assert!(
        building_gen < ready_gen,
        "Ready flip must publish a fresh catalog generation after Building: {building_gen} < {ready_gen}",
    );

    let indexes = client
        .database(DB)
        .collection::<Document>(COLL_A)
        .list_indexes()
        .expect("list_indexes after Ready flip");
    assert!(
        indexes.iter().any(|index| index.name == TAG_INDEX),
        "Ready index must be visible after create_index_commit",
    );
}

/// US-031 — after admissions reopen during the Building window, concurrent
/// same-namespace writers dual-write to the Building tree. The Ready view
/// must include those writes once `create_index_commit` publishes.
#[test]
fn test_create_index_ready_view_contains_post_reopen_dual_writes() {
    let (_dir, client) = open_with_collection_a();
    let mut build_hook = client.__install_create_index_build_hook(NS_A, TAG_INDEX);

    let build_client = client.clone();
    let build = thread::spawn(move || -> Result<(), Error> {
        build_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .create_index(tag_index_model())
            .map(|_| ())
    });

    build_hook
        .wait_until_entered()
        .expect("create_index reached the build scan hook");
    let during_doc = doc! { "_id": 3101i32, "tag": "ready-dual-write" };
    client
        .database(DB)
        .collection::<Document>(COLL_A)
        .insert_one(&during_doc)
        .expect("post-Building writer succeeds during build scan");

    build_hook
        .release()
        .expect("build was waiting on release channel");
    build
        .join()
        .expect("create_index thread joined")
        .expect("create_index succeeds after dual-write");

    let cursor = client
        .database(DB)
        .collection::<Document>(COLL_A)
        .find(doc! { "tag": "ready-dual-write" })
        .run()
        .expect("Ready-index query starts");
    let explain = cursor.explain().expect("query explain available");
    let docs = cursor
        .collect::<mqlite::Result<Vec<_>>>()
        .expect("Ready-index query collects results");
    assert_eq!(docs, vec![during_doc]);
    assert_eq!(
        explain.index_used.as_deref(),
        Some(TAG_INDEX),
        "Ready query must use the index containing post-reopen dual-writes",
    );
    assert!(
        !explain.full_scan,
        "Ready query should not fall back to COLLSCAN after create_index_commit",
    );
}

/// US-031 — if the Building entry disappears before the Ready flip, commit
/// must fail before publishing a new catalog generation.
#[test]
fn test_create_index_commit_rejects_missing_building_entry_after_drop_index() {
    let (_dir, client) = open_with_collection_a();
    let mut build_hook = client.__install_create_index_build_hook(NS_A, TAG_INDEX);

    let build_client = client.clone();
    let build = thread::spawn(move || -> Result<(), Error> {
        build_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .create_index(tag_index_model())
            .map(|_| ())
    });

    build_hook
        .wait_until_entered()
        .expect("create_index reached the build scan hook");
    client
        .database(DB)
        .collection::<Document>(COLL_A)
        .drop_index(TAG_INDEX)
        .expect("drop_index removes Building before commit");
    let gen_after_drop = client.__published_catalog_gen();

    build_hook
        .release()
        .expect("build was waiting on release channel");
    let err = build
        .join()
        .expect("create_index thread joined")
        .expect_err("Ready flip must fail when Building was removed");
    assert!(
        matches!(
            err,
            Error::WriteConflict {
                reason: WriteConflictReason::CatalogGenerationChanged
            }
        ),
        "missing Building entry must return a deterministic write conflict, got {err:?}",
    );
    assert_eq!(
        client.__published_catalog_gen(),
        gen_after_drop,
        "failed Ready flip must not publish another catalog generation",
    );
}

/// US-031 — if the namespace name is dropped and recreated during the long
/// scan, the Ready flip must compare the original durable `ns_id` before
/// closing/draining writers on the recreated namespace.
#[test]
fn test_create_index_commit_revalidates_ns_id_under_metadata_write() {
    let (_dir, client) = open_with_collection_a();
    let original_ns_id = client
        .__us036_namespace_id(NS_A)
        .expect("engine not poisoned at setup")
        .expect("original namespace id");
    let mut build_hook = client.__install_create_index_build_hook(NS_A, TAG_INDEX);

    let build_client = client.clone();
    let build_done = Arc::new(AtomicBool::new(false));
    let build_done_flag = Arc::clone(&build_done);
    let build = thread::spawn(move || -> Result<(), Error> {
        let res = build_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .create_index(tag_index_model())
            .map(|_| ());
        build_done_flag.store(true, Ordering::Release);
        res
    });

    build_hook
        .wait_until_entered()
        .expect("create_index reached the build scan hook");
    client
        .database(DB)
        .drop_collection(COLL_A)
        .expect("drop original namespace");
    client
        .database(DB)
        .create_collection(COLL_A)
        .expect("recreate namespace with same name");
    let recreated_ns_id = client
        .__us036_namespace_id(NS_A)
        .expect("engine not poisoned after recreate")
        .expect("recreated namespace id");
    assert_ne!(
        original_ns_id, recreated_ns_id,
        "drop/recreate must allocate a fresh durable namespace id",
    );
    let gen_after_recreate = client.__published_catalog_gen();

    let mut writer_hook = client.__install_write_body_entry_hook(NS_A);
    let writer_client = client.clone();
    let writer = thread::spawn(move || -> Result<(), Error> {
        writer_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .insert_one(&doc! { "_id": 3102i32, "tag": "recreated-held" })
            .map(|_| ())
    });
    writer_hook
        .wait_until_entered_timeout(SETTLE_DEADLINE)
        .expect("writer on recreated namespace admitted");

    build_hook
        .release()
        .expect("build was waiting on release channel");
    let commit_finished_while_writer_held = flag_set_within(&build_done, SETTLE_DEADLINE);

    writer_hook
        .release()
        .expect("writer was waiting on release channel");
    writer
        .join()
        .expect("writer thread joined")
        .expect("writer on recreated namespace succeeds");
    let err = build
        .join()
        .expect("create_index thread joined")
        .expect_err("stale Ready flip must fail");

    assert!(
        commit_finished_while_writer_held,
        "Ready flip must reject stale ns_id without draining the recreated namespace lane",
    );
    assert!(
        matches!(
            err,
            Error::WriteConflict {
                reason: WriteConflictReason::CatalogGenerationChanged
            }
        ),
        "stale Ready flip must return write conflict, got {err:?}",
    );
    assert_eq!(
        client.__published_catalog_gen(),
        gen_after_recreate,
        "stale Ready flip must not publish against the recreated namespace",
    );
}

/// US-031 — source gate for `create_index_commit` owning the short Ready
/// publish directly instead of delegating to the name-based `run_ddl` helper.
#[test]
fn test_create_index_commit_source_gate_owns_ready_publish() {
    let project_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = project_root.join("src/storage/paged_engine/index_build.rs");
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    let body = extract_function_body(&source, "create_index_commit")
        .expect("create_index_commit function body must be locatable");

    let metadata_write = body
        .find(".metadata")
        .expect("create_index_commit must acquire metadata.write");
    let collection_lookup = body
        .find(".get_collection(ns)?")
        .expect("create_index_commit must resolve namespace identity");
    let index_lookup = body
        .find(".get_index(ns, name)?")
        .expect("create_index_commit must resolve target index identity");
    let state_check = body
        .find("IndexState::Building")
        .expect("create_index_commit must verify Building state");
    let drain = body
        .find("close_and_drain_guard")
        .expect("create_index_commit must drain admitted writers");
    let reserve_gen = body
        .find("next_catalog_gen")
        .expect("create_index_commit must reserve a fresh catalog generation");
    let register = body
        .find("register_with_oracle")
        .expect("create_index_commit must allocate a fresh publish slot");
    let mark_ready = body
        .find("mark_ready")
        .expect("create_index_commit must publish through mark_ready");
    let guard_commit = body
        .find("guard.commit")
        .expect("create_index_commit must reopen admissions after publish");

    assert!(
        metadata_write < collection_lookup
            && collection_lookup < index_lookup
            && index_lookup < state_check
            && state_check < drain
            && drain < reserve_gen
            && reserve_gen < register
            && register < mark_ready
            && mark_ready < guard_commit,
        "create_index_commit order must be metadata.write -> identity -> Building check -> drain -> fresh generation -> fresh slot -> publish -> guard.commit",
    );
    assert!(
        body.contains("mark_aborted")
            && body.contains("PostDurableDdlPublishFailure")
            && !body.contains("run_ddl(")
            && !body.contains("create_index_build")
            && !body.contains("build_index_mvcc")
            && !body.contains("metadata.read()"),
        "create_index_commit must own Ready publish/failure routing without scan work or run_ddl",
    );
}

// ---------------------------------------------------------------------------
// US-038 — create-index cleanup is a fresh guarded DDL delete.
// ---------------------------------------------------------------------------

/// US-038 — if the build scan fails after the Building entry is published,
/// cleanup must close and drain ordinary writers that admitted in the
/// post-Building window before deleting the Building catalog entry.
#[test]
fn test_create_index_cleanup_drains_dual_writers_before_delete() {
    let (_dir, client) = open_with_collection_a();
    let mut build_hook = client.__install_create_index_build_failure_hook(NS_A, TAG_INDEX);

    let build_client = client.clone();
    let build_done = Arc::new(AtomicBool::new(false));
    let build_done_flag = Arc::clone(&build_done);
    let build = thread::spawn(move || -> Result<(), Error> {
        let res = build_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .create_index(tag_index_model())
            .map(|_| ());
        build_done_flag.store(true, Ordering::Release);
        res
    });

    build_hook
        .wait_until_entered()
        .expect("create_index reached the build scan hook");

    let mut writer_hook = client.__install_write_body_entry_hook(NS_A);
    let writer_client = client.clone();
    let writer = thread::spawn(move || -> Result<(), Error> {
        writer_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .insert_one(&doc! { "_id": 3801i32, "tag": "cleanup-held" })
            .map(|_| ())
    });
    writer_hook
        .wait_until_entered_timeout(SETTLE_DEADLINE)
        .expect("post-Building writer admitted before cleanup starts");

    build_hook
        .release()
        .expect("build was waiting on release channel");
    thread::sleep(SHORT_SLEEP);
    let cleanup_waited_for_writer = !build_done.load(Ordering::Acquire);

    writer_hook
        .release()
        .expect("writer was waiting on release channel");
    writer
        .join()
        .expect("writer thread joined")
        .expect("post-Building writer succeeds before cleanup delete");

    wait_for_flag(&build_done, "create_index cleanup after writer drain");
    let err = build
        .join()
        .expect("create_index thread joined")
        .expect_err("injected build failure must surface after cleanup");
    assert!(
        matches!(err, Error::Internal(ref message) if message.contains("US-038 injected")),
        "expected injected build failure after cleanup, got {err:?}",
    );
    assert!(
        cleanup_waited_for_writer,
        "cleanup must drain the admitted post-Building writer before deleting the Building index",
    );

    let indexes = client
        .database(DB)
        .collection::<Document>(COLL_A)
        .list_indexes()
        .expect("list_indexes after cleanup");
    assert!(
        indexes.iter().all(|index| index.name != TAG_INDEX),
        "failed build cleanup must remove the Building index",
    );
    client
        .database(DB)
        .collection::<Document>(COLL_A)
        .insert_one(&doc! { "_id": 3802i32, "tag": "post-cleanup" })
        .expect("writer registry must reopen after cleanup");
}

/// US-038 — if `drop_index` removes the Building entry while the build scan
/// is still running, cleanup must revalidate identity and no-op without
/// publishing another catalog generation.
#[test]
fn test_create_index_cleanup_noops_if_drop_index_already_removed_building() {
    let (_dir, client) = open_with_collection_a();
    let mut build_hook = client.__install_create_index_build_failure_hook(NS_A, TAG_INDEX);

    let build_client = client.clone();
    let build = thread::spawn(move || -> Result<(), Error> {
        build_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .create_index(tag_index_model())
            .map(|_| ())
    });

    build_hook
        .wait_until_entered()
        .expect("create_index reached the build scan hook");
    client
        .database(DB)
        .collection::<Document>(COLL_A)
        .drop_index(TAG_INDEX)
        .expect("drop_index removes the Building entry first");
    let gen_after_drop = client.__published_catalog_gen();

    build_hook
        .release()
        .expect("build was waiting on release channel");
    let err = build
        .join()
        .expect("create_index thread joined")
        .expect_err("injected build failure must surface");
    assert!(
        matches!(err, Error::Internal(ref message) if message.contains("US-038 injected")),
        "expected injected build failure after no-op cleanup, got {err:?}",
    );
    assert_eq!(
        client.__published_catalog_gen(),
        gen_after_drop,
        "cleanup must not reserve or publish when drop_index already removed Building",
    );
}

/// US-023 — source gate for `drop_index`'s metadata-protected identity
/// resolution, RAII DDL barrier, publish-sequencer ownership, and failure
/// routing.
#[test]
fn test_drop_index_revalidates_ns_and_index_under_metadata_write() {
    let project_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = project_root.join("src/storage/paged_engine/index_maint.rs");
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    let body = extract_function_body(&source, "drop_index")
        .expect("drop_index function body must be locatable");

    let metadata_write = body
        .find(".metadata")
        .expect("drop_index must acquire metadata.write");
    let collection_lookup = body
        .find(".get_collection(ns)?")
        .expect("drop_index must resolve namespace identity under metadata.write");
    let index_lookup = body
        .find(".get_index(ns, name)?")
        .expect("drop_index must resolve target index identity");
    let drain = body
        .find("close_and_drain_guard")
        .expect("drop_index must close and drain the namespace writer lane");
    let register = body
        .find("register_with_oracle")
        .expect("drop_index must allocate a fresh publish slot");
    let mark_ready = body
        .find("mark_ready")
        .expect("drop_index must publish through mark_ready");
    let guard_commit = body
        .find("guard.commit")
        .expect("drop_index must explicitly reopen admissions after publish");
    assert!(
        metadata_write < collection_lookup
            && collection_lookup < index_lookup
            && index_lookup < drain
            && drain < register
            && register < mark_ready
            && mark_ready < guard_commit,
        "drop_index order must be metadata.write -> identity -> drain -> fresh slot -> publish -> guard.commit",
    );
    assert!(
        body.contains("mark_aborted")
            && body.contains("PostDurableDdlPublishFailure")
            && !body.contains("run_ddl(")
            && !body.contains("lane_for(")
            && !body.contains("acquire_lane(")
            && !body.contains("published.store("),
        "drop_index must not use legacy lanes/run_ddl/published.store and must route failures",
    );
}

/// US-023 — source gate for drop-index page reclamation. Runtime tests
/// cover the drain/build overlap; this locks the page-free structure so
/// every index page is collected, sorted by page id, latched exclusively,
/// and only then freed before the catalog delete publishes.
#[test]
fn test_drop_index_page_free_holds_exclusive_latches_against_build_scan() {
    let project_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let maint_path = project_root.join("src/storage/paged_engine/index_maint.rs");
    let build_path = project_root.join("src/storage/paged_engine/index_build.rs");
    let maint_source = std::fs::read_to_string(&maint_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", maint_path.display()));
    let build_source = std::fs::read_to_string(&build_path)
        .unwrap_or_else(|err| panic!("read {}: {err}", build_path.display()));

    let drop_body = extract_function_body(&maint_source, "drop_index")
        .expect("drop_index function body must be locatable");
    let free_call = drop_body
        .find("free_index_pages_exclusive")
        .expect("drop_index must free target index pages under exclusive latches");
    let catalog_delete = drop_body
        .find("cat.drop_index(ns, name)?")
        .expect("drop_index must delete the catalog entry after page-free staging");
    assert!(
        free_call < catalog_delete,
        "drop_index must free target pages before deleting the catalog entry",
    );

    let free_body = extract_function_body(&build_source, "free_index_pages_exclusive")
        .expect("free_index_pages_exclusive function body must be locatable");
    assert_free_helper_latches_before_free(&free_body, "drop-index page-free");
}

// ---------------------------------------------------------------------------
// US-024 — drop-namespace DDL barrier.
// ---------------------------------------------------------------------------

/// US-024 — `drop_namespace` drains already-admitted writers before it
/// force-expires active read views and frees pages. The held writer proves the
/// drop is waiting on the namespace writer registry rather than deleting pages
/// behind an in-flight CRUD body.
#[test]
fn test_drop_namespace_force_expire_with_concurrent_writers() {
    let (_dir, client) = open_with_collection_a();
    let registry = client
        .__read_view_registry()
        .expect("buffer-pool backed client has a ReadViewRegistry");
    let view = ReadView::open(
        Arc::clone(&registry),
        Ts {
            physical_ms: 24_000,
            logical: 0,
        },
        24_000,
    );
    assert!(!view.is_poisoned(), "view starts active");

    let mut writer_hook = client.__install_write_body_entry_hook(NS_A);
    let writer_client = client.clone();
    let writer = thread::spawn(move || -> Result<(), Error> {
        writer_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .insert_one(&doc! { "_id": 2401i32, "v": "held-before-drop" })
            .map(|_| ())
    });
    writer_hook
        .wait_until_entered()
        .expect("writer reached body-entry hook");

    let drop_done = Arc::new(AtomicBool::new(false));
    let drop_done_flag = Arc::clone(&drop_done);
    let drop_client = client.clone();
    let dropper = thread::spawn(move || -> Result<(), Error> {
        let res = drop_client.database(DB).drop_collection(COLL_A);
        drop_done_flag.store(true, Ordering::Release);
        res
    });

    thread::sleep(SHORT_SLEEP);
    assert!(
        !drop_done.load(Ordering::Acquire),
        "drop_namespace must drain the already-admitted writer before completing",
    );

    writer_hook
        .release()
        .expect("writer was waiting on release channel");
    writer
        .join()
        .expect("writer thread joined")
        .expect("held writer succeeds before drop publishes");

    wait_for_flag(&drop_done, "drop_namespace after writer release");
    dropper
        .join()
        .expect("drop thread joined")
        .expect("drop_namespace succeeds after drain");

    assert!(
        view.is_poisoned(),
        "drop_namespace must force-expire active ReadViews before page-free",
    );
    assert!(matches!(view.check_active(), Err(Error::ReadViewExpired)));
}

/// US-024 / Phase 6 US-003 — while `drop_namespace` waits for a held CRUD
/// writer's metadata guard, the namespace writer-registry lane is not yet the
/// blocking authority. After the writer releases, drop reaches the DDL drain
/// fence and completes.
#[test]
fn test_drop_namespace_admit_refused_during_drain() {
    let (_dir, client) = open_with_collection_a();
    let ns_id = client
        .__us036_namespace_id(NS_A)
        .expect("engine not poisoned at setup")
        .expect("namespace id");

    let mut writer_hook = client.__install_write_body_entry_hook(NS_A);
    let writer_client = client.clone();
    let writer = thread::spawn(move || -> Result<(), Error> {
        writer_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .insert_one(&doc! { "_id": 2402i32, "v": "held-before-drop" })
            .map(|_| ())
    });
    writer_hook
        .wait_until_entered()
        .expect("writer reached body-entry hook");

    let drop_done = Arc::new(AtomicBool::new(false));
    let drop_done_flag = Arc::clone(&drop_done);
    let drop_client = client.clone();
    let dropper = thread::spawn(move || -> Result<(), Error> {
        let res = drop_client.database(DB).drop_collection(COLL_A);
        drop_done_flag.store(true, Ordering::Release);
        res
    });

    assert!(
        !flag_set_within(&drop_done, SHORT_SLEEP),
        "drop_namespace must wait behind the paused writer's metadata guard",
    );
    let lane_ticket = client
        .__us036_admit_writer(ns_id, 0)
        .expect("ordinary CRUD must not close the DDL admission lane before metadata.write");
    drop(lane_ticket);

    writer_hook
        .release()
        .expect("writer was waiting on release channel");
    writer
        .join()
        .expect("writer thread joined")
        .expect("held writer succeeds before drop publishes");
    wait_for_flag(&drop_done, "drop_namespace after writer release");
    dropper
        .join()
        .expect("drop thread joined")
        .expect("drop_namespace succeeds after drain");

    client
        .database(DB)
        .create_collection(COLL_A)
        .expect("recreate after drop");
    let new_ns_id = client
        .__us036_namespace_id(NS_A)
        .expect("engine not poisoned after recreate")
        .expect("recreated namespace id");
    let ticket = client
        .__us036_admit_writer(new_ns_id, 0)
        .expect("recreated namespace lane admits cleanly");
    drop(ticket);
}

/// US-024 — source gate for `drop_namespace`'s metadata-protected namespace
/// identity, RAII DDL barrier, publish-sequencer ownership, and failure
/// routing.
#[test]
fn test_drop_namespace_revalidates_ns_id_under_metadata_write() {
    let project_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = project_root.join("src/storage/paged_engine.rs");
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    let body = extract_function_body(&source, "drop_namespace")
        .expect("drop_namespace function body must be locatable");

    let metadata_write = body
        .find(".metadata")
        .expect("drop_namespace must acquire metadata.write");
    let collection_lookup = body
        .find("cat.get_collection(ns)?")
        .expect("drop_namespace must resolve namespace identity under metadata.write");
    let drain = body
        .find("close_and_drain_guard")
        .expect("drop_namespace must close and drain the namespace writer lane");
    let force_expire = body
        .find("force_expire_all")
        .expect("drop_namespace must force-expire readers");
    let register = body
        .find("register_with_oracle")
        .expect("drop_namespace must allocate a fresh publish slot");
    let mark_ready = body
        .find("mark_ready")
        .expect("drop_namespace must publish through mark_ready");
    let mark_dropped = body
        .find("mark_dropped")
        .expect("drop_namespace must mark the RAII guard as dropped");
    assert!(
        metadata_write < collection_lookup
            && collection_lookup < drain
            && drain < force_expire
            && force_expire < register
            && register < mark_ready
            && mark_ready < mark_dropped,
        "drop_namespace order must be metadata.write -> ns_id -> drain -> force_expire -> fresh slot -> publish -> mark_dropped",
    );
    assert!(
        body.contains("mark_aborted")
            && body.contains("PostDurableDdlPublishFailure")
            && !body.contains("run_ddl(")
            && !body.contains("ns_lanes")
            && !body.contains("guard.commit"),
        "drop_namespace must not use legacy lanes/run_ddl, and must route pre/post-durable failures",
    );
}

/// US-024 — a drop followed by an insert through the same `Client` must not
/// resurrect rows from the dropped incarnation. Durable namespace ids, not a
/// name-keyed tombstone set, isolate the new incarnation.
#[test]
fn test_drop_then_insert_same_session_does_not_resurrect() {
    let (_dir, client) = open_with_collection_a();
    let coll = client.database(DB).collection::<Document>(COLL_A);
    coll.insert_one(&doc! { "_id": 2403i32, "v": "before-drop" })
        .expect("insert before drop");

    client
        .database(DB)
        .drop_collection(COLL_A)
        .expect("drop namespace");
    coll.insert_one(&doc! { "_id": 2404i32, "v": "after-drop" })
        .expect("same-session insert recreates cleanly");

    assert_eq!(
        coll.count_documents(doc! {}).expect("count after recreate"),
        1,
        "recreated collection must contain only the post-drop row",
    );
    assert_eq!(
        coll.count_documents(doc! { "v": "before-drop" })
            .expect("count pre-drop value"),
        0,
        "pre-drop row must not resurrect",
    );
}

/// US-024 — after a namespace is dropped, reusing the same name allocates a
/// fresh durable namespace id and starts from an empty collection.
#[test]
fn test_dropped_namespace_name_can_be_reused_cleanly() {
    let (_dir, client) = open_with_collection_a();
    let original_ns_id = client
        .__us036_namespace_id(NS_A)
        .expect("engine not poisoned at setup")
        .expect("original namespace id");

    client
        .database(DB)
        .drop_collection(COLL_A)
        .expect("drop namespace");
    assert_eq!(
        client
            .__us036_namespace_id(NS_A)
            .expect("engine not poisoned after drop"),
        None,
        "dropped namespace id must disappear from the published catalog",
    );

    client
        .database(DB)
        .create_collection(COLL_A)
        .expect("reuse dropped namespace name");
    let recreated_ns_id = client
        .__us036_namespace_id(NS_A)
        .expect("engine not poisoned after recreate")
        .expect("recreated namespace id");
    assert!(
        recreated_ns_id > original_ns_id,
        "namespace id must be monotonic across drop/recreate: old={original_ns_id}, new={recreated_ns_id}",
    );

    let coll = client.database(DB).collection::<Document>(COLL_A);
    coll.insert_one(&doc! { "_id": 2405i32, "v": "fresh" })
        .expect("insert into recreated namespace");
    assert_eq!(coll.count_documents(doc! {}).expect("fresh count"), 1);
}

/// US-024 — source gate for lock ordering: drop-namespace takes
/// `metadata.write()`, resolves and drains the namespace lane, force-expires
/// readers, then performs page-free under explicit exclusive page latches
/// before catalog removal and publish.
#[test]
fn test_drop_namespace_does_not_invert_lock_order() {
    let project_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = project_root.join("src/storage/paged_engine.rs");
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    let body = extract_function_body(&source, "drop_namespace")
        .expect("drop_namespace function body must be locatable");
    let free_body = extract_function_body(&source, "free_tree_pages_exclusive")
        .expect("free_tree_pages_exclusive function body must be locatable");

    let metadata_write = body
        .find(".metadata")
        .expect("drop_namespace must acquire metadata.write");
    let drain = body
        .find("close_and_drain_guard")
        .expect("drop_namespace must drain through NsDdlBarrierGuard");
    let force_expire = body
        .find("force_expire_all")
        .expect("drop_namespace must force-expire readers");
    let free_call = body
        .find("free_tree_pages_exclusive")
        .expect("drop_namespace must free tree pages under exclusive latches");
    let catalog_drop = body
        .find("cat.drop_collection(ns)?")
        .expect("drop_namespace must delete the catalog entry");
    assert!(
        metadata_write < drain
            && drain < force_expire
            && force_expire < free_call
            && free_call < catalog_drop,
        "drop_namespace lock order must be metadata -> registry drain -> force-expire -> page-free -> catalog delete",
    );

    assert_free_helper_latches_before_free(&free_body, "drop-namespace page-free");
}

/// US-024 — CRUD, checkpoint/reconcile, and drop-namespace can overlap without
/// a metadata/registry cycle or stale lane after the namespace is dropped.
#[test]
fn test_crud_reconcile_checkpoint_drop_namespace_interleaved() {
    let (_dir, client) = open_with_collection_a();
    let coll = client.database(DB).collection::<Document>(COLL_A);
    for id in 1..=8 {
        coll.insert_one(&doc! { "_id": 2406 + id, "pad": "x".repeat(2048) })
            .expect("seed before checkpoint");
    }
    client.checkpoint().expect("checkpoint seeded tree");
    for id in 1..=4 {
        coll.update_one(
            doc! { "_id": 2406 + id },
            doc! { "$set": { "pad": "y".repeat(4096) } },
        )
        .run()
        .expect("dirty update before interleave");
    }

    let mut writer_hook = client.__install_write_body_entry_hook(NS_A);
    let writer_client = client.clone();
    let writer = thread::spawn(move || -> Result<(), Error> {
        writer_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .insert_one(&doc! { "_id": 2499i32, "pad": "held" })
            .map(|_| ())
    });
    writer_hook
        .wait_until_entered()
        .expect("writer reached body-entry hook");

    let checkpoint_done = Arc::new(AtomicBool::new(false));
    let checkpoint_done_flag = Arc::clone(&checkpoint_done);
    let checkpoint_client = client.clone();
    let checkpoint = thread::spawn(move || -> Result<(), Error> {
        let res = checkpoint_client.checkpoint();
        checkpoint_done_flag.store(true, Ordering::Release);
        res
    });

    let drop_done = Arc::new(AtomicBool::new(false));
    let drop_done_flag = Arc::clone(&drop_done);
    let drop_client = client.clone();
    let dropper = thread::spawn(move || -> Result<(), Error> {
        let res = drop_client.database(DB).drop_collection(COLL_A);
        drop_done_flag.store(true, Ordering::Release);
        res
    });

    thread::sleep(SHORT_SLEEP);
    writer_hook
        .release()
        .expect("writer was waiting on release channel");

    let writer_res = writer.join().expect("writer thread joined");
    match writer_res {
        Ok(()) => {}
        Err(Error::WriteConflict {
            reason: WriteConflictReason::CatalogGenerationChanged,
        }) => {}
        Err(Error::WriterBusy) => {}
        other => panic!("writer must terminate without engine fatal, got {other:?}"),
    }

    wait_for_flag(&checkpoint_done, "checkpoint during drop interleave");
    checkpoint
        .join()
        .expect("checkpoint thread joined")
        .expect("checkpoint terminates during drop interleave");
    wait_for_flag(&drop_done, "drop during checkpoint interleave");
    dropper
        .join()
        .expect("drop thread joined")
        .expect("drop_namespace succeeds during checkpoint interleave");

    assert_eq!(
        client
            .__us036_namespace_id(NS_A)
            .expect("engine not poisoned after interleave"),
        None,
        "dropped namespace must be absent after interleaved checkpoint/drop",
    );
    client
        .database(DB)
        .create_collection(COLL_A)
        .expect("recreate after interleaved drop");
    coll.insert_one(&doc! { "_id": 2500i32, "pad": "fresh" })
        .expect("writer registry must be clean after interleaved drop");
}

/// US-038 — cleanup must use the namespace id and target index identity from
/// the failed build. If the namespace name was dropped and recreated while the
/// build scan was running, cleanup must not drain or publish against the new
/// namespace lane.
#[test]
fn test_create_index_cleanup_revalidates_ns_id_under_metadata_write() {
    let (_dir, client) = open_with_collection_a();
    let original_ns_id = client
        .__us036_namespace_id(NS_A)
        .expect("engine not poisoned at setup")
        .expect("original namespace id");
    let mut build_hook = client.__install_create_index_build_failure_hook(NS_A, TAG_INDEX);

    let build_client = client.clone();
    let build_done = Arc::new(AtomicBool::new(false));
    let build_done_flag = Arc::clone(&build_done);
    let build = thread::spawn(move || -> Result<(), Error> {
        let res = build_client
            .database(DB)
            .collection::<Document>(COLL_A)
            .create_index(tag_index_model())
            .map(|_| ());
        build_done_flag.store(true, Ordering::Release);
        res
    });

    build_hook
        .wait_until_entered()
        .expect("create_index reached the build scan hook");
    client
        .database(DB)
        .drop_collection(COLL_A)
        .expect("drop original namespace");
    client
        .database(DB)
        .create_collection(COLL_A)
        .expect("recreate namespace with same name");
    let recreated_ns_id = client
        .__us036_namespace_id(NS_A)
        .expect("engine not poisoned after recreate")
        .expect("recreated namespace id");
    assert_ne!(
        original_ns_id, recreated_ns_id,
        "drop/recreate must allocate a fresh durable namespace id",
    );
    let gen_after_recreate = client.__published_catalog_gen();

    build_hook
        .release()
        .expect("build was waiting on release channel");
    wait_for_flag(&build_done, "stale create_index cleanup");
    let build_err = build
        .join()
        .expect("create_index thread joined")
        .expect_err("stale cleanup must reject the old namespace identity");
    assert!(
        matches!(build_err, Error::Internal(ref message) if message.contains("cleanup also failed")),
        "create_index must report cleanup rejection for stale namespace identity, got {build_err:?}",
    );
    assert_eq!(
        client.__published_catalog_gen(),
        gen_after_recreate,
        "stale cleanup must not publish against the recreated namespace",
    );

    let project_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = project_root.join("src/storage/paged_engine/index_build.rs");
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    let body = extract_function_body(&source, "create_index_cleanup")
        .expect("create_index_cleanup function body must be locatable");
    let first_identity_check = body
        .find("collection.id != target.ns_id")
        .expect("cleanup must revalidate namespace identity");
    let drain = body
        .find("close_and_drain_guard")
        .expect("cleanup must keep DDL drain fencing for live targets");
    assert!(
        first_identity_check < drain,
        "stale cleanup must reject namespace-id drift before DDL drain fencing",
    );
}

/// US-038 — source gate for cleanup's guarded DDL delete protocol and
/// exclusive page-free ordering. Runtime tests above cover the drain/no-op
/// behavior; this gate locks the page-latch and publish-slot structure.
#[test]
fn test_create_index_cleanup_page_free_holds_exclusive_latches_against_build_scan() {
    let project_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = project_root.join("src/storage/paged_engine/index_build.rs");
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    let cleanup_body = extract_function_body(&source, "create_index_cleanup")
        .expect("create_index_cleanup function body must be locatable");

    let metadata_write = cleanup_body
        .find(".metadata")
        .expect("cleanup must acquire metadata.write");
    let collection_lookup = cleanup_body
        .find("cat.get_collection(ns)?")
        .expect("cleanup must resolve namespace identity under metadata.write");
    let index_lookup = cleanup_body
        .find("cat.get_index(ns, name)?")
        .expect("cleanup must resolve target index identity");
    let drain = cleanup_body
        .find("close_and_drain_guard")
        .expect("cleanup must close and drain the namespace writer lane");
    let register = cleanup_body
        .find("register_with_oracle")
        .expect("cleanup must allocate a fresh publish slot");
    let mark_ready = cleanup_body
        .find("mark_ready")
        .expect("cleanup must publish through mark_ready");
    let guard_commit = cleanup_body
        .find("guard.commit")
        .expect("cleanup must explicitly reopen admissions after publish");
    assert!(
        metadata_write < collection_lookup
            && collection_lookup < index_lookup
            && index_lookup < drain
            && drain < register
            && register < mark_ready
            && mark_ready < guard_commit,
        "cleanup order must be metadata.write -> identity -> drain -> fresh slot -> publish -> guard.commit",
    );
    assert!(
        cleanup_body.contains("mark_aborted")
            && cleanup_body.contains("PostDurableDdlPublishFailure")
            && !cleanup_body.contains("run_ddl(")
            && !cleanup_body.contains("published.store("),
        "cleanup must not use run_ddl/published.store and must route pre/post-durable failures correctly",
    );

    let free_body = extract_function_body(&source, "free_index_pages_exclusive")
        .expect("free_index_pages_exclusive function body must be locatable");
    assert_free_helper_latches_before_free(&free_body, "cleanup page-free");
}

/// US-013 — source gate for stale namespace identity and publish
/// sequencing. `create_index_reserve` must resolve the namespace id under
/// `metadata.write()`, close through `NsDdlBarrierGuard`, publish through
/// `PublishSequencer::mark_ready`, and avoid the retired namespace lane
/// wrappers.
#[test]
fn test_create_index_reserve_revalidates_ns_id_under_metadata_write() {
    let project_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let path = project_root.join("src/storage/paged_engine/index_build.rs");
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("read {}: {err}", path.display()));
    let body = extract_function_body(&source, "create_index_reserve")
        .expect("create_index_reserve function body must be locatable");

    let metadata_write = body
        .find(".metadata")
        .expect("create_index_reserve must reference self.metadata");
    let get_collection = body
        .find("cat.get_collection(ns)?")
        .expect("create_index_reserve must resolve the namespace under metadata.write");
    let drain = body
        .find("close_and_drain_guard")
        .expect("create_index_reserve must use the RAII DDL barrier guard");
    let register = body
        .find("register_with_oracle")
        .expect("create_index_reserve must register a publish slot");
    let mark_ready = body
        .find("mark_ready")
        .expect("create_index_reserve must publish through mark_ready");
    assert!(
        metadata_write < get_collection && get_collection < drain,
        "namespace identity must be resolved under metadata.write before close_and_drain_guard",
    );
    assert!(
        drain < register && register < mark_ready,
        "create_index_reserve must drain, register, then publish Building through mark_ready",
    );
    assert!(
        !body.contains("lane_for(") && !body.contains("acquire_lane("),
        "create_index_reserve must not use retired namespace-lane wrappers",
    );
}

/// US-013 — grep gate for direct DDL publish bypasses and manual
/// writer-registry reopen calls.
#[test]
fn test_create_index_publish_and_raii_guard_source_gates() {
    let project_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let paths = [
        project_root.join("src/storage/paged_engine.rs"),
        project_root.join("src/storage/paged_engine/index_build.rs"),
        project_root.join("src/storage/paged_engine/index_maint.rs"),
    ];
    let mut combined = String::new();
    for path in paths {
        combined.push_str(
            &std::fs::read_to_string(&path)
                .unwrap_or_else(|err| panic!("read {}: {err}", path.display())),
        );
        combined.push('\n');
    }

    assert!(
        !combined.contains("published.store("),
        "DDL call sites must not store PublishedEpoch directly",
    );

    let publish_calls = combined.matches("rebuild_and_publish_locked(").count();
    let mark_ready_calls = combined.matches(".mark_ready(").count();
    assert!(
        mark_ready_calls >= publish_calls,
        "publish surfaces must route rebuild_and_publish_locked through mark_ready; rebuilds={publish_calls}, mark_ready={mark_ready_calls}",
    );

    assert!(
        !combined.contains("ns_writers.reopen") && !combined.contains(".reopen(ns_id)"),
        "DDL call sites must settle writer admissions through NsDdlBarrierGuard::commit/Drop",
    );
}
