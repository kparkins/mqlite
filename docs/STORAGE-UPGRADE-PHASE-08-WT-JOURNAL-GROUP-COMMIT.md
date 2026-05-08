# Storage Upgrade Phase 8 — WiredTiger-Style Journal And Group Commit

**Status:** Implemented in the current Phase 8 remediation branch.
**Parent plan:** post-v1 storage extension after `docs/STORAGE-UPGRADE-v1.md`.
**Canonical execution artifact:** `.omc/phase-08-prd.json`.
**Ralph adapters:** `.omx/plans/prd-phase-08-wt-journal-group-commit.md`
and `.omx/plans/test-spec-phase-08-wt-journal-group-commit.md`.

Phase 8 replaces the current shared-tail journal envelope with a
WiredTiger-style LSN-addressed redo log and high-water group commit. There is
no production migration requirement. Existing pre-Phase-8 journal bytes are not
read, parsed, or migrated; the Phase 8 opener may discard/truncate them under
the pre-release storage contract.

---

## 1. Why This Phase Exists

Phase 5 made ordinary CRUD bodies overlap, but the durability path still
serializes on a global journal envelope. Current HEAD has three load-bearing
constraints that prevent true log-level multi-writer behavior:

1. `JournalManager` owns one mutable `write_cursor` and writes with
   `seek + write_all`.
2. `begin_txn` / `rollback_txn` treats rollback as truncating the shared
   journal tail.
3. `handle.flush()` writes global dirty main/history/header pages and is
   therefore unsafe to run concurrently with another writer's unjournaled dirty
   state.

Phase 8 deletes those constraints. Writers build a length-known record draft in
memory, reserve a byte LSN range, finalize the record header with that range,
write at the reserved offset, and join durability by `end_lsn`. Group commit
advances `durable_lsn`, not synthetic tickets. Recovery consumes complete
CRC-valid records and ignores torn or incomplete tail bytes.

---

## 2. Scope

Phase 8 owns the journal, group commit, recovery authority, and commit
durability boundary. It does not redesign query planning, index semantics,
read-view semantics, or the global `PublishSequencer` visibility contract.

In scope:

- New LSN-addressed redo log record format.
- Per-writer log record draft/finalize/write flow.
- Offset-based log-slot reservation and append.
- LSN high-water group commit for `FullSync`.
- Interval/None durability semantics over the same log manager.
- Recovery from complete committed log records.
- Removal of live journal-tail rollback.
- Removal of commit-path global dirty flush.
- Page `last_lsn` fencing for checkpoint/main-file materialization.
- Conversion of CRUD, DDL, index, and checkpoint journal users to the new log.
- Log checkpoint lifecycle: persisted `checkpoint_applied_lsn`, main-file fsync
  ordering, and recovery replay start rules.
- Deletion or replacement of `JournalIndex` / `shm` assumptions that depend on
  old page-frame journal authority.

Out of scope:

- Per-namespace `PublishedEpoch.visible_ts`.
- Per-namespace publish sequencers.
- Multi-document transactions.
- Prepared transactions.
- Background eviction redesign beyond the page-LSN fence needed here.
- Migration, compatibility parsing, or fail-fast legacy format detection for
  old pre-release journal files.
- Physical prefix recycling or truncation of valid old log records. Phase 8
  keeps byte-offset LSNs stable by retaining valid log bytes for the life of the
  file. Segment/base-LSN recycling can be a later phase.
- DDL reader admission and deferred physical free for destructive namespace/index
  teardown. Track this as the next object-lifetime cleanup before treating drop
  operations as WiredTiger-grade under mixed readers.

---

## 3. Non-Negotiable Invariants

1. **No live tail rollback.** Ordinary commit cleanup must never call
   `truncate_to(mark)` to represent transaction abort.
2. **Commit records are self-contained.** Recovery must not pair an orphan
   logical frame with a later chain frame. One CRC-valid record is the durable
   authority for one committed operation.
3. **LSNs are byte offsets.** `start_lsn` is the record's byte offset in the
   log. `end_lsn` is exclusive. Group commit and recovery speak in byte LSNs.
4. **Record finalization is two-stage.** Payload length and payload CRC are
   computed before reservation; `start_lsn`, `end_lsn`, and header CRC are
   filled after reservation and before `write_reserved`.
5. **Post-reservation write failure is fatal.** Once an LSN range is reserved,
   any failure before `mark_written` poisons the live engine. The process must
   not skip the gap or let later records become durable. Reopen truncates to the
   valid prefix.
6. **`ready_lsn` is contiguous.** `ready_lsn` may advance only over a
   contiguous prefix of fully written records. A gap blocks later records from
   becoming durable in the live process.
7. **`durable_lsn` only follows synced bytes.** `durable_lsn` may advance only
   after the log file has synced every byte through the selected ready
   frontier.
8. **FullSync publish is gated by durability.** A `FullSync` writer may not
   flip `Pending -> Committed` or call `PublishSequencer::mark_ready` until
   `durable_lsn >= end_lsn`.
9. **Interval publish is gated by completed log write and publish order.**
   `Interval` may publish after the complete record is written and `ready_lsn`
   covers it, but it still waits on the global `PublishSequencer` order. Crash
   durability is guaranteed only through the most recent sync.
10. **Main-file writes are LSN-fenced.** Checkpoint, eviction, or reconcile may
    write a dirty page only when `page.last_lsn <= durable_lsn`. If a caller
    needs to flush a page above `durable_lsn`, it must first force log sync
    through that page LSN.
11. **Publish order remains global.** `PublishSequencer` continues to own dense
    publish order, HLC commit timestamp allocation, and monotonic global
    `PublishedEpoch.visible_ts`.
12. **Publish sequence is the replay order.** LSN order is the durable prefix
    validation order. Recovery applies accepted records by persisted
    `publish_seq` order, with gaps allowed for slots that aborted before a
    record existed. Duplicate `publish_seq` values are corruption.
13. **Sync failure is fatal.** A failed sync poisons the live file-backed engine
    for `FullSync` and `Interval`, wakes all waiters, and leaves already-visible
    interval writes to reopen recovery. `None` does not initiate sync, but any
    sync it explicitly forces through checkpoint/flush uses the same fatal rule.

---

## 4. Target Architecture

### 4.1 Log File

The new log replaces the mixed page-frame/logical journal authority. It may use
a new suffix such as `.mqlite-log`; it must not silently interpret an existing
`.mqlite-journal` file as Phase 8 data.

Every record uses one outer envelope:

```text
LogRecord
  magic: u32
  format_version: u16
  header_len: u16
  record_kind: u16
  flags: u16
  total_len: u32
  start_lsn: u64
  end_lsn: u64
  txn_id: u64
  publish_seq: u64
  commit_ts_physical_ms: u64
  commit_ts_logical: u32
  payload_len: u32
  header_crc32c: u32
  payload_crc32c: u32
  payload: kind-specific bytes
```

`header_len` must equal the fixed Phase 8 header size. `total_len` must equal
`header_len + payload_len`. `end_lsn` must equal `start_lsn + total_len`.
All integer fields are little-endian. The header CRC covers bytes
`0..header_len` with the `header_crc32c` field zeroed. The payload CRC covers
exactly `payload_len` payload bytes. `MAX_LOG_RECORD_BYTES` is `64 MiB`;
oversize drafts fail before LSN reservation.

Unknown `record_kind`, unknown flag bits, invalid kind/flag combinations,
length mismatches, and CRC mismatches are invalid and stop recovery at the
previous valid LSN.

Legal `record_kind` values:

- `CrudCommit`: payload contains the logical operation bytes plus the
  chain/refcount/page-write payload needed to replay the commit as one unit.
- `CatalogCommit`: payload contains namespace/catalog/index lifecycle metadata
  changes that participate in recovery authority.
- `CheckpointBoundary`: payload records the persisted checkpoint frontier and
  the log LSN through which main-file state has been materialized.

`publish_seq = 0` is legal only for `CheckpointBoundary`. Checkpoint records
are control records: recovery validates them in the LSN prefix scan, uses them
to establish `checkpoint_applied_lsn`, and excludes them from publish-sequence
sorting, duplicate-publish checks, HLC/publish floor seeding, and CRUD/catalog
apply ordering.

Legal kind/flag combinations:

- `CrudCommit`: `HAS_LOGICAL_PAYLOAD | HAS_CHAIN_PAYLOAD`
- `CatalogCommit`: `HAS_CATALOG_PAYLOAD`
- `CheckpointBoundary`: `CHECKPOINT_BOUNDARY`

Legal bits are `0x0001 HAS_LOGICAL_PAYLOAD`,
`0x0002 HAS_CHAIN_PAYLOAD`, `0x0004 HAS_CATALOG_PAYLOAD`, and
`0x0008 CHECKPOINT_BOUNDARY`.

`commit_ts` serializes the existing `Ts` shape as physical milliseconds plus
logical counter. The codec must round-trip the exact `Ts` value and preserve the
HLC floor on recovery.

### 4.2 Two-Stage Codec

The codec has two explicit types:

```rust
pub(crate) struct LogRecordDraft {
    kind: LogRecordKind,
    flags: LogRecordFlags,
    txn_id: u64,
    publish_seq: u64,
    commit_ts: Ts,
    payload: Vec<u8>,
}

pub(crate) struct FinalizedLogRecord {
    start_lsn: u64,
    end_lsn: u64,
    bytes: Vec<u8>,
}
```

Required operations:

```rust
impl LogRecordDraft {
    pub(crate) fn encoded_len(&self) -> Result<usize>;
    pub(crate) fn finalize(self, start_lsn: u64) -> Result<FinalizedLogRecord>;
}

impl FinalizedLogRecord {
    pub(crate) fn end_lsn(&self) -> u64;
    pub(crate) fn bytes(&self) -> &[u8];
}
```

`encoded_len` is computed before reservation. `finalize` fills
`start_lsn/end_lsn`, computes header CRC after the LSN splice, and produces the
exact bytes passed to `write_reserved`.

### 4.3 Log Manager

`JournalManager` is replaced or narrowed into `LogManager`:

```rust
pub(crate) struct LogManager {
    next_lsn: AtomicU64,
    ready_lsn: AtomicU64,
    durable_lsn: AtomicU64,
    slots: Mutex<BTreeMap<u64, LogSlotState>>,
    sync_cv: Condvar,
    sync_in_progress: AtomicBool,
    file: PositionedLogFile,
}
```

Required APIs:

```rust
impl LogManager {
    pub(crate) fn reserve(&self, bytes_len: usize) -> Result<LogSlot>;
    pub(crate) fn write_reserved(&self, slot: &LogSlot, bytes: &[u8]) -> Result<()>;
    pub(crate) fn mark_written(&self, slot: &LogSlot) -> Result<LogWriteReceipt>;
    pub(crate) fn poison_slot(&self, slot: &LogSlot, error: Error) -> Error;
    pub(crate) fn wait_ready(&self, end_lsn: u64) -> Result<()>;
    pub(crate) fn ensure_sync(&self, target_lsn: u64) -> Result<()>;
    pub(crate) fn wait_durable(&self, end_lsn: u64) -> Result<()>;
    pub(crate) fn ready_lsn(&self) -> u64;
    pub(crate) fn durable_lsn(&self) -> u64;
}
```

`PositionedLogFile` must use offset writes. It must not rely on a shared
`seek` cursor for correctness.

`reserve` only allocates bytes. It must not mutate publish state, commit
timestamp state, or namespace state. The record still persists the already
allocated `publish_seq` so recovery can apply records in the same order live
publish used. A write failure after reservation calls `poison_slot`; the live
engine is fatal until reopen.

### 4.4 Group Commit

The current ticket-based `GroupCommitManager` is deleted. LSN group commit is
owned by `LogManager` or a replacement `LsnGroupCommit` helper.

Leader/waiter protocol:

```text
wait_durable(end_lsn):
  loop:
    if engine_poisoned: return fatal error
    if durable_lsn >= end_lsn: return ok
    if ready_lsn < end_lsn:
      wait on sync_cv until ready_lsn >= end_lsn or poisoned
      continue
    if sync_in_progress CAS false -> true succeeds:
      target = ready_lsn
      result = sync_data(log_file)
      if result ok:
        durable_lsn.store(target, Release)
      else:
        poison engine with sync failure
      sync_in_progress.store(false, Release)
      notify all sync_cv waiters
    else:
      wait on sync_cv until durable_lsn >= end_lsn, leader released, or poisoned
```

Late arrivals whose `end_lsn > target` are not covered by the in-flight sync and
must drive or wait for a later sync. The sync leader must not hold page latches,
metadata locks, publish-sequencer mutexes, or slot-state mutexes while calling
the OS sync primitive.

### 4.5 Durability Modes

`DurabilityMode::FullSync`:

- Writer waits until `durable_lsn >= end_lsn`.
- Then flips Pending entries to Committed and publishes through
  `PublishSequencer`.

`DurabilityMode::Interval(duration)`:

- Writer waits until the log record is fully written and `ready_lsn >= end_lsn`.
- Writer may publish before `durable_lsn` catches up, but only in global publish
  order.
- A periodic sync calls `ensure_sync(ready_lsn())` and advances `durable_lsn`.
- A checkpoint or flush of a page above `durable_lsn` must force sync first.
- Reopen recovers only complete valid records through the synced/persisted
  frontier visible to the filesystem.

`DurabilityMode::None`:

- Writer waits until the log record is fully written and `ready_lsn >= end_lsn`.
- It does not wait for sync during commit.
- A checkpoint or dirty-page flush still must force sync through the page LSN
  before materializing that page to the main file.

### 4.6 CRUD Commit Flow

Ordinary CRUD keeps the current body, page-latch, Pending install, and global
publish-sequencer model. The durable envelope changes:

```text
checkpoint admission
metadata/catalog identity capture
write body
captured identity revalidation
publish_slot = PublishSequencer::register_with_oracle()
draft = LogRecordDraft::crud(
  logical ops + chain payload,
  publish_slot.publish_seq,
  publish_slot.commit_ts
)
log_slot = LogManager::reserve(draft.encoded_len())
record = draft.finalize(log_slot.start_lsn)
install Pending secondary deltas stamped Dirty { last_lsn: record.end_lsn }
install Pending primary deltas stamped Dirty { last_lsn: record.end_lsn }
LogManager::write_reserved(&log_slot, record.bytes())
receipt = LogManager::mark_written(&log_slot)
wait_ready or wait_durable according to durability mode
flip Pending -> Committed
PublishSequencer::mark_ready(publish_slot)
```

No durable record is written before the publish slot is registered, because the
record carries the allocated `publish_seq` and `commit_ts`.
`PublishSlotGuard` must expose both values to the commit-log encoder. If a
writer crashes after a complete record is written but before live publish,
recovery treats the durable record as authoritative; the in-memory abort/ready
state is not durable without a log record saying so.

Failures before reservation abort Pending state and the publish slot. After
`reserve` returns, every error before `mark_written` is fatal via
`poison_slot`; no live release, reuse, skip, or rollback of the reserved LSN
range is allowed.

### 4.7 DDL, Index, And Checkpoint Records

All DDL, index, and checkpoint journal users must move to the same log manager.
It is not acceptable to leave a second production journal model for:

- namespace create/drop
- index reserve/build/commit/cleanup/drop
- catalog generation publish records
- checkpoint boundary records that participate in recovery authority

`CatalogCommit` payloads are tagged catalog page-image commits. The outer
`LogRecord.publish_seq` is the only publish-order authority. Each catalog
variant carries `catalog_generation_before`, `catalog_generation_after`, the
post-commit file header, and the catalog page images needed to restore that
post-commit catalog state. The variant kind records which lifecycle operation
produced the image; Phase 8 recovery does not maintain a second per-variant
identity replay format.

Recovery validates the log envelope, payload codec, duplicate publish sequence,
and catalog generation ordering, then applies accepted `CatalogCommit` records
in `publish_seq` order by overlaying the persisted header/catalog page images.
Replaying the same valid record is idempotent because it writes the same
post-image. Mismatched or corrupt payloads are rejected through the shared log
CRC/length/kind checks and catalog/header decoding rather than through
story-local typed identity fields. A later phase may add semantic per-variant
identity validation, but Phase 8's production authority is the ordered
page-image record.

`CheckpointBoundary` payloads contain `checkpoint_applied_lsn`,
`catalog_root_page`, `catalog_root_level`, `history_store_root_page`,
`history_store_root_level`, file-header generation/checksum fields, and the
main-file header generation written by the checkpoint. Recovery skips replay of
records with `end_lsn <= checkpoint_applied_lsn` only after the main header
checksum proves that checkpoint boundary was durably applied.

DDL may still serialize through `metadata.write()` and namespace barriers. The
requirement here is one journal/recovery model, not DDL parallelism.

### 4.8 Page `last_lsn` Fence

Commit no longer calls global `handle.flush()`.

Every dirty page produced by a committed operation must carry:

```rust
enum PageDirtyLsn {
    Clean,
    Unflushable,
    Dirty { last_lsn: u64 },
}
```

The implementation may encode this as an atomic integer plus sentinels, but the
semantic contract is the enum above. Transition from `Unflushable` to
`Dirty { last_lsn }` is a Release store; flush paths load with Acquire. Dirty
state created before the commit record's `end_lsn` is known must be marked
`Unflushable`. Once the LSN is known, the page is stamped with that `end_lsn`.
Checkpoint/reconcile/main-file flush may write a dirty page only after an
Acquire load proves:

```text
page.last_lsn <= durable_lsn
```

If a page is `Unflushable`, the flush path must skip it. If
`page.last_lsn > durable_lsn`, the flush path may either skip the page or force
log sync through `page.last_lsn`; it must not write the page first. A checkpoint
may materialize only pages with `page.last_lsn <= checkpoint_applied_lsn`, even
when `page.last_lsn <= durable_lsn`; otherwise it must advance the selected
checkpoint frontier or skip the page. This prevents the main file from becoming
newer than the durable log or newer than its recorded checkpoint frontier.

### 4.9 Recovery And Log Lifecycle

Open recovery:

1. Read the main file header and its `checkpoint_applied_lsn`.
2. Read the log header and validate the Phase 8 format. The log header contains
   `log_id`, `format_version`, `first_record_lsn`, and `next_lsn_hint`; Phase 8
   requires `first_record_lsn` to equal the fixed log-header length because
   valid prefix recycling is out of scope.
3. Scan forward from `first_record_lsn`.
4. Accept only complete outer records with valid CRCs and matching
   `start_lsn/end_lsn`.
5. Stop at the first invalid, incomplete, torn, or unsupported record.
6. Call the recovery-only tail function, named distinctly from the old live
   rollback API, to truncate to the valid end LSN.
7. Set `next_lsn = ready_lsn = durable_lsn = valid_end_lsn` for the reopened
   process.
8. Discard accepted records whose `end_lsn <= checkpoint_applied_lsn`.
9. Remove `CheckpointBoundary` control records from the apply set.
10. Sort the remaining accepted records by `publish_seq`; duplicate nonzero
   `publish_seq` values are corruption, and gaps are allowed.
11. Rebuild logical frame, catalog, chain/refcount, and page-write effects from
   typed payloads.
12. Seed the HLC floor and next publish sequence from the maximum accepted
    non-control `commit_ts` and `publish_seq`. The live
    `PublishSequencer` starts fresh above that floor; dense old publish-slot
    state is not reconstructed.

Checkpoint lifecycle:

1. A checkpoint selects `checkpoint_applied_lsn <= durable_lsn`.
2. It materializes only pages with `page.last_lsn <= checkpoint_applied_lsn`.
   A page with `checkpoint_applied_lsn < last_lsn <= durable_lsn` must be
   skipped or force the checkpoint to choose a later frontier before it is
   written.
3. It writes the main file/header with `checkpoint_applied_lsn` and fsyncs the
   main file/header.
4. It writes a `CheckpointBoundary` log record and syncs the log.
5. Phase 8 does not recycle or trim valid prefix log bytes. The boundary lets
   recovery skip already materialized records while preserving byte-offset LSNs.

Recovery must not replay bytes after a torn gap, and live code must not use the
recovery truncation helper as transaction rollback.

---

## 5. File Map

Primary files:

- `src/journal/log_file.rs`
- `src/journal/mod.rs`
- `src/journal/recovery.rs`
- `src/journal/shm.rs`
- `src/storage/file_io.rs`
- `src/storage/handle.rs`
- `src/storage/lock.rs`
- `src/storage/paged_engine.rs`
- `src/storage/paged_engine/group_commit.rs`
- `src/storage/paged_engine/state.rs`
- `src/storage/paged_engine/index_build.rs`
- `src/storage/paged_engine/catalog_ops.rs`
- `src/storage/paged_engine/snapshot_ops.rs`
- `src/storage/paged_engine/recovery_apply.rs`
- `src/storage/paged_engine/publish_sequencer.rs`
- `src/storage/buffer_pool/mod.rs`
- `src/storage/buffer_pool/partition.rs`
- `src/storage/buffer_pool/page_latch.rs`
- `src/client/open.rs`
- `src/client/test_accessors.rs`
- `src/options.rs`
- `src/error.rs`

Test/verification files:

- `src/journal/mod_tests.rs`
- `src/journal/tests/*`
- `src/storage/paged_engine/tests/*`
- `tests/mwmr_crash_recovery.rs`
- `tests/phase8_journal_group_commit.rs`
- `benches/group_commit_lsn.rs`
- `scripts/verify-phase8-cleanup.sh`

---

## 6. Story Graph

The canonical story graph is `.omc/phase-08-prd.json`. This section is a prose
mirror for readers.

1. `US-001` — log record envelope, typed payloads, flags, and codec.
2. `US-002` — `LogManager` slot reservation and positioned writes.
3. `US-003` — LSN recovery scanner and HLC/frontier seeding.
4. `US-004` — LSN high-water group commit and durability modes.
5. `US-006` — page `last_lsn` dirty-state fence.
6. `US-005` — CRUD commit flow integration after the page fence exists.
7. `US-007` — DDL/index/checkpoint users converted to the new log.
8. `US-008` — delete legacy journal mutex, tail rollback, old append APIs, and
   obsolete probes/docs.
9. `US-009` — crash/concurrency/benchmark verification matrix.
10. `US-010` — final Ralph readiness gate.

Stories must remain independently reviewable. If a story grows to combine a
codec change, engine integration, and recovery verification in one patch, split
it before Ralph executes it.

---

## 7. Test Matrix

Required focused tests:

- Codec rejects truncated, oversize, bad CRC, bad LSN, unknown kind, unknown
  flag, and mismatched length records.
- Concurrent reservations return disjoint monotonic LSN ranges.
- A failed post-reservation writer poisons the live engine and prevents
  `ready_lsn` from skipping the gap.
- `ready_lsn` stops at the first unwritten or failed slot.
- `durable_lsn` advances only after sync.
- FullSync four-writer cohort performs one sync and releases all covered LSNs.
- Late writer after cohort close requires a later sync.
- Sync failure poisons waiters and publishes no uncertain FullSync member.
- Interval sync failure poisons the live engine; reopen recovery decides which
  already-visible interval writes survived.
- Crash cuts before reservation, after reservation before dirty install, after
  dirty install before write, mid-record, after write before sync, after sync
  before Pending flip, after Pending flip before publish, and post-publish.
- Recovery ignores torn tail and replays exactly the valid committed prefix.
- Recovery applies accepted records by persisted `publish_seq`, not by LSN
  reservation order, and covers an earlier publish slot stalling before reserve
  while a later writer completes first.
- Checkpoint refuses to flush pages with `last_lsn > durable_lsn` unless it
  first forces log sync through that LSN.
- Checkpoint skips or advances when `checkpoint_applied_lsn < page.last_lsn <=
  durable_lsn`; it must not write that page under the lower checkpoint frontier.
- DDL and index paths leave no production call sites of the old journal model.
- Named `phase8_*` tests are listed by `cargo test -- --list`; a zero-test
  filter is not acceptable evidence.

Required command gates:

```bash
jq empty .omc/phase-08-prd.json
cargo fmt --check
git diff --check
cargo test --release --features test-hooks --lib journal
cargo test --release --features test-hooks --test mwmr_crash_recovery
cargo test --release --features test-hooks --test phase8_journal_group_commit -- --list
cargo test --release --features test-hooks --test phase8_journal_group_commit
cargo test --release --tests
scripts/verify-phase8-cleanup.sh
cargo bench --bench group_commit_lsn
```

The benchmark gate must record fsync amortization evidence. Passing requires
`fsyncs_per_commit` at four `FullSync` writers to be no more than `0.50x` the
single-writer value. The benchmark must fail when this threshold is missed and
must write `.omc/artifacts/phase8-bench.json` with the measured values.

---

## 8. Done Definition

Phase 8 is complete only when:

1. `.omc/phase-08-prd.json` has every story `passes:true`.
2. No production CRUD, DDL, index, or checkpoint path uses live
   `journal_mutex` serialization for commit durability.
3. No production commit cleanup path uses `begin_txn`, `rollback_txn`, or live
   `truncate_to(mark)`.
4. `GroupCommitManager` is replaced by an LSN high-water mechanism.
5. `FullSync` publish is gated by `durable_lsn >= end_lsn`.
6. Commit-time `handle.flush()` is gone from ordinary CRUD and DDL durability.
7. Dirty main-file writes are fenced by page `last_lsn <= durable_lsn`.
8. Recovery replays only complete CRC-valid records and stops at the first torn
   record.
9. Phase 8 checkpoint handling skips already materialized records by
   `checkpoint_applied_lsn` without physically trimming valid prefix log bytes.
10. The crash-cut and concurrency tests named in §7 pass.
11. The benchmark evidence shows FullSync fsync amortization across concurrent
    writers.

---

## 9. Anti-Goals

- Do not preserve old journal compatibility.
- Do not introduce a second DDL-only production journal.
- Do not make publish order per namespace.
- Do not hide recovery uncertainty by aborting a post-sync-failure writer.
- Do not flush global dirty page state from the commit path.
- Do not claim WT-like behavior while LSN reservation still depends on a shared
  `seek` cursor.

---

## 10. Follow-Up TODO

- Close the destructive DDL object-lifetime gap: block new readers from opening
  an epoch that still references a namespace/index whose pages are about to be
  freed, or defer physical free until the reader horizon has advanced beyond the
  retired catalog epoch. This should cover `drop_namespace`, index teardown, and
  any future tree-page reclamation path that can invalidate roots visible in an
  older `PublishedEpoch`.
