# Scalability Analysis

## Summary

mqlite's scalability profile is fundamentally different from server databases: it scales down (IoT devices with 4MB RAM, SD cards, battery-powered sensors) rather than up (clusters, sharding, distributed writes). The ceiling is one writer, many readers, one file, one machine. Within that envelope, the critical scaling dimensions are: (1) concurrent read throughput under WAL-based snapshot isolation, (2) write latency as a function of durability mode and document size, (3) database file size growth and its impact on B+ tree depth and query performance, and (4) WAL checkpoint overhead versus recovery time. The single-writer / multiple-reader (SWMR) architecture — modeled directly on SQLite's WAL mode — is the confirmed concurrency model, with writer contention resolved by blocking until the lock is available (configurable timeout, default 5 seconds).

The most consequential scaling trade-off is durability versus write throughput. FullSync mode (fsync per commit) limits writes to ~100-500/s on spinning disk, ~2,000-10,000/s on SSD. Interval mode (periodic flush, e.g., every 100ms) enables ~50,000-200,000 writes/s but creates a loss window. In-memory mode removes I/O entirely. The buffer pool is the primary lever for read performance — a well-sized pool (64MB default) keeps the B+ tree's internal nodes resident, making most indexed lookups a single leaf-page I/O. For edge/IoT deployments, the minimum viable configuration is ~2MB buffer pool, and the library must handle disk-full and power-loss gracefully without corruption. Variable page sizes (4KB internal / 32KB leaf) improve space utilization for MongoDB's typically large documents but increase write amplification compared to uniform small pages.

## Analysis

### Key Considerations

- **Single-writer is the hard ceiling.** All writes serialize through one lock. This is inherent to the WAL design and cannot be lifted without a fundamentally different architecture (MVCC with multiple writers). For embedded use cases, this is acceptable — SQLite proves the model works for millions of deployments.
- **Read concurrency scales with reader count.** Each reader operates on a consistent snapshot (WAL-based MVCC). Readers never block writers and writers never block readers. The practical limit is the `max_readers` configuration (default 64), bounded by SHM slots and file descriptor count.
- **WAL growth is the primary write-side scaling concern.** Under sustained write load, the WAL grows until checkpoint occurs. A 100MB WAL (default max before forced checkpoint) contains ~3,000 32KB leaf pages. Checkpoint must copy all of these to the main file, creating a latency spike.
- **B+ tree depth determines lookup cost.** With 4KB internal nodes (~150 keys each) and 32KB leaf nodes, a 3-level tree indexes ~3.3 million leaf pages (~100GB). A 4-level tree handles ~500 million leaves (~15TB). Depth increases are rare but each adds one page read per lookup.
- **Buffer pool hit rate dominates read performance.** If internal nodes are cached (they're small and frequently accessed), an indexed point lookup requires exactly one leaf-page I/O. The working set for internal nodes of a 10GB database is ~2-5MB.
- **Document size affects everything.** A 16MB document spans ~500 overflow pages. Insert, update, and delete of large documents are proportionally expensive. The average MongoDB document is 1-5KB, fitting comfortably in a 32KB leaf page.
- **Disk I/O is the bottleneck, not CPU.** BSON parsing, key comparison, and B+ tree traversal are fast. Page reads from disk (especially on SD cards: 1-5ms random read) dominate latency. This makes buffer pool sizing the single most impactful performance knob.
- **Write amplification compounds.** A single document insert touches: WAL (write page image), and later during checkpoint: main file (copy page). If the insert causes a page split, two pages are written. If an index is updated, additional pages are written. Total write amplification for an indexed insert is typically 2-4x the document size.

### Options Explored

#### Option 1: Single-Threaded with Cooperative Scheduling

- **Description**: No concurrency. All reads and writes happen on a single thread. Callers block while their operation completes. No WAL, no locking, no shared memory.
- **Pros**: Simplest possible implementation. No concurrency bugs. No locking overhead. Suitable for single-threaded CLI tools.
- **Cons**: No concurrent reads. A long-running scan blocks all other operations including writes. Cannot serve wire protocol clients concurrently. Incompatible with the multi-reader requirement.
- **Effort**: Low.

#### Option 2: SWMR with WAL — SQLite Model (Recommended)

- **Description**: Single writer, multiple readers via write-ahead logging. Readers see a consistent snapshot without blocking the writer. Writer appends to WAL; periodic checkpoint merges WAL to main file. Multi-process access via POSIX fcntl file locking and shared memory.
- **Pros**: Proven model (SQLite serves billions of deployments). Well-understood correctness properties. Readers never block. Writer throughput limited only by disk I/O. Multi-process safe. Clean single-file-when-closed semantics.
- **Cons**: Single writer is a hard ceiling. Checkpoint creates periodic latency spikes. WAL file grows under sustained writes. Shared memory file adds complexity. More complex than single-threaded.
- **Effort**: Medium-High (WAL correctness is hard).

#### Option 3: MVCC with Multiple Writers

- **Description**: Multiple writers with multi-version concurrency control. Each transaction sees a consistent snapshot. Conflict detection at commit time. Similar to PostgreSQL's model.
- **Pros**: Multiple concurrent writers. Higher write throughput potential. No writer blocking.
- **Cons**: Dramatically more complex. Conflict detection and resolution adds overhead. Version chain management (garbage collection of old versions) is a significant subsystem. Not justified for embedded use cases where write contention is rare. Would require a completely different storage engine design.
- **Effort**: Very High.

#### Option 4: Sharded Writer with Per-Collection Locks

- **Description**: Each collection has its own writer lock. Writes to different collections can proceed concurrently.
- **Pros**: Higher write throughput for multi-collection workloads.
- **Cons**: Cross-collection operations (e.g., future multi-document transactions) become very complex. The single-file model means all collections share the same page allocator and free list, which still requires global coordination. Marginal benefit for typical embedded workloads.
- **Effort**: High.

### Recommendation

**Option 2: SWMR with WAL (SQLite model).** This is confirmed by the project design and human answers. Implementation priorities:

1. **WAL implementation**: Page-level redo log with commit markers. CRC32C checksums per frame. Salt-based association with main file to detect stale WAL files.
2. **Shared memory**: Hash table mapping page numbers to WAL frame offsets. Reader snapshot tracking (up to 64 concurrent readers). Writer lock via PID in SHM.
3. **Multi-process locking**: POSIX `fcntl(F_SETLK)` for writer exclusion. `flock` for SHM coordination. Follow SQLite's locking protocol exactly where applicable.
4. **Checkpoint**: Configurable auto-checkpoint threshold (default 1000 pages). Forced checkpoint at WAL size limit (default 100MB). Non-blocking checkpoint (runs between write operations).
5. **Writer blocking**: Acquire writer lock with `busy_timeout` (default 5 seconds). Return `Error::WriterBusy` on timeout.

## Concurrency Model

```
                    ┌─────────────────────────────────────┐
                    │           .mqlite (main file)        │
                    │  ┌─────────────────────────────────┐ │
                    │  │   Pages: [0][1][2]...[N]        │ │
                    │  └─────────────────────────────────┘ │
                    └──────────────┬──────────────────────┘
                                   │ checkpoint copies
                    ┌──────────────▼──────────────────────┐
                    │         .mqlite-wal (WAL file)       │
                    │  [Header][Frame1][Frame2]...[FrameN] │
                    │  Writer appends new page images here │
                    └──────────────┬──────────────────────┘
                                   │ indexed by
                    ┌──────────────▼──────────────────────┐
                    │         .mqlite-shm (shared memory)  │
                    │  [WriterLock][ReaderSlots][WAL Index] │
                    │  Hash: page_number → WAL offset      │
                    └─────────────────────────────────────┘

  Writer (1 max)              Readers (up to 64)
  ┌─────────┐                ┌─────────┐ ┌─────────┐
  │ Acquire │                │ Snapshot │ │ Snapshot │
  │ writer  │                │ at WAL   │ │ at WAL   │
  │ lock    │                │ frame N  │ │ frame M  │
  │ (block) │                │          │ │          │
  │         │                │ Read:    │ │ Read:    │
  │ Write:  │                │ 1. Check │ │ 1. Check │
  │ Append  │                │    WAL   │ │    WAL   │
  │ to WAL  │                │ 2. Fall  │ │ 2. Fall  │
  │         │                │    back  │ │    back  │
  │ Release │                │    to    │ │    to    │
  │ lock    │                │    main  │ │    main  │
  └─────────┘                └─────────┘ └─────────┘
```

### Read Path

1. Reader acquires a snapshot by recording the current WAL end position in its SHM reader slot.
2. For each page needed: check WAL index for the page (up to snapshot position). If found, read from WAL. If not, read from main file.
3. Reader releases snapshot by clearing its SHM slot. This allows checkpoint to proceed past that point.

### Write Path

1. Writer acquires the writer lock (blocking, with timeout).
2. Modifications produce new page images.
3. Page images are appended to the WAL with a commit frame marker.
4. WAL index in SHM is updated.
5. Writer lock is released.
6. If auto-checkpoint threshold is reached, checkpoint runs (can be deferred to a background operation).

### Checkpoint

1. Determine the oldest reader snapshot position.
2. Copy WAL frames up to that position to the main file.
3. Update the WAL start position.
4. If all readers have advanced past the WAL start, truncate/reset the WAL.

Checkpoint does NOT block readers or writers. It runs concurrently with reads. Writers may need to wait briefly if checkpoint is updating the WAL index, but this is sub-millisecond.

## Performance Targets

| Operation | Target Latency | Target Throughput | Notes |
|-----------|---------------|-------------------|-------|
| Point lookup by _id (cached) | < 10 us | > 100,000/s | Buffer pool hit, no disk I/O |
| Point lookup by _id (uncached) | < 1 ms (SSD) / < 5 ms (HDD) | > 1,000/s (SSD) | One leaf page read |
| Indexed range scan (100 docs) | < 5 ms (cached) | > 10,000 scans/s | Sequential leaf reads |
| Collection scan (10K docs) | < 50 ms (cached) | Depends on doc size | Sequential I/O |
| Insert single doc (FullSync) | < 2 ms (SSD) | 500-2,000/s | Dominated by fsync |
| Insert single doc (Interval) | < 100 us | 10,000-50,000/s | WAL append only |
| Insert single doc (In-memory) | < 10 us | > 100,000/s | No I/O |
| Bulk insert 10K docs (Interval) | < 500 ms | 20,000+ docs/s | Batched WAL writes |
| Index creation (100K docs) | < 5 s | N/A | Full collection scan + sort |

These are order-of-magnitude targets for Phase 1. SQLite achieves similar numbers for comparable operations, validating feasibility.

### Memory Footprint Targets

| Deployment | Buffer Pool | Total mqlite RSS | Notes |
|------------|------------|------------------|-------|
| IoT/Edge (Raspberry Pi) | 2-4 MB | 5-10 MB | Minimal, constrained device |
| CLI Tool | 16-32 MB | 20-40 MB | Short-lived, moderate data |
| Desktop App (default) | 64 MB | 80-100 MB | Long-running, typical use |
| Server (wire protocol) | 128-256 MB | 150-300 MB | Many concurrent readers |

## Resource Limits and Configuration

| Resource | Default | Min | Max | Configuration |
|----------|---------|-----|-----|--------------|
| Buffer pool size | 64 MB | 512 KB | No limit | `OpenOptions::buffer_pool_size()` |
| Max concurrent readers | 64 | 1 | 64 (**Phase 1 hard limit**) | `OpenOptions::max_readers()` |
| WAL auto-checkpoint | 1000 pages | 100 | No limit | `OpenOptions::wal_auto_checkpoint()` |
| WAL max size (forced checkpoint) | 100 MB | 10 MB | No limit | `OpenOptions::wal_max_size()` |
| Busy timeout (writer lock) | 5 seconds | 0 (immediate fail) | No limit | `OpenOptions::busy_timeout()` |
| Max document size | 16 MB | N/A (fixed) | 16 MB | Not configurable (MongoDB compat) |
| Cursor batch size | 101 | 1 | 10,000 | `FindOptions::batch_size()` |
| Max cursor idle time | 600 s (10 min) | 10 s | No limit | Wire protocol only |
| Max active cursors | 1,000 | 10 | No limit | Configurable |
| BSON nesting depth | 100 | N/A (fixed) | 100 | Not configurable (safety limit) |

### File Descriptor Usage

| Component | FDs per instance | Notes |
|-----------|-----------------|-------|
| Main .mqlite file | 1 | Always open |
| WAL file | 1 | Open during operation |
| SHM file | 1 | Open during operation, mmap'd |
| Wire protocol | 1 per connection | TCP sockets |
| **Total (no wire)** | **3** | Minimal |
| **Total (wire, 10 clients)** | **13** | Moderate |

## WAL Scaling Characteristics

### WAL Growth Rate

Under sustained write load:
- Each write operation appends one or more page images to the WAL.
- A single indexed document insert writes: ~1 leaf page (32KB) + ~1 index leaf page (32KB) + potential internal page updates (4KB each).
- Worst case per insert: ~70KB WAL growth (including frame headers).
- At 10,000 inserts/s (Interval mode): WAL grows at ~700MB/s → hits 100MB forced checkpoint in ~150ms.

### Checkpoint Cost

| WAL Size | Pages to Copy | Checkpoint Time (SSD) | Checkpoint Time (HDD) |
|----------|--------------|----------------------|----------------------|
| 10 MB | ~300 | ~5 ms | ~50 ms |
| 50 MB | ~1,500 | ~25 ms | ~250 ms |
| 100 MB | ~3,000 | ~50 ms | ~500 ms |

Checkpoint time is dominated by sequential disk writes. SSDs handle this well. HDDs see significant latency at larger WAL sizes.

### Recovery Time

On crash, WAL replay time is proportional to WAL size:
- WAL replay reads frames sequentially and applies to main file.
- 100 MB WAL: ~100-500 ms recovery time.
- Minimal data loss: only uncommitted transactions (no commit frame) are lost. Committed data in WAL is guaranteed recoverable.

## Database Size Limits

### Theoretical Limits

| Metric | 32-bit Page Numbers | Notes |
|--------|-------------------|-------|
| Max 4KB pages | 4,294,967,296 | 16 TB of internal nodes |
| Max 32KB pages | 4,294,967,296 | 128 TB of leaf/data pages |
| Max file size | ~128 TB | If all pages are 32KB leaves |
| Practical limit (Phase 1) | ~1 TB | Beyond this, consider a real database |

### B+ Tree Depth Analysis

With 4KB internal nodes (~150 keys) and 32KB leaf nodes:

| Documents | Avg Doc Size | Leaf Pages | Tree Depth | Internal Page I/O per Lookup |
|-----------|-------------|------------|------------|------------------------------|
| 1,000 | 1 KB | ~33 | 2 | 1 |
| 100,000 | 1 KB | ~3,300 | 3 | 2 |
| 10,000,000 | 1 KB | ~330,000 | 3 | 2 |
| 100,000,000 | 1 KB | ~3,300,000 | 4 | 3 |
| 1,000,000 | 10 KB | ~330,000 | 3 | 2 |

The B+ tree stays shallow. For databases under 100 million documents, depth is 3 (2 internal page reads per lookup). With internal nodes cached in the buffer pool, most lookups require only 1 leaf page I/O.

### Performance Degradation

| Database Size | Buffer Pool Hit Rate (64MB) | Lookup Latency | Notes |
|---------------|---------------------------|----------------|-------|
| < 64 MB | ~100% | < 10 us | Fully cached |
| 100 MB | ~60% | < 500 us | Internal nodes cached, some leaf misses |
| 1 GB | ~6% | ~1 ms | Internal nodes cached, most leaves from disk |
| 10 GB | < 1% | ~2-5 ms | Working set exceeds cache; increase buffer pool |
| 100 GB | < 0.1% | ~5-10 ms | Disk-bound; need 256MB+ buffer pool |

## Edge/IoT Scaling Constraints

### Minimum Viable Configuration

| Resource | Minimum | Notes |
|----------|---------|-------|
| RAM | 2 MB buffer pool + 3 MB overhead | 5 MB total |
| Storage | 64 KB (empty DB) | Grows with data |
| CPU | Single core, ARMv7+ | No SIMD required |
| File system | POSIX-compatible | fcntl locking support required |

### Flash/SD Card Considerations

- **Write amplification matters more on flash.** Each WAL write + checkpoint copy = 2x write amplification minimum. Flash cells have limited write cycles (10,000-100,000 for MLC).
- **Wear leveling**: Modern SD cards handle wear leveling internally. mqlite should not attempt to optimize for specific flash characteristics.
- **Sequential writes preferred**: WAL is append-only (sequential). Checkpoint writes are semi-sequential (page-order). This is favorable for flash.
- **Avoid frequent fsync**: FullSync mode on flash cards can be very slow (50-200ms per fsync). Interval mode (100ms flush) batches writes, reducing fsync count and extending flash life.
- **File size monitoring**: `db.stats().file_size` lets applications monitor growth and take action before the SD card fills up.

### Power Loss Handling

1. **Clean state**: All committed data in WAL is recoverable. Uncommitted writes are lost (correct behavior).
2. **On next open**: mqlite detects WAL file, replays committed frames, discards partial frames (verified by checksum).
3. **Torn pages**: CRC32C per page detects partial writes from power loss mid-page. Torn pages in WAL are discarded (they're after the last commit frame). Torn pages in main file are recovered from WAL.
4. **SHM file**: Stale SHM from crash is detected (salt mismatch or lock state) and rebuilt from WAL scan.

### Disk-Full Behavior

1. Write operation attempts to extend file or append to WAL.
2. OS returns ENOSPC.
3. mqlite returns `Error::DiskFull { path, required_bytes, available_bytes }`.
4. Database remains readable. No corruption occurs.
5. Application can delete data, free disk space, and retry writes.
6. Checkpoint may help: if WAL has pages that overwrite existing main file pages, checkpoint reclaims WAL space without growing the main file.

## Write Amplification Analysis

| Operation | WAL Write | Checkpoint Write | Total Write Amp | Notes |
|-----------|----------|-----------------|-----------------|-------|
| Insert 1KB doc (no index) | 32 KB (1 leaf page) | 32 KB | 64x | Worst case: nearly empty leaf |
| Insert 1KB doc (1 index) | 64 KB (2 leaf pages) | 64 KB | 128x | Data page + index page |
| Insert 1KB doc (leaf full, split) | 96 KB (3 pages) | 96 KB | 192x | Split creates new leaf + updates parent |
| Update 1KB doc in-place | 32 KB | 32 KB | 64x | Rewrites entire leaf page |
| Delete 1KB doc | 32 KB | 32 KB | 64x | Rewrites leaf page |
| Bulk insert 1000 x 1KB | ~32 MB | ~32 MB | ~64x avg | Amortized over page fills |

Write amplification is high at the page level (32KB page for 1KB document). This is the cost of the variable-page design — it optimizes for larger documents. For 10KB documents, amplification drops to ~6x. For very small documents (< 100 bytes), consider batching inserts to fill pages efficiently.

### Comparison with SQLite

| Metric | mqlite (projected) | SQLite (WAL mode) |
|--------|-------------------|-------------------|
| Page size | 4KB/32KB variable | 4KB uniform |
| Write amp (1KB doc) | ~64x | ~8x (4KB page) |
| Write amp (10KB doc) | ~6x | ~16x (3 overflow pages) |
| Checkpoint overhead | Higher (32KB pages) | Lower (4KB pages) |
| Read amp (scan) | Lower (fewer pages) | Higher (more pages) |
| Buffer pool efficiency | Better for large docs | Better for small records |

mqlite trades higher write amplification for small documents in exchange for better read performance and lower overhead for large documents.

## Durability vs Performance Trade-offs

| Mode | Write Latency | Throughput | Data Loss Window | Use Case |
|------|--------------|------------|------------------|----------|
| **FullSync** | 1-5 ms (SSD) / 10-50 ms (HDD) | 200-1,000/s (SSD) | Zero (after API returns) | Financial data, IoT sensors |
| **Interval(100ms)** | < 100 us | 10,000-100,000/s | Up to 100ms of writes | General applications |
| **Interval(1s)** | < 100 us | 10,000-100,000/s | Up to 1s of writes | High-throughput logging |
| **None** | < 10 us | 100,000+/s | All uncommitted writes | In-memory mode, testing |

### FullSync Mode Detail

Every commit calls `fsync(wal_fd)`. This guarantees that once `insert_one()` returns `Ok`, the data survives power loss. Cost: 1 fsync per write operation. SSDs handle this at ~2,000-10,000 fsync/s. HDDs: ~100-200 fsync/s.

Optimization: batch multiple writes into a single transaction. 100 inserts in one transaction = 1 fsync.

### Interval Mode Detail

WAL writes are buffered in OS page cache. A background timer calls `fsync(wal_fd)` at the configured interval. Writes return as soon as the WAL frame is in the page cache (microseconds). Risk: power loss before the next fsync loses up to `interval` worth of committed writes.

This is the recommended default for most applications. The 100ms default means at most 100ms of data loss — acceptable for the vast majority of embedded use cases.

## Constraints Identified

1. **Single writer is a hard ceiling.** No amount of optimization changes this. Applications with sustained high write concurrency should not use mqlite.

2. **WAL checkpoint creates latency spikes.** A 100MB WAL checkpoint takes ~50ms on SSD. Applications sensitive to tail latency should tune `wal_auto_checkpoint` lower (more frequent, smaller checkpoints).

3. **32KB leaf pages cause high write amplification for small documents.** A 100-byte document update rewrites a 32KB page. This is the trade-off of optimizing for MongoDB's larger document sizes.

4. **Buffer pool sizing requires tuning.** The default 64MB is reasonable for desktop apps but may be too large for IoT and too small for server use. Application developers must size this for their deployment.

5. **Multi-process access depends on correct file locking.** POSIX `fcntl` is reliable on local filesystems. Network filesystems (NFS, SMB) may not support it correctly. Document: "mqlite on network filesystems is unsupported."
   - **macOS-specific fcntl behavior**: macOS differs from Linux in several ways: (a) `fcntl` locks are inherited by child processes after `fork()`, unlike Linux; (b) lock release behavior differs when a locked thread exits without releasing; (c) macOS has a ~10K advisory lock limit that can cause silent failures under load. Reference SQLite's `os_unix.c` VFS implementation for macOS workarounds before writing the locking protocol. Test multi-process access on both Linux and macOS in CI.
   - **WAL design fallback**: If the SHM hash-table-based WAL index proves unworkable on target platforms, fall back to linear WAL scan for readers. Instead of the SHM hash table, readers scan the WAL file linearly to find the latest committed frame for each page they need. Performance is O(WAL frames) per page read but requires no SHM complexity. Acceptable for Phase 1 if WAL is aggressively checkpointed (e.g., every 100 pages). Define decision trigger: if SHM-based WAL index is not working correctly on both Linux and macOS after 2 weeks of implementation, switch to linear WAL scan for Phase 1.

6. **Flash storage life is affected by write amplification.** The 64x write amplification for small documents means 1GB of logical writes = 64GB of physical writes. On consumer SD cards (100TB write endurance), this limits lifetime.

7. **No read replicas or read scaling beyond one machine.** mqlite is single-file, single-machine. Read scaling is limited by disk bandwidth and buffer pool size.

8. **Cursor memory scales with active query count.** Each open cursor holds a read snapshot reference, preventing WAL truncation past that point. Many idle cursors can cause WAL growth.

9. **fsync latency is hardware-dependent and unpredictable.** FullSync mode performance varies dramatically across SSDs, HDDs, and SD cards. mqlite cannot control this.

## Open Questions

1. **Should checkpoint be synchronous or asynchronous?** Synchronous: checkpoint happens inline after a write crosses the threshold (simpler, deterministic). Asynchronous: checkpoint runs in a background thread (better write latency, more complex). SQLite supports both. Recommendation: synchronous for Phase 1, async as Phase 2 optimization.

2. **What is the auto-checkpoint page threshold?** Default 1000 pages = ~32MB of WAL data (if all 32KB pages). This triggers every ~30MB of writes. Is this too frequent (overhead) or too infrequent (large recovery time)? Needs benchmarking.

3. **Should mqlite support read-only file mode?** If the .mqlite file is on a read-only filesystem (e.g., IoT device in recovery mode), can mqlite open it for reads without attempting WAL creation? SQLite supports this. Recommendation: yes, via `OpenOptions::read_only(true)`.

4. **How does mqlite handle clock skew in ObjectId generation?** ObjectIds include a timestamp. If the system clock jumps backward, ObjectIds may not be monotonically increasing, affecting _id index locality. This is a MongoDB-wide issue, not mqlite-specific.

5. **Should write batching be exposed in the API?** A `Transaction` or `WriteBatch` API that groups multiple writes into a single WAL commit (one fsync) would dramatically improve FullSync throughput for bulk operations. SQLite's explicit transactions serve this purpose.

6. **What is the maximum number of collections?** Each collection requires catalog entries and at least one B+ tree (the _id index). Thousands of collections are feasible. Millions would stress the catalog. Define a practical limit.

7. **How does buffer pool handle mixed page sizes?** Two separate pools (one for 4KB, one for 32KB) or one pool with variable-size frames? Separate pools are simpler but require ratio tuning. Single pool wastes space (32KB frame for 4KB page).

## Integration Points

### -> API Design
- `OpenOptions` exposes all resource configuration (buffer pool, WAL, busy timeout, durability mode)
- `Database::checkpoint()` and `Database::compact()` are user-facing scaling controls
- `Database::stats()` exposes buffer pool hit rate, WAL size, page counts for monitoring
- `FindOptions::batch_size()` controls memory usage per cursor

### -> Data Model
- Variable page sizes (4KB/32KB) are a data model decision with scaling consequences
- Document size distribution determines write amplification and buffer pool efficiency
- Index count per collection multiplies write cost (each index = additional page writes per document mutation)
- B+ tree depth is a function of data volume and page fan-out

### -> Security
- Resource limits (max readers, max WAL size, max cursors) prevent denial-of-service
- Buffer pool memory cap prevents memory exhaustion of the host process
- Busy timeout prevents indefinite writer starvation

### -> Wire Protocol
- Each wire protocol connection consumes a file descriptor and a reader slot
- Wire protocol cursor idle timeout prevents resource leaks from abandoned clients
- Connection count limits prevent FD exhaustion
- OP_COMPRESSED reduces bandwidth but adds CPU overhead (trade-off depends on deployment)
