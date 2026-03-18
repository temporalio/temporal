# PRD: TemporalFS -- Durable Filesystem for AI Agent Workflows

**Authors:** Temporal Engineering
**Status:** Draft
**Last Updated:** 2026-03-18
**Companion:** [1-Pager](./temporal-fs.md)

---

## Table of Contents

1. [Executive Summary](#executive-summary)
2. [Problem Statement](#problem-statement)
3. [Target Users](#target-users)
4. [Solution Overview](#solution-overview)
5. [Technical Architecture](#technical-architecture)
6. [API Surface](#api-surface)
7. [Storage Architecture](#storage-architecture)
8. [Layered Storage Efficiency](#layered-storage-efficiency)
9. [Consistency and Replay Model](#consistency-and-replay-model)
10. [Temporal Cloud Considerations](#temporal-cloud-considerations)
11. [Repository and Project Structure](#repository-and-project-structure)
12. [Phased Delivery Plan](#phased-delivery-plan)
13. [Success Metrics](#success-metrics)
14. [Risks and Mitigations](#risks-and-mitigations)
15. [Open Questions](#open-questions)

---

## Executive Summary

TemporalFS is a new CHASM Archetype that provides a durable, versioned, replay-safe virtual filesystem as a first-class primitive in Temporal. It enables multiple workflows and activities to share a common file tree with full Temporal guarantees: deterministic replay, multi-cluster replication, and crash recovery. The primary use case is AI agent workloads that need to read, write, and collaborate on files across workflow boundaries.

The system is designed Cloud-first. The FS layer uses the same inode-based storage model as ZeroFS (inodes, directory entries, fixed-size chunks, layered manifests, bloom filters), with a pluggable `Store` interface and two planned backends: PebbleStore (local/OSS) and WalkerStore (direct Walker for Cloud). Walker is being extended with S3 tiered storage ([Walker S3 Tiered Storage](./walker-s3-design.md)) so cold SSTs are stored on S3 — giving WalkerStore effectively unlimited capacity without FS-specific tiering. Billing, quotas, and multi-tenant isolation are first-class concerns.

---

## Problem Statement

### The Gap

AI agents running on Temporal today have no native way to work with files. They face three bad options:

1. **Ephemeral scratch:** Write to local disk in the worker. Files are lost on failure, unavailable to other workflows, and invisible during replay.
2. **External storage with manual coordination:** Use S3/GCS directly. No consistency with workflow state, no replay determinism, no versioning tied to workflow transitions. Developers must build their own sync logic.
3. **Serialize into payloads:** Encode file content as workflow/activity inputs and outputs. Works for small data but explodes payload sizes, prevents random access, and makes multi-file workspaces impractical.

### Why This Matters Now

The AI agent ecosystem is exploding. Every major framework (LangGraph, CrewAI, AutoGen, OpenAI Agents SDK) needs file state for:

- **Code generation:** Agents write, test, and iterate on code files
- **Data processing:** Agents read datasets, produce intermediate results, generate reports
- **Multi-agent collaboration:** Multiple agents work on the same project directory
- **Model checkpointing:** Agents save and restore model state across retries

These workloads are Temporal's fastest-growing segment. Without native file support, customers build fragile workarounds or choose platforms that offer it natively (even without Temporal's durability guarantees).

### What Competitors Offer

- **Replit Agent / Devin / Cursor:** Built-in file systems, but no durability, no replay, no multi-workflow sharing.
- **Modal:** Volume mounts with snapshots, but no workflow-level versioning or replay determinism.
- **Flyte:** Typed artifact storage, but no live filesystem semantics, no concurrent multi-workflow access.

None of them combine a live filesystem with durable execution guarantees. This is Temporal's unique opportunity.

---

## Target Users

### Primary: AI Agent Developers on Temporal

Developers building AI agent systems using Temporal workflows. They need agents to read/write files naturally (code, data, configs) with Temporal's durability guarantees.

**Jobs to be done:**
- Give my AI agent a workspace where it can read and write files
- Share a file workspace across multiple agent workflows
- Recover file state automatically on workflow failure or retry
- See what files my agent produced at any point in its execution

### Secondary: Data Pipeline Engineers

Teams building multi-step data processing pipelines where intermediate results are files (CSVs, Parquet, images, PDFs) that need to be shared across workflow stages.

### Tertiary: Platform Teams

Teams building internal platforms on Temporal who need durable, shared state beyond what workflow state provides.

---

## Solution Overview

TemporalFS is a new CHASM Archetype -- a first-class execution type like Workflow. It provides:

1. **Independent lifecycle:** A TemporalFS execution lives independently of any workflow. It is created, used by many workflows/activities, and eventually archived or deleted.
2. **Shared access:** Multiple workflows and activities can `Open()` the same TemporalFS concurrently for reading and writing.
3. **Versioned state:** Every file mutation is a tracked transition. Any historical state is retrievable by transition number.
4. **Replay determinism:** SDK records which FS transition was observed; replay reads from that exact snapshot.
5. **Efficient storage:** Inode-based with fixed-size chunks in Walker. Cold data automatically tiers to S3 via Walker's S3 tiered storage.

### Access: FUSE Mount

TemporalFS is accessed via a FUSE mount -- a local directory that behaves like a normal filesystem. Unmodified programs (`git`, `python`, `gcc`, etc.) work without changes. The mount connects to the Temporal server; all reads and writes flow through CHASM. This is the single interface for all file access.

```go
// Create a TemporalFS execution -- lives independently, like a Workflow
fsId := temporalfs.Create(ctx, "project-workspace", temporalfs.Options{
    Namespace: "default",
})

// Workflow: orchestrates an AI coding agent
workflow.Execute(ctx) {
    // Activity gets a FUSE mount -- agent and its tools use normal file I/O
    workflow.ExecuteActivity(ctx, func(actCtx context.Context) {
        mountPath := temporalfs.Mount(actCtx, fsId, "/workspace")
        // Any program can read/write files normally:
        //   git clone ... /workspace/repo
        //   python /workspace/repo/train.py
        //   The agent writes output files to /workspace/output/
    })
}

// Activity on a different host can also mount the same FS
activity.Execute(ctx) {
    mountPath := temporalfs.Mount(ctx, fsId, "/workspace")
    // Normal file I/O -- reads see prior writes, new writes are persisted
    os.WriteFile(filepath.Join(mountPath, "data/results.csv"), results, 0644)
}
```

**Why not just NFS?** NFS requires provisioning and managing a separate NFS server, doesn't integrate with Temporal's durability model (no versioning, no replay determinism, no automatic failover), and has no concept of workflow-scoped lifecycle. TemporalFS is zero-infrastructure for the developer -- `Create()` and `Mount()` are all it takes.

TemporalFS state lives server-side in CHASM, not on worker disk. Workers on different hosts all access the same FS execution via RPC. Worker-local caches are a performance optimization; the source of truth is always the server. Temporal handles versioning, persistence, caching, concurrent writes, replay consistency, and multi-cluster replication.

**Workflow read access:** Workflows need read access to TemporalFS for branching decisions (e.g., "if config file contains X, run path A"). The SDK records which FS transition was observed; on replay, reads resolve against that same transition for determinism. Versioning is essential for activity failure rollback (rewind to pre-activity state) and workflow reset.

---

## Technical Architecture

### CHASM Archetype Design

```
TemporalFS Archetype ("temporalfs/filesystem")
│
├── Execution (BusinessID = user-provided workspace name)
│   ├── CHASM Root Component (lightweight: config, lifecycle, mount table only)
│   │   ├── Config           Field[*fspb.FSConfig]        // chunk size, quotas, policies
│   │   ├── MountTable       Field[*fspb.MountTable]      // active mounts and their cursors
│   │   └── Stats            Field[*fspb.FSStats]         // size, file count, inode count
│   │
│   ├── FS Storage (pluggable: PebbleStore / WalkerStore)
│   │   ├── inode/{id}               // inode metadata (type, size, mode, timestamps)
│   │   ├── dir_entry/{dir}/{name}   // directory name -> child inode
│   │   ├── chunk/{inode}/{idx}      // file content in 32KB chunks
│   │   ├── manifest/{T}             // transition diff (changed inodes)
│   │   └── meta/*                   // FS metadata
│   │
│   ├── Tasks
│   │   ├── ManifestCompactionTask   // flatten manifest diff chain
│   │   ├── ChunkGCTask              // delete orphaned chunks
│   │   ├── SnapshotCleanupTask      // remove expired snapshots
│   │   └── QuotaEnforcementTask     // check and enforce storage quotas
│   │
│   └── Lifecycle: Created -> Running -> Archived -> Deleted
```

### Storage Architecture

#### The FS Layer (Definite)

Regardless of how data reaches the storage engine, the FS abstraction layer is the same -- the gap between "key-value store" and "filesystem":

| Component | What It Does | ZeroFS Equivalent |
|-----------|-------------|-------------------|
| **Inode Manager** | Allocate/free inodes, store metadata (type, size, mode, timestamps), manage directory entries | ZeroFS inode layer |
| **Chunk Store** | Read/write/delete/truncate fixed-size 32KB chunks keyed by `(inode_id, chunk_index)` | ZeroFS chunk manager |
| **Transition Manager** | Track inode-level diffs per transition. One manifest key per transition for replay. | No equivalent (ZeroFS has no replay) |
| **Snapshot Index** | Map of transition -> storage snapshot. Enables O(1) time-travel to any version. | ZeroFS checkpoint system |
| **Chunk Cache (worker-side)** | LRU cache on SDK workers for hot chunks. Keyed by `(inode_id, chunk_index)`. | ZeroFS disk cache + in-memory cache |
| **GC / Compaction** | Tombstone-based async GC of deleted inodes and their chunks. | ZeroFS standalone compactor |
| **Mount Manager** | Track active mounts (which workflows/activities are reading/writing). | N/A (ZeroFS is single-client) |
| **Replay Resolver** | Given a workflow's recorded transition T, serve reads from manifest at T. | N/A (ZeroFS has no replay concept) |

#### Pluggable Storage Backend

The FS layer communicates with storage through a `Store` interface. We plan two implementations:

| Backend | Engine | Use Case |
|---------|--------|----------|
| **PebbleStore** | Local Pebble | v1 / OSS / local development |
| **WalkerStore** | Direct Walker (with S3 tiering) | Cloud: full control over key layout, bottomless capacity via Walker S3 tiered storage |

| Aspect | PebbleStore | WalkerStore |
|--------|------------|-------------|
| **Tiered storage** | None (all local) | Walker S3 tiering: cold SSTs (L4+) on S3, hot data on local SSD |
| **Key layout control** | Full | Full |
| **Value size limits** | None | None |
| **Sharding** | None (single node) | Walker sharding |
| **Replication** | None | Walker replication |
| **New infra to build** | Minimal | Walker S3 adapter (see [Walker S3 Tiered Storage](./walker-s3-design.md)) |

**Why not CDS?** CDS's existing tiered storage is purpose-built for workflow history (`HistoryAggregator` + `WARM_TIER_UPLOAD` tasks tightly coupled to the history data model). It does not provide generic KV tiering. With Walker S3 tiering, WalkerStore gets bottomless capacity at the storage engine level — no FS-specific tiering needed. CDS's constraints (key layout, potential value size limits) are drawbacks without offsetting benefits.

#### What Walker Already Provides (= SlateDB Equivalent)

Walker provides the core LSM-tree primitives that ZeroFS gets from SlateDB:

| Primitive | Walker (Pebble) | SlateDB | Status |
|-----------|----------------|---------|--------|
| Memtable (in-memory write buffer) | Built-in | Built-in | Ready |
| SST flush (memtable -> persistent storage) | Built-in (local disk/EBS) | Built-in (S3) | Ready |
| Leveled compaction | Built-in | Built-in | Ready |
| Bloom filters per SST | Built-in | Built-in | Ready |
| WAL for crash recovery | Built-in (local) | Built-in (S3) | Ready |
| Batch writes (atomic) | `pebble.Batch` | `WriteBatch` | Ready |
| Point lookups | `pebble.Get()` | `db.get()` | Ready |
| Range scans / iterators | `pebble.NewIter()` | `db.scan()` | Ready |
| Snapshots (consistent reads) | `pebble.NewSnapshot()` | `db.snapshot()` | Ready |
| Distributed sharding | Walker sharding layer | N/A (single-node) | Ready |

#### FS Key Schema

TemporalFS uses a prefix-based key schema (same design as ZeroFS). The logical schema is the same regardless of backend (PebbleStore uses these keys directly; WalkerStore adds a namespace prefix):

```
Prefix-based keys (same design as ZeroFS):

  0x01  inode/{inode_id:8B}                          -> InodeProto (type, size, mode, timestamps)
  0x02  dir_entry/{dir_inode:8B}/{name}              -> DirEntryProto (child inode_id + cookie)
  0x03  dir_scan/{dir_inode:8B}/{cookie:8B}          -> DirScanProto (name + embedded inode data)
  0x04  meta/{key}                                    -> metadata (config, stats, superblock)
  0x05  snapshot/{snapshot_id}                        -> SnapshotProto (pinned state + refcount)
  0x06  tombstone/{timestamp:8B}/{inode_id:8B}        -> TombstoneProto (GC tracking)
  0x07  manifest/{transition_id:8B}                   -> TransitionDiff (changed inodes)
  0x08  manifest_latest                               -> uint64 (latest transition number)
  ...
  0xFE  chunk/{inode_id:8B}/{chunk_index:8B}          -> raw chunk content (32KB)
```

This key schema enables:
- **Efficient inode lookup:** Point get by inode ID, bloom filter accelerated
- **Efficient directory listing:** Prefix scan on `dir_scan/{dir_inode}` for ReadDir
- **LSM-optimized layout:** Low-prefix metadata (0x01-0x08) stays hot in upper SST levels; high-prefix chunk data (0xFE) settles into cold lower levels -- prevents metadata ops from pulling chunk data into the storage engine's block cache
- **Namespace isolation:** Key prefix scoping prevents cross-tenant access
- **Range deletes:** `DeleteRange(chunk/{inode}/..., chunk/{inode}/...)` cleans up all chunks for a deleted file in O(1)

*Note: For PebbleStore, these are the literal byte-level keys. For WalkerStore, these are prefixed with a namespace/shard scope.*

#### Multi-FS Partitioning (PrefixedStore)

Multiple TemporalFS executions can share a single underlying storage engine via `PrefixedStore`. Each FS execution is assigned a unique `partitionID` (uint64), and the store transparently prepends an 8-byte big-endian prefix to all keys. This provides full keyspace isolation without requiring separate PebbleDB instances per FS:

- **PrefixedStore** wraps Store, Batch, Iterator, and Snapshot interfaces
- **Zero FS-layer changes:** The FS layer is unaware of partitioning -- it sees a normal Store interface
- **partitionID=0** returns the inner store directly (no wrapping) for backwards compatibility
- **Iterator key stripping:** The prefixed iterator strips the partition prefix from keys returned to the FS layer, so key parsing works unchanged

This is how Temporal Cloud will run many TemporalFS executions per Walker shard without key collisions.

#### Large Chunk Direct-to-S3

For chunks above a configurable size threshold, the client SDK writes directly to S3 and the Temporal server receives only the S3 location metadata -- not the data payload. This avoids double-egress (client->server->S3) and significantly reduces cost and latency for large files. This aligns with the approach validated by the large payload project.

#### Tiered Storage

TemporalFS data naturally separates into hot metadata and cold chunk data:

- **Hot:** Inode metadata, directory entries, transition manifests, config -- small, frequently accessed
- **Cold:** Chunk data (32KB each) -- bulk of storage, accessed on file reads

Tiered storage is handled at the Walker level via [Walker S3 Tiered Storage](./walker-s3-design.md). Pebble v2's built-in shared storage support moves cold SSTs (L4+ by default) to S3 while hot data stays on local SSD. This means:

- **WalkerStore gets tiering for free:** Cold chunk data (0xFE prefix, naturally settling into lower LSM levels) is automatically stored on S3. No FS-specific tiering code needed.
- **PebbleStore has no tiering:** All data on local disk (acceptable for OSS/development).
- **FS key layout is optimized for this:** Low-prefix metadata (0x01-0x08) stays hot in upper SST levels; high-prefix chunk data (0xFE) settles into cold lower levels that Walker tiers to S3.

```
Write Path (via FUSE mount -- e.g., echo "code" > /workspace/src/main.py):
  FUSE intercepts write() and close() syscalls:
    1. Writes buffered locally during the file handle's lifetime
    2. On close(): flush to server (close-to-open consistency)
    3. Resolve path: walk dir_entry keys from root inode to parent dir
    4. Allocate inode (or get existing inode ID for the file)
    5. Split data into 32KB chunks
    6. For each chunk: Set(chunk/{inode}/{idx}, content)
    7. Set(inode/{id}, updated InodeProto with new size/modtime)
    8. Set(dir_entry/{parent}/{name}, DirEntryProto) if new file
    9. Set(manifest/{T+1}, TransitionDiff{modified: [inode_id]})
    10. Set(manifest_latest, T+1)
    All steps 6-10 in a single atomic batch

Read Path (via FUSE mount -- e.g., cat /workspace/src/main.py):
  FUSE intercepts open() and read() syscalls:
    1. Resolve path: walk dir_entry keys -> inode ID
    2. Get(inode/{id}) -> InodeProto (size, chunk count)
    3. For each chunk (parallel):
       a. Worker chunk cache  -- ~10us  (LRU, keyed by inode+index)
       b. Storage engine      -- ~100us (bloom filter + point lookup)
    4. Reassemble chunks into file content
    5. Cache fetched chunks at layer (a) for future reads
```

#### Inode-Based Storage

Every file is identified by a monotonically increasing inode ID. Content is stored as fixed-size chunks keyed by `(inode_id, chunk_index)` -- the same model ZeroFS uses:

- **Simple, unique keys:** Every chunk has a deterministic key by construction. No hash computation, no collision risk at any scale.
- **Efficient updates:** Editing a file only rewrites the changed chunks. A 1MB file = 32 chunks; editing line 50 rewrites only chunk #2 = 32KB.
- **Sparse storage:** All-zero chunks are never stored. A missing chunk key means zeros.
- **Fast cleanup:** Deleting a file = `DeleteRange(chunk/{inode}/..., chunk/{inode}/...)` -- O(1) regardless of file size.

#### Chunk Lifecycle

```
Chunk States:
  Live      -> inode exists and references this chunk
  Orphaned  -> inode deleted (tombstone written), chunks eligible for GC

GC Process (tombstone-based, same as ZeroFS):
  1. Scan tombstone prefix (0x06) for deleted inodes
  2. For each tombstoned inode: DeleteRange all chunk/ keys for that inode
  3. Delete tombstone after cleanup
  4. Run on configurable schedule (default: daily)
  5. Storage engine's own compaction handles lower-level cleanup automatically
```

---

## Layered Storage Efficiency

The FS layer is architecturally isomorphic to ZeroFS, regardless of which backend is in use:

```
ZeroFS (SlateDB on S3)              TemporalFS (pluggable backend)
──────────────────────              ────────────────────────────────
VFS layer (inodes, chunks)     -->  FS layer (inodes, chunks, transitions)
SlateDB (LSM on S3)            -->  Store interface (Pebble / Walker)
Memtable                       -->  Pebble memtable (both backends use Pebble under the hood)
SST flush + compaction         -->  Pebble SST flush + leveled compaction
SSTs on S3                     -->  Walker S3 tiering (cold SSTs on S3, hot on local SSD)
Bloom filters                  -->  Pebble bloom filters
WAL                            -->  Pebble WAL (+ Walker replication for Cloud)
Manifest checkpoint            -->  Manifest key (manifest/{T})
32KB chunks                    -->  32KB inode-based chunks
```

Both backends ultimately run on Pebble. Walker IS distributed/sharded Pebble with S3 tiering for cold data. We are not building "something like" an LSM -- we are building an FS layer on one.

### Manifest Diffs as the Layering Mechanism

Each TemporalFS write produces a **manifest diff**, not a full manifest copy. This is the core of the layered model:

```
Transition T=0 (initial):
  TransitionDiff: {created_inodes: [inode_1 (root dir), inode_2 (/src/main.py)]}

Transition T=1 (edit main.py, add utils.py):
  TransitionDiff: {modified_inodes: [inode_2], created_inodes: [inode_3 (/src/utils.py)]}
  (inode_2's chunk #2 rewritten; inode_3 is new; all other chunks unchanged)

Transition T=2 (delete main.py):
  TransitionDiff: {deleted_inodes: [inode_2]}

Full state at T=2 = apply(T=0, T=1, T=2):
  Active inodes: [inode_1 (root dir), inode_3 (/src/utils.py)]
```

This is exactly how LSM layers work: each layer is a diff, and reads merge layers top-down. The manifest keys in Walker *are* the layer stack.

### Write Efficiency: Only Store What Changed

```
Scenario: AI agent edits line 50 of a 1MB Python file

Without chunked storage:
  Rewrite entire file = 1MB new storage per edit

With inode-based chunks (32KB):
  File (inode_42) = 32 chunks: chunk/{42}/0 through chunk/{42}/31
  Line 50 falls in chunk #2
  Only chunk #2 is rewritten = 32KB new storage per edit
  Other 31 chunks remain unchanged in Walker

Storage cost of edit: 32KB (not 1MB) = 97% reduction
```

For AI agent workloads where agents make incremental edits to code files, this is the common case. Most edits touch a small fraction of the file.

### Read Efficiency: Fetch Only What's Needed

TemporalFS never loads the full filesystem into memory. Reads are surgical:

```
Read Path for cat /workspace/src/main.py (via FUSE mount):

  1. Path resolution: walk dir_entry keys from root -> inode ID
     - dir_entry/{root}/"src" -> inode_5 (dir)
     - dir_entry/{5}/"main.py" -> inode_42 (file)
     - Each step: O(1) point lookup, bloom filter accelerated

  2. Inode lookup: Get(inode/{42}) -> InodeProto (size=1MB, 32 chunks)

  3. Per-chunk resolution (parallel):
     a. Worker chunk cache     -- ~10us  (LRU, keyed by inode+index, on SDK worker)
     b. Storage engine         -- ~100us (bloom filter check, then point lookup)

  4. Bloom filter fast-path (Pebble built-in):
     - Answers "does chunk/{42}/{idx} exist?" without scanning SST files
     - Avoids unnecessary disk I/O for missing keys

  5. Parallel chunk fetch:
     - All chunks for a file fetched concurrently
     - 1MB file = 32 chunks = 32 parallel reads
     - Typical warm read of 1MB file: < 1ms
```

Contrast with "serialize files into workflow payloads": reading one file would require deserializing the entire payload. TemporalFS via FUSE reads only the chunks for the requested file -- the agent just does `cat /workspace/src/main.py` and gets surgical chunk-level access transparently.

### Snapshot Efficiency: Zero-Copy Versioning

Snapshots are the cheapest operation in the system:

```
Snapshot at T=5:
  - Record: "Pebble snapshot pinned at T=5" + manifest pointer
  - No data copied. No chunks duplicated.
  - Pebble snapshots are lightweight: they prevent compaction from deleting
    the state visible at that point, but don't copy data.

Cost of snapshot: one manifest pointer + Pebble snapshot handle (< 1KB metadata)

Cost of maintaining 100 snapshots of a 1GB filesystem:
  - Unchanged chunks are shared across snapshots (same inode, same index = same KV pair)
  - Only chunks that were rewritten between snapshots occupy additional storage
  - If each snapshot has 1% unique changes: ~2GB total (not 100GB)
```

This is how ZeroFS checkpoints work (metadata-only manifest references to immutable SSTs), and our model is identical in principle.

### Manifest Compaction: Preventing Diff Accumulation

Over time, a TemporalFS execution with thousands of transitions accumulates thousands of manifest diffs. Reconstructing current state requires replaying all diffs -- this gets slow.

**Manifest compaction** solves this by periodically flattening the diff chain:

```
Before compaction (1000 transitions):
  Current state = apply(diff_0, diff_1, diff_2, ..., diff_999)
  Read cost: must traverse up to 1000 diffs to resolve a path

After compaction:
  Checkpoint at T=950: full manifest snapshot (all active inodes and their metadata)
  Current state = apply(checkpoint_950, diff_951, ..., diff_999)
  Read cost: checkpoint lookup + up to 50 diffs

Compaction process (runs as CHASM CompactionTask):
  1. Take the last checkpoint (or T=0 if none)
  2. Apply all diffs since that checkpoint to produce a full manifest
  3. Store as a new checkpoint at the current transition
  4. Old diffs before the checkpoint are eligible for deletion
     UNLESS a snapshot still references them (snapshot retention)
  5. Schedule: trigger when diff count since last checkpoint exceeds threshold
     (default: 500 diffs, configurable per-execution)
```

This is the direct equivalent of LSM compaction: merge small, overlapping layers into larger, consolidated ones. The difference is we operate on manifest diffs rather than SST files, and Pebble handles the underlying chunk storage compaction independently.

### What We Intentionally Skip (and Why)

| ZeroFS Feature | TemporalFS | Rationale |
|---|---|---|
| Custom LSM tuning for FS workloads | Walker's Pebble tuning (already optimized for Temporal Cloud) | Walker is battle-tested at scale. We'd tune only if benchmarks show a bottleneck. |
| WAL direct to S3 | Pebble WAL to local disk + replication | Replication handles durability. Direct-to-S3 WAL adds latency without benefit. |
| Standalone compaction process | Pebble compaction (automatic) + FS manifest compaction (scheduled) | Pebble handles SST compaction. FS layer only compacts manifest diffs. No separate process. |
| Read prefetching across files | Per-chunk parallel fetch | Sufficient for workspace-sized file trees. FUSE mount can add directory-level prefetching. |
| NFS/9P/NBD protocol servers | FUSE mount (P1) | FUSE provides full POSIX compatibility without protocol server complexity. Unmodified programs work naturally. |
| Custom encryption layer (XChaCha20) | Chunk-level encryption with per-FS keys; metadata encrypted separately | Temporal server sees FS metadata (inode structure, sizes, timestamps) but not file content when client-side encryption is enabled. Per-FS keys enable key rotation and per-tenant isolation. |
| SlateDB (separate storage engine) | Pluggable Store (Pebble / Walker) | Both backends run on Pebble. Walker adds S3 tiering for cold data. No need for a second engine. |

### Storage Efficiency Summary

| Operation | Efficiency |
|---|---|
| Write 1MB file | Store only changed chunks (~32KB per small edit) |
| Snapshot a 1GB filesystem | ~1KB metadata pointer + Pebble snapshot handle |
| Read one file from 10,000-file FS | Load only that file's chunks (not the full FS) |
| 100 versions of a 1GB filesystem | ~1-2GB total (only rewritten chunks stored twice) |
| Manifest lookup after 1000 transitions | O(50) diffs after compaction (not O(1000)) |

---

## Consistency and Replay Model

### Write Consistency

All writes to a TemporalFS execution are **totally ordered through CHASM's state machine**. This means:

- Every write, remove, or mkdir is a CHASM mutation on the TemporalFS execution
- Mutations are ordered by `VersionedTransition` -- monotonically increasing, no interleaving within a transition
- Multiple workflows writing to the same TemporalFS are serialized -- no distributed locking needed
- The CHASM engine handles conflict resolution: writes are applied in transition order
- **No ABBA ordering errors:** Total ordering means two writers can never produce interleaved partial state

**Close-to-open consistency:** For FUSE-mounted access, the mount provides close-to-open consistency (like NFS) -- writes are flushed on `close()` and visible to subsequent `open()` calls. This avoids the latency cost of per-operation round-trips to the server while preserving strong consistency at file boundaries. Two workflows writing different files never conflict. Two workflows writing the same file produce ordered transitions -- last writer wins, with full history preserved.

### Replay Determinism

When a workflow reads a file (via FUSE mount or SDK):

1. The read is routed to the TemporalFS execution, which returns the file content **and** the current FS transition number `T`
2. The SDK records `(path, T)` in the workflow's event history
3. On replay, the SDK sees the recorded `(path, T)` and reads from the TemporalFS snapshot at transition `T`
4. The TemporalFS execution maintains snapshots (manifest checkpoints) for all transitions that are still referenced by active workflow replays

This means:
- Replay always sees the same file content, even if the FS has advanced
- No special replay mode -- the SDK just pins to a transition
- Snapshot retention is automatic: CHASM tracks which transitions are still needed

### Concurrency Model

```
                    TemporalFS Execution
                    (serialized mutations)
                           │
            ┌──────────────┼──────────────┐
            │              │              │
     Workflow A       Workflow B      Activity C
     (read-write)    (read-write)   (read-only @T=5)
            │              │              │
     Records T=7    Records T=9    Pinned to T=5
     in its history in its history (deterministic)
```

- **Server-side state:** FS state lives in the CHASM engine, not on any worker's disk. Workers on different hosts all access the same FS execution via RPC. Worker-local chunk caches are a performance optimization; the source of truth is always the server.
- **Read-write mounts:** See the latest state. Writes are sequenced by the FS execution.
- **Read-only snapshots:** Pinned to a specific transition. Used for replay and for activities that need a consistent view.
- **No locks:** Writers don't block each other. Last writer wins for the same path, with full history preserved.
- **Efficient storage:** Snapshots share unchanged chunks. Only rewritten chunks occupy additional storage. Worker-local caches are keyed by `(inode_id, chunk_index)`, so two workflow instances on the same machine reading the same file hit the same cached chunks.

---

## Temporal Cloud Considerations

### Billing Model

TemporalFS introduces two new billable dimensions:

| Dimension | Unit | Description |
|-----------|------|-------------|
| **FS Storage** | GB-month | Total size of all TemporalFS data (metadata + chunks) in a namespace |
| **FS Operations** | Per 1,000 ops | Read, write, list, snapshot operations against TemporalFS executions |

Existing TRU-based billing does not change. FS operations are a new meter, separate from workflow actions.

### Storage Quotas

| Quota | Default | Configurable |
|-------|---------|-------------|
| Max TemporalFS executions per namespace | 100 | Yes (account-level) |
| Max size per TemporalFS execution | 10 GB | Yes (per-execution) |
| Max total FS storage per namespace | 100 GB | Yes (account-level) |
| Max file size | 1 GB | Yes (per-execution) |
| Max files per FS execution | 100,000 | Yes (per-execution) |
| Snapshot retention | 30 days after last reference | Yes (per-execution) |

### Multi-Tenant Isolation

- **Namespace isolation:** TemporalFS executions are scoped to a namespace. No cross-namespace access.
- **Storage isolation:** TemporalFS data is scoped by namespace and execution ID. Key prefixes prevent cross-tenant data access.
- **Resource limits:** Per-namespace quotas enforced by `QuotaEnforcementTask` running as a CHASM task.
- **Encryption:** Chunk-level encryption with per-FS keys. Metadata encrypted separately. Temporal server sees FS metadata (inode structure, sizes, timestamps) but not file content when client-side encryption is enabled. Per-FS keys enable key rotation and per-tenant isolation.

### Storage Management

- **Customer-managed keys:** Support for BYOK via cloud KMS integration (same pattern as existing Temporal Cloud BYOK)
- **Cross-region replication:** For multi-cluster setups, TemporalFS replication follows the same model as CHASM state replication. Chunk data is immutable once written (only deleted, never updated in place), so replication is idempotent.
- **Tiered storage:** Walker S3 tiered storage moves cold SSTs to S3 automatically. TemporalFS chunk data (0xFE prefix) naturally settles into lower LSM levels that Walker tiers to S3. No FS-specific tiering needed.

### Observability

- **Metrics:** `temporalfs_operations_total`, `temporalfs_storage_bytes`, `temporalfs_chunk_cache_hit_ratio`, `temporalfs_blob_fetch_latency`
- **Search Attributes:** `TemporalFSId`, `TemporalFSSize`, `TemporalFSFileCount`, `TemporalFSLastWriteTime`
- **Audit log:** All FS operations logged with caller workflow/activity identity

---

## Repository and Project Structure

### Existing Repositories (Changes Required)

| Repository | Changes | Phase |
|------------|---------|-------|
| **[temporalio/temporal](https://github.com/temporalio/temporal)** | New CHASM archetype under `chasm/lib/temporalfs/`. Proto definitions, server-side state machine, inode/chunk management, compaction tasks. | P1 |
| **[temporalio/api](https://github.com/temporalio/api)** | New proto service `temporal.api.temporalfs.v1` with RPCs: `CreateFilesystem`, `OpenFilesystem`, `MountFilesystem`, `ReadChunks`, `WriteChunks`, `Snapshot`, `GetFilesystemInfo`, `ArchiveFilesystem`. FUSE mount translates POSIX syscalls into these RPCs. | P1 |
| **[temporalio/api-go](https://github.com/temporalio/api-go)** | Generated Go bindings for the new protos. | P1 |
| **[temporalio/sdk-go](https://github.com/temporalio/sdk-go)** | `temporalfs` package: FUSE mount, `Create()`/`Open()`/`Mount()`, snapshot pinning, replay integration, local chunk cache. | P1 |
| **[temporalio/sdk-python](https://github.com/temporalio/sdk-python)** | `temporalio.fs` module: Python bindings for TemporalFS with `pathlib`-style API. | P2 |
| **[temporalio/sdk-typescript](https://github.com/temporalio/sdk-typescript)** | `@temporalio/fs` package: TypeScript/Node bindings with `fs`-compatible API. | P2 |
| **[temporalio/saas-temporal](https://github.com/temporalio/saas-temporal)** | TemporalFS Cloud backend: WalkerStore implementation, Walker S3 tiering adapter, billing meter hooks. | P1 |
| **[temporalio/saas-control-plane](https://github.com/temporalio/saas-control-plane)** | Namespace-level TemporalFS configuration: enable/disable, quotas, blob storage settings. | P1 |
| **[temporalio/cloud-api](https://github.com/temporalio/cloud-api)** | Cloud API extensions for TemporalFS management (quota config, billing visibility). | P2 |
| **[temporalio/ui](https://github.com/temporalio/ui)** | TemporalFS explorer: browse files, view history, see mount table, storage usage. | P2 |
| **[temporalio/cli](https://github.com/temporalio/cli)** | `temporal fs` subcommands: `create`, `ls`, `cat`, `write`, `snapshot`, `info`, `archive`. | P2 |
| **[temporalio/tcld](https://github.com/temporalio/tcld)** | Cloud CLI extensions for TemporalFS quota management. | P2 |
| **[temporalio/object-storage-cache](https://github.com/temporalio/object-storage-cache)** | Extend for TemporalFS chunk caching use case. Chunk cache keyed by `(inode, index)` with bloom filters. | P1 |
| **[temporalio/samples-go](https://github.com/temporalio/samples-go)** | TemporalFS examples: AI agent workspace, multi-workflow collaboration, data pipeline. | P2 |
| **[temporalio/documentation](https://github.com/temporalio/documentation)** | TemporalFS concept docs, API reference, tutorials, Cloud configuration guide. | P2 |

### New Repositories

No new repositories are needed. All code lives within existing repos:

- **Server-side:** `temporalio/temporal` under `chasm/lib/temporalfs/`
- **Cloud-side:** `temporalio/saas-temporal` for Walker integration and blob storage
- **SDK-side:** Each SDK repo gets a new package/module
- **Protos:** `temporalio/api` gets the new service definition

This follows the established pattern for CHASM archetypes (Activity, Scheduler) and avoids repo sprawl.

---

## Phased Delivery Plan

### Phase 1: Foundation (Target: Q3 2026)

**Goal:** A working TemporalFS that a single Go workflow can create, write to, read from, and share with activities. Cloud-deployed with basic billing.

**Deliverables:**
- [ ] Proto definitions in `temporalio/api` (`temporalfs.v1` service)
- [ ] CHASM archetype in `temporalio/temporal` (`chasm/lib/temporalfs/`)
  - Inode manager (alloc/free, metadata, directory operations)
  - Chunk store (32KB fixed-size, read/write/delete/truncate)
  - Manifest compaction (flatten diff chains)
  - Chunk GC via tombstones
- [ ] Two storage backends behind pluggable `Store` interface:
  - PebbleStore (v1/OSS, local development)
  - WalkerStore (direct Walker with S3 tiering, Cloud)
  - Benchmarks: 1K / 100K / 1M file workloads, verify S3 tiering for cold chunks
- [ ] Walker S3 tiered storage adapter (prerequisite, see [Walker S3 Tiered Storage](./walker-s3-design.md))
- [ ] Go SDK client (`temporalfs` package) with:
  - `Create()`, `Open()`, `Mount()` (FUSE)
  - FUSE mount providing full POSIX filesystem access (Linux and macOS)
  - Close-to-open consistency for FUSE-mounted access
  - Snapshot pinning for replay determinism
  - Local chunk cache on worker
- [ ] Chunk-level encryption with per-FS keys (metadata encrypted separately)
- [ ] Chunk-level compression (LZ4 default) applied before encryption
- [ ] Direct-to-S3 for large chunks (client SDK writes directly to S3, server receives only location metadata)
- [ ] Basic Cloud integration:
  - Namespace-level enable/disable
  - Default quotas
  - Storage metering (GB-month)
  - Operations metering (per 1K ops)
- [ ] Integration tests and `temporalio/features` compatibility tests

**Key constraint:** Single TemporalFS execution accessed by one workflow + its activities. Multi-workflow sharing deferred to P2 to keep scope tight.

### Phase 2: Multi-Workflow Sharing & Multi-SDK (Target: Q4 2026)

**Goal:** Multiple workflows share a TemporalFS. Python and TypeScript SDK support. UI and CLI integration.

**Deliverables:**
- [ ] Multi-workflow concurrent access
  - Serialized writes through CHASM state machine
  - Mount table tracking active readers/writers
  - Snapshot reads for replay across workflows
- [ ] Python SDK (`temporalio.fs`)
- [ ] TypeScript SDK (`@temporalio/fs`)
- [ ] UI: TemporalFS browser (file tree, version history, mount table, storage stats)
- [ ] CLI: `temporal fs` subcommands
- [ ] Cloud API extensions for quota management
- [ ] Advanced quotas: per-execution size limits, file count limits
- [ ] `temporalio/samples-go` examples: AI agent workspace, multi-agent collaboration

### Phase 3: Advanced Features (Target: H1 2027)

**Goal:** Production hardening, advanced access patterns, ecosystem integration.

**Deliverables:**
- [ ] **Directory-level locking:** Optional pessimistic locking for workflows that need exclusive access to a subtree
- [ ] **File watchers:** Workflows can subscribe to changes on specific paths (event-driven, not polling)
- [ ] **Cross-namespace sharing:** Read-only mounts from other namespaces (with ACL)
- [ ] **Tiered storage policies:** Hot/warm/cold tiers with automatic migration based on access patterns
- [ ] **Import/export:** Bulk import from S3/GCS, export TemporalFS to a zip/tar archive
- [ ] **Java, .NET, PHP, Ruby SDK support**
- [ ] **Customer-managed encryption keys (BYOK)** for TemporalFS chunks

---

## Success Metrics

### Phase 1

| Metric | Target |
|--------|--------|
| TemporalFS executions created (Cloud) | 500+ in first 3 months |
| P95 WriteFile latency (< 1MB file) | < 100ms |
| P95 ReadFile latency (cached) | < 10ms |
| P95 ReadFile latency (cold, from blob) | < 500ms |
| Replay correctness | 100% (zero replay divergences attributable to TemporalFS) |

### Phase 2

| Metric | Target |
|--------|--------|
| Namespaces using TemporalFS (Cloud) | 50+ |
| Multi-workflow FS sharing adoption | 30% of TemporalFS users |
| SDK adoption (Python + TS) | At parity with Go within 6 months of launch |

### Phase 3

| Metric | Target |
|--------|--------|
| Advanced feature adoption (locking, watchers) | 20% of TemporalFS users |
| Average FS size | Trending upward (indicates deeper usage) |
| Customer retention impact | Measurable reduction in churn for AI agent accounts |

---

## Risks and Mitigations

| Risk | Severity | Mitigation |
|------|----------|------------|
| **Storage pressure:** TemporalFS chunks could significantly increase storage requirements | High | Quota enforcement prevents unbounded growth. Monitor storage per namespace. Walker S3 tiering moves cold chunk data to S3 automatically (~4x cheaper than SSD). Walker may need dedicated shards for heavy FS users. |
| **Storage costs at scale:** Large FS executions with many versions accumulate chunk data | Medium | Only changed chunks are stored per version. Tombstone-based GC cleans up deleted inodes and their chunks. Clear billing visibility so customers can manage costs. |
| **Replay complexity:** Recording FS transitions in workflow history adds a new replay dependency | High | Extensive replay correctness testing in `temporalio/features`. Snapshot retention guarantees prevent data loss. SDK-level integration tests for all supported languages. |
| **Write serialization bottleneck:** All writes to one TemporalFS go through one CHASM execution | Medium | Only inode metadata and manifest diffs are serialized (small). For extreme write throughput, users can shard across multiple TemporalFS executions. |
| **Multi-region replication latency:** Chunks need to be available in all regions | Medium | Chunks are immutable once written, so replication is idempotent. Async replication with read-your-writes guarantee within a region. Cross-region reads may have higher latency for uncached chunks. |
| **Scope creep toward general-purpose distributed filesystem** | High | Stay focused on the AI agent use case. TemporalFS is not HDFS or NFS -- it's a durable workspace for workflow state. Resist adding features that don't serve replay determinism or workflow collaboration. |

---

## Open Questions

1. **Chunk size optimization:** Is 32KB the right default chunk size? Smaller chunks = finer-grained updates but more metadata overhead. Larger chunks = less overhead but more data rewritten per small edit. Need benchmarking with real AI agent workloads.

2. **Manifest size limits:** For TemporalFS executions with 100K+ files, the manifest itself becomes large. Should we support manifest sharding (split by directory prefix) in P1 or defer? Manifest compaction (see [Layered Storage Efficiency](#layered-storage-efficiency)) handles diff accumulation, but full manifest size is a separate concern.

3. **Snapshot retention policy:** How long should we keep manifest snapshots for replay? Options: (a) keep until all referencing workflows complete, (b) time-based TTL, (c) configurable per-execution. This directly impacts storage costs.

4. **CHASM transition cost:** Each `WriteFile()` is a CHASM mutation. `WriteBatch()` is included in P1 for atomic multi-file writes, but for workloads with very high write frequency (e.g., streaming writes), should we support buffered/debounced writes that accumulate mutations before committing?

5. **Symlinks and hard links:** Do we support them in P1? AI agent workloads rarely need them, but data pipeline workloads might. Recommend deferring to P3.

6. **File permissions model:** POSIX-style permissions or simpler (read-only / read-write per mount)? Recommend simpler model for P1, since the primary access control is at the Temporal namespace level.

7. **Maximum TemporalFS execution lifetime:** Should TemporalFS executions have a maximum lifetime (like workflow execution timeout), or can they live indefinitely? Indefinite lifetimes need robust GC and archival.

8. **Walker capacity for FS workloads:** For very large FS executions (100K+ files, GB-scale chunks), does Walker's Pebble sharding handle the load without impacting other Temporal Cloud workloads? Need capacity modeling and potentially dedicated Walker shards for heavy FS users.

9. **Walker S3 tiering readiness:** WalkerStore depends on Walker S3 tiered storage ([design doc](./walker-s3-design.md)). Key questions:
   - Can Walker S3 tiering be production-ready in time for TemporalFS P1?
   - What is the read latency impact for cold chunk data (S3 fetch vs local SSD)?
   - How should `SecondaryCacheSizeBytes` be sized for FS-heavy datanodes?
   - Does TemporalFS's key layout (0xFE chunks in lower levels, 0x01 metadata in upper levels) achieve the expected hot/cold separation in practice?

---

*TemporalFS: Files that remember everything, replay perfectly, and never lose a byte.*
