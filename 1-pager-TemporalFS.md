# [1-pager] TemporalFS: Durable Filesystem for AI Agent Workflows

Driver: Moe Dashti
Status: 1-pager Ready for review
Strategic Alignment: 5: Perfect alignment (linchpin of company strategy)
Revenue Potential (Direct or Indirect): 4: $100M ARR next 3 years (10% business improvement)
Customer Demand: 3: 25%+ customers requesting
Customer Value: 5: Critical value (essential  for core customer needs)
Company Effort (LOE): 3: 3-6 person months (anything requiring 2+ teams is automatically at least a 3)
Created time: March 5, 2026 8:53 PM

## Problem

AI agent workloads generate and consume files: code repositories, build artifacts, datasets, model checkpoints, configuration trees. Today, these agents either lose file state on failure (ephemeral scratch), rely on external storage with no consistency guarantees, or serialize entire file trees into workflow payloads (expensive, brittle, no random access).

The core tension: **AI agents need a filesystem, but durable execution needs determinism.** If an agent writes files during an activity, those files must be visible in exactly the same state during replay. If the workflow forks, retries, or resets, the filesystem must fork with it. And critically, **real workloads don’t run in isolation** – multiple workflows and activities need to collaborate on the same file tree, like engineers sharing a repository. No existing solution provides this.

## Vision

Today, teams building AI agents on Temporal face an awkward gap: the agent's *execution* is durable, but its *files* are not. A coding agent that clones a repo, edits files, and runs tests will lose all of that state if the activity fails or the worker restarts. The workarounds -- stuffing file trees into workflow payloads, mounting NFS shares, syncing to S3 between steps -- are brittle, expensive, and invisible to Temporal's durability guarantees.

**TemporalFS closes this gap.** It gives every AI agent workflow a durable, versioned filesystem that Temporal manages end-to-end -- just like it manages workflow state today. Files survive failures, replays, and worker migrations without any application-level plumbing.

The developer experience is straightforward: an activity gets a FUSE-mounted directory that behaves like a normal filesystem. The agent (or any unmodified program it invokes -- `git`, `pytest`, a compiler) reads and writes files naturally. Temporal persists every mutation, versions the file tree, and restores it exactly on retry or replay. No custom storage code, no S3 sync scripts, no payload serialization.

**TemporalFS** is a new CHASM Archetype -- a first-class execution type like Workflow itself -- that provides a durable, versioned, replay-safe virtual filesystem. It has its own lifecycle, independent of any single workflow, enabling **multiple workflows and activities to share the same filesystem**.

### Phasing

- **P1 (MVP): Single-workflow Agent FS.** One workflow and its activities own a TemporalFS execution. Activities get a FUSE mount (or programmatic API) for natural file access. This covers the primary AI agent use case.
- **P2: Multi-workflow sharing.** Multiple workflows mount the same TemporalFS execution with controlled concurrency. The Archetype model is designed from day 1 to support this without re-architecture.

### Access: FUSE Mount

TemporalFS is accessed via a FUSE mount -- a local directory that behaves like a normal filesystem. Unmodified programs (`git`, `python`, `gcc`, etc.) work without changes. The mount connects to the Temporal server; all reads and writes flow through CHASM. This is the single interface for all file access.

```
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

TemporalFS state lives server-side in CHASM, not on worker disk. Workers on different hosts all access the same FS execution via RPC. Worker-local caches are a performance optimization; the source of truth is always the server. Temporal handles versioning, persistence, caching, concurrent writes, replay consistency, and multi-cluster replication.

**Why not just NFS?** NFS requires provisioning and managing a separate NFS server, doesn't integrate with Temporal's durability model (no versioning, no replay determinism, no automatic failover), and has no concept of workflow-scoped lifecycle. TemporalFS is zero-infrastructure for the developer -- `Create()` and `Mount()` are all it takes.

## How It Works

### 1. TemporalFS as a CHASM Archetype

TemporalFS is its own Archetype – like Workflow. It has its own Execution with an independent lifecycle, its own `BusinessID`, and its own state tree. This is the key architectural decision: **the filesystem outlives any single workflow and is shared across many**.

```
TemporalFS Archetype
├── Execution       (independent lifecycle, addressable by BusinessID)
│   ├── InodeTable      Field[*InodeTable] // inode metadata (files, dirs)
│   ├── ChunkStore      Field[*ChunkIndex] // file content in fixed-size chunks
│   ├── Config          Field[*FSConfig]  // mount options, limits, cache policy
│   ├── AccessLog       Field[*AccessLog] // who mounted, read, wrote, when
│   └── Lifecycle       Running | Archived | Deleted
│
├── Mounts          (concurrent readers and writers)
│   ├── WorkflowA   read-write, transition T=5
│   ├── WorkflowB   read-write, transition T=8
│   └── ActivityC   read-only snapshot at T=3
```

Any workflow or activity in the namespace can `Open()` the TemporalFS execution by ID. Each mount tracks its own transition cursor for replay determinism while the filesystem itself maintains a global, linearized mutation history.

### 2. **Storage: Inode-Based, Same Model as ZeroFS**

TemporalFS uses the same inode-based storage model as ZeroFS, with a thin FS layer on top of an LSM-tree storage engine:

| Layer | ZeroFS | TemporalFS |
| --- | --- | --- |
| FS abstraction | VFS layer (inodes, dirs, chunks) | TemporalFS layer (inodes, dirs, chunks, transitions) |
| Storage engine | SlateDB (LSM on S3) | CHASM persistence (Walker/Pebble, with S3 tiering for cold data)  |

The underlying storage engine (Walker) already provides the LSM-tree primitives: memtable, SST flush, leveled compaction, bloom filters, batch writes, and snapshots. TemporalFS adds the FS-specific layer on top:

- **Write path:** Files are identified by inodes (monotonically increasing IDs). Content stored as fixed-size chunks (32KB default) keyed by `(inode_id, chunk_index)` -- the same model ZeroFS uses. Directory entries map names to inode IDs. Only changed chunks are written. Transition diff records which inodes changed.
- **Snapshot path:** Each transition produces a manifest diff entry. Reverting to transition N means reading the snapshot at that point -- no data copying, just a key lookup.
- **Read path:** Path resolution walks directory entries (bloom filter accelerated) -> inode lookup -> parallel chunk fetch. Multi-layer cache: storage engine cache -> worker-local cache.
- The storage backend is pluggable via a `Store` interface. We plan two implementations: **PebbleStore** (local/OSS) and **WalkerStore** (direct Walker for Cloud). Walker is being extended with S3 tiered storage (see [Walker S3 Tiered Storage](https://www.notion.so/Walker-S3-Tiered-Storage-31e8fc567738808eba33faa6c43800b5?pvs=21)) -- cold SSTs at lower LSM levels are stored on S3 while hot data stays on local SSD. This gives WalkerStore effectively unlimited capacity without TemporalFS-specific tiering work. The FS layer above is identical regardless of backend.
- **Large chunk direct-to-S3:** For chunks above a size threshold, the client SDK writes directly to S3 and the Temporal server receives only the S3 location metadata -- not the data payload. This avoids double-egress (client->server->S3) and significantly reduces cost and latency for large files. This aligns with the approach validated by the large payload project.

### 3. Concurrent Writes and Consistency

Because TemporalFS is a shared Archetype, multiple workflows can write concurrently. The consistency model:

- **Linearized mutations:** All writes are serialized through the TemporalFS execution’s CHASM state machine. Each write is a transition in the TemporalFS execution (not the caller’s workflow). For FUSE-mounted access, the mount provides close-to-open consistency (like NFS) -- writes are flushed on `close()` and visible to subsequent `open()` calls, avoiding the latency cost of per-operation round-trips to the server.
- **File-level conflict resolution:** Two workflows writing different files never conflict. Two workflows writing the same file produce ordered transitions -- last writer wins, with full history preserved.
- **Snapshot reads:** A caller can open a read-only snapshot at any transition T, getting an immutable view. This is how activities get a consistent view -- they pin to the transition at which they originally read.
- **Read-write mounts:** Writers get the latest state and their writes are sequenced by the TemporalFS execution engine.

### 4. Replay Determinism

The key invariant: **`fs.Read(path)` at transition T always returns the same bytes, regardless of when or where it executes.**

Because TemporalFS is its own Archetype with its own transition history:

- Each TemporalFS mutation gets a global `VersionedTransition` in the FS execution
- When a workflow calls `fs.Read()`, the SDK records which FS transition it observed
- On replay, the SDK replays the read against the same FS transition – not the current state
- This decouples the caller’s replay from the FS’s current state, which may have advanced due to other writers
- No external I/O during replay – all reads resolve against the recorded transition snapshot
- Efficient storage: snapshots are metadata-only (manifest pointers). Chunk data is shared across snapshots -- only changed chunks are written per transition. No full-copy duplication.

## Architecture

```
┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
│  Worker A            │  │  Worker B            │  │  Worker C            │
│  ┌───────────────┐   │  │  ┌───────────────┐   │  │  ┌───────────────┐   │
│  │ Orchestrator  │   │  │  │ AI Agent      │   │  │  │ Data Pipeline │   │
│  │ Workflow      │   │  │  │ Workflow      │   │  │  │ Activity      │   │
│  └──────┬────────┘   │  │  └──────┬────────┘   │  │  └──────┬────────┘   │
│         │            │  │         │            │  │         │            │
│  ┌──────v────────┐   │  │  ┌──────v────────┐   │  │  ┌──────v────────┐   │
│  │ FS Mount      │   │  │  │ FS Mount      │   │  │  │ FS Mount      │   │
│  │ read-write    │   │  │  │ read-write    │   │  │  │ read-only     │   │
│  │ + local cache │   │  │  │ + local cache │   │  │  │ snapshot @T=5 │   │
│  └──────┬────────┘   │  │  └──────┬────────┘   │  │  └──────┬────────┘   │
└─────────┼────────────┘  └─────────┼────────────┘  └─────────┼────────────┘
          │                         │                         │
          └─────────────┬───────────┘─────────────────────────┘
                        │  all mounts target the same TemporalFS execution
                        v
┌──────────────────────────────────────────────────────────────────────────┐
│  Temporal Server (CHASM Engine)                                          │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │  TemporalFS Execution  (Archetype: "temporalfs", ID: "project-ws") │  │
│  │                                                                    │  │
│  │  Manifest[T=0] ──> Manifest[T=1] ──>  ...  ──> Manifest[T=N]       │  │
│  │       │                  │                           │             │  │
│  │       v                  v                           v             │  │
│  │  ┌─────────┐      ┌─────────┐                 ┌─────────┐          │  │
│  │  │ Chunks  │      │ Chunks  │                 │ Chunks  │          │  │
│  │  │ i1:0-2  │      │ +i2, ~i1│                 │ +i3, ~i1│          │  │
│  │  └─────────┘      └─────────┘                 └─────────┘          │  │
│  │                                                                    │  │
│  │  Writes serialized through TemporalFS execution state machine      │  │
│  │  Mutations replicated via standard CHASM replication               │  │
│  │  Storage: pluggable (PebbleStore / WalkerStore)                    │  │
│  └────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

| Decision | Choice | Rationale |
| --- | --- | --- |
| **Storage unit** | Inode-based with fixed-size chunks (32KB default) | Same model as ZeroFS. Unique key per chunk `(inode, index)` -- no hash collisions, no hash overhead. Only changed chunks written per edit. |
| **Manifest model** | Per-transition diffs tracking changed inodes | O(1) version switching via Pebble snapshots. Manifest compaction prevents diff accumulation. |
| **Large file handling** | Fixed-size chunks; Walker S3 tiering for cold data | Walker's S3 tiered storage moves cold SSTs (including chunk data) to S3 automatically. No FS-specific tiering needed. |
| **CHASM integration** | Own Archetype, not a child Component of Workflow | Independent lifecycle enables sharing across workflows. Multiple workflows and activities mount the same FS. Survives individual workflow completion or failure. |
| **Cache hierarchy** | Storage engine cache -> worker-local cache | Hot files served in microseconds. Cold files fetched once and cached. Bloom filters prevent unnecessary reads. |
| **Replay strategy** | Callers record the FS transition they observed; replay reads from that snapshot | Decouples caller replay from FS state that may have advanced. Each caller replays deterministically against its recorded transition. |
| **Concurrency** | Multi-writer via serialized mutations on the TemporalFS execution; read-only snapshots for deterministic replay | Writes are linearized through CHASM’s state machine – no distributed locking. Readers pin to a transition for consistency. |
| **Encryption** | Chunk-level encryption with per-FS keys; metadata encrypted separately | Temporal server sees FS metadata (inode structure, sizes, timestamps) but not file content when client-side encryption is enabled. Designing encryption at the chunk level from day 1 avoids costly retrofits. Per-FS keys enable key rotation and per-tenant isolation. |
| **Compression** | Chunk-level compression (LZ4 default) applied before encryption | Compression must happen before encryption (encrypted data is incompressible). Chunk-level granularity preserves random access -- no need to decompress entire files for partial reads. LZ4 chosen for speed; zstd available for higher ratios on cold data. |

## Why Now

1. **CHASM is ready.** The Archetype/Component/Field/Task model is mature enough. Adding TemporalFS as a new Archetype follows the same pattern as Workflow and Scheduler – own execution, own state tree, own lifecycle.
2. **AI agents need durable state, not just durable execution.** Every agent framework (LangGraph, CrewAI, AutoGen) bolts on ad-hoc file storage. TemporalFS makes file state a native primitive – versioned, replicated, and replay-safe by construction.
3. **The storage primitives exist.** Inode-based filesystems, LSM-tree storage, layered manifests, and bloom filter indexes are proven patterns. ZeroFS proved the architecture works. We are applying the same model (inodes + chunks on an LSM-tree) within CHASM's transactional model, not inventing new storage theory.

**The result:** Any Temporal workflow can `Open()` a shared filesystem. Multiple AI agents can collaborate on the same file tree. On failure, retry, reset, or cluster failover, every participant sees consistent state. This is infrastructure that does not exist anywhere else.

---

*TemporalFS: Files that remember everything, replay perfectly, and never lose a byte.*