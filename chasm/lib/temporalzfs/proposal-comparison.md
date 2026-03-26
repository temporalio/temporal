# Durable Filesystem Proposals — Comparison

Three proposals for durable filesystem state in Temporal agent workflows, compared honestly across dimensions that matter for the CTO decision.

## Proposals at a Glance

### TemporalZFS (Moe)
Server-side CHASM archetype backed by a transactional copy-on-write filesystem (PebbleDB + MVCC). Each workflow gets an isolated, versioned filesystem accessible via gRPC. No SDK changes, no FUSE, no external dependencies. FS operations are server RPCs — the filesystem lives next to the execution, not on the worker.

### Durable Workspace (Yimin)
SDK-side FUSE overlay filesystem with incremental diffs stored in External Payload Storage (S3/GCS). Activities see a real local directory (`activity.WorkspacePath(ctx)`). Changes are captured as chunked tar diffs, uploaded on activity completion, and lazily reconstructed on any worker. Requires proto changes + server-side workspace state tracking.

### Durable Bash (Sergey)
Dedicated system workers running ZFS + Firecracker/gVisor sandboxes. Workflows call `bash.exec(ctx, cmd)` to run untrusted code in isolated microVMs with per-workflow ZFS datasets. Snapshots stored in S3 via `zfs send`. Orchestrated by a CHASM sub-component. Targets bare-metal infrastructure at scale.

---

## Architecture Comparison

| Dimension | TemporalZFS | Durable Workspace | Durable Bash |
|-----------|-------------|-------------------|--------------|
| **Where FS lives** | Server-side (CHASM + PebbleDB) | Worker-side (FUSE overlay + S3) | Dedicated bash workers (ZFS + S3) |
| **SDK changes** | Additive client library (no core SDK/proto changes) | Core proto changes + SDK WorkspaceManager | Core proto changes + CHASM entity + new SDK API |
| **Server changes** | CHASM library (self-contained) | Workspace state in ExecutionInfo, proto additions | CHASM sub-component, system task queues |
| **External dependencies** | None (PebbleDB embedded) | FUSE (libfuse3 / macFUSE) | ZFS + Firecracker or gVisor |
| **Platform** | Any (pure Go) | Linux + macOS | Linux only (bare metal recommended) |
| **Sandboxing** | None | None | Full (Firecracker microVM or gVisor) |
| **Root / privileges** | No | No (FUSE userspace) | Yes (ZFS + VM management) |

---

## Storage & Efficiency

| Dimension | TemporalZFS | Durable Workspace | Durable Bash |
|-----------|-------------|-------------------|--------------|
| **Versioning granularity** | Chunk-level MVCC (256KB default) | File-level diffs (tar) | Block-level COW (ZFS) |
| **Write amplification (1-byte append to 1MB file)** | Up to 1 chunk (256KB worst case) | File-level: stores the changed file | Block-level: ~128KB (ZFS recordsize) |
| **Read performance** | O(1) per chunk — direct KV lookup | O(1) after lazy load — local disk | O(1) — local ZFS read |
| **First read on new worker** | Instant (FS co-located with server) | Lazy: download manifest (~50KB) then chunks on demand | Reconstruct: download + `zfs receive` all diffs |
| **Point-in-time snapshots** | Instant (MVCC txnID) | Requires diff chain replay | Instant (`zfs snapshot`) |
| **Storage backend** | PebbleDB (local SSDs); pluggable for SaaS | S3/GCS via External Payload Storage | S3 for diffs, local ZFS for live data |
| **Client-side encryption** | Not addressed yet | Full support (per-chunk encryption via PayloadCodec) | ZFS native encryption |

---

## Operational Characteristics

| Dimension | TemporalZFS | Durable Workspace | Durable Bash |
|-----------|-------------|-------------------|--------------|
| **Worker setup** | None — FS is server-side | `worker.Options{WorkspaceManager: ...}` | Dedicated bare-metal fleet with ZFS + Firecracker |
| **Failover cost** | Zero — data on server, not worker | Lazy: manifest download only (~50KB) | Reconstruct from S3 diffs (proportional to history) |
| **Scalability model** | Scales with server shards + PebbleDB capacity | Scales with S3 + worker fleet | Scales with dedicated bash worker fleet |
| **Resource isolation** | Per-filesystem quotas (size, files, IOPS via PebbleDB) | None built-in | Full: CPU, memory, disk, IOPS, network per sandbox |
| **GC / lifecycle** | Automatic (ChunkGC, OwnerCheck, DataCleanup tasks) | Manual cleanup needed (known FUSE mount leak issue) | CHASM-managed lifecycle (planned, not yet implemented) |
| **Infrastructure cost** | Shares server infrastructure | Shares worker infrastructure + S3 | Dedicated bare-metal fleet |

---

## Developer Experience

| Dimension | TemporalZFS | Durable Workspace | Durable Bash |
|-----------|-------------|-------------------|--------------|
| **Workflow code** | `tzfs.WriteFile(ctx, fsID, path, data)` via gRPC | `workflow.ExecuteActivity(ctx, MyActivity)` — no FS API in workflow | `bash.exec(ctx, "gcc main.c -o main")` — direct from workflow |
| **Activity code** | Open FS handle, read/write, close | `os.ReadFile(activity.WorkspacePath(ctx) + "/file.txt")` — native OS calls | N/A — bash commands run in sandbox, not in activity code |
| **External tool compat** | Must use FS API (no native path) | Full — real directory, `gcc`, `git`, etc. work unmodified | Full — real sandbox filesystem, any command works |
| **Learning curve** | New gRPC API for FS operations | Minimal — just `activity.WorkspacePath(ctx)` | New `bash.exec` API, template management |
| **Multi-language SDK** | Any language with gRPC (API-first) | Go SDK first; other SDKs need WorkspaceManager port | Go SDK first; other SDKs need CHASM integration |

---

## Maturity & Implementation Status

| Dimension | TemporalZFS | Durable Workspace | Durable Bash |
|-----------|-------------|-------------------|--------------|
| **Core implementation** | Complete: 22 gRPC RPCs, CHASM lifecycle, GC, quotas | Complete: SnapshotFS interface, 4 backends, chunked lazy loading | Prototype: exec, snapshots, S3 save/restore, templates |
| **Tests** | Unit + integration + research-agent demo (200+ workflows) | Unit tests for manifests, workspace manager, tar diff, FUSE | Benchmarking metrics; no test suite visible |
| **Known issues** | None critical | FUSE mount leak, activity timeout dirty state | No failover, stickiness, or GC yet |
| **Demo-ready** | Yes (live dashboard, HTML reports) | Yes (end-to-end lazy loading demo with Lima VM) | Partial (exec works, lifecycle/failover incomplete) |
| **Production blockers** | S3/external storage for OSS (PebbleDB is local) | FUSE mount cleanup, pre-activity rollback | Failover, stickiness, GC, full CHASM integration |

---

## Strengths & Weaknesses — Honest Assessment

### TemporalZFS

**Strengths:**
- Simplest architecture: no FUSE, no VMs, no external storage dependencies
- Instant snapshots and O(1) reads via MVCC — no reconstruction or diff replay
- Server-side means zero failover cost for workers — data doesn't live on workers
- Any-platform, any-language — client library is a thin gRPC wrapper, not a deep SDK integration
- Self-contained CHASM library with full lifecycle management (GC, quotas, owner tracking)
- Already integrated into the history service alongside other CHASM archetypes

**Weaknesses:**
- Cannot run native tools (gcc, git, npm) directly on the filesystem — no real mount path
- Activities must use the gRPC API, not standard POSIX file I/O
- PebbleDB is local to the server; no built-in replication to S3/GCS for OSS deployments
- No sandboxing — untrusted code cannot be safely executed
- Chunk-level MVCC has higher write amplification than diff-based approaches for tiny changes
- No lazy loading — all data must be on the server's local disk

### Durable Workspace

**Strengths:**
- Activities see a real local directory — external tools work unmodified
- Lazy loading is genuinely clever: 500MB workspace → download only the 8MB you touch
- Client-side encryption fully solved (per-chunk PayloadCodec pipeline)
- Multiple SnapshotFS backends (TarDiffFS, FuseOverlayFS, ZFS, Btrfs) with cross-compatible diff format
- Reuses existing External Payload Storage infrastructure — no new storage system
- Clean developer experience: `activity.WorkspacePath(ctx)` is all you need

**Weaknesses:**
- FUSE dependency limits platforms and adds operational complexity (mount leaks, stale mounts after crashes)
- Diff reconstruction on new workers adds latency (even lazy mode has per-chunk download cost)
- Known unresolved issues: FUSE mount leak, activity timeout dirty workspace state
- Sequential consistency only — one activity at a time per workspace
- Server-side changes required (proto additions, workspace state in ExecutionInfo)
- Go SDK only for now; porting WorkspaceManager to other SDKs is non-trivial

### Durable Bash

**Strengths:**
- Only proposal with real sandboxing (Firecracker/gVisor) — critical for untrusted AI-generated code
- ZFS provides best-in-class block-level COW with instant snapshots, compression, encryption
- Resource isolation (CPU, memory, disk, IOPS, network) per workflow — true multi-tenancy
- `bash.exec(ctx, cmd)` from workflow code is the most natural API for agent workloads
- Template system (Docker images → SquashFS) provides reproducible environments
- Designed for scale: millions of parallel executions on dedicated infrastructure

**Weaknesses:**
- Heaviest infrastructure requirements: bare metal, ZFS, Firecracker — highest operational cost
- Linux only, root/privileges required
- Least mature: no failover, stickiness, or GC implementation yet
- Dedicated bash worker fleet is a separate infrastructure to operate and scale
- ZFS send/receive for S3 diffs is opaque — harder to inspect/debug than tar-based diffs
- Reconstruction on failover requires full diff chain replay

---

## When Each Approach Wins

| Use Case | Best Fit | Why |
|----------|----------|-----|
| **Durable state for workflow activities (structured data, reports, intermediate results)** | TemporalZFS | Server-side, zero failover cost, instant snapshots, no worker dependencies |
| **Activities that run external tools (compilers, linters, git)** | Durable Workspace | Real local directory, native POSIX I/O, lazy loading for large workspaces |
| **Running untrusted/AI-generated code** | Durable Bash | Only option with sandboxing and resource isolation |
| **Multi-language SDK support** | TemporalZFS | Standalone gRPC service — client library is thin and portable; others need deep SDK integration |
| **Large binary files with small mutations** | Durable Bash (ZFS) | Block-level COW is most efficient for sub-file changes |
| **Client-side encryption requirement** | Durable Workspace | Fully designed and implemented with PayloadCodec pipeline |
| **Minimal infrastructure / quick adoption** | TemporalZFS | No FUSE, no VMs, no dedicated fleet — just the server |
| **Hostile multi-tenant environment** | Durable Bash | Firecracker provides VM-level isolation per execution |

---

## Are These Complementary or Competing?

These proposals optimize for different points in the design space:

- **TemporalZFS** optimizes for **simplicity and read performance** — a server-side primitive that "just works" with zero worker setup, at the cost of not providing a real mount path.
- **Durable Workspace** optimizes for **developer experience and tool compatibility** — real directories that external tools can use, at the cost of FUSE complexity and worker-side state management.
- **Durable Bash** optimizes for **security and isolation** — sandboxed execution of untrusted code, at the cost of infrastructure complexity and operational overhead.

A layered architecture is plausible: TemporalZFS as the durable storage primitive (server-side, always available), with Durable Workspace or Durable Bash as higher-level abstractions that use it or complement it for specific use cases. For example, TemporalZFS could serve as the backing store while a FUSE layer on the worker provides the real mount path, or Durable Bash could snapshot to TemporalZFS instead of raw S3.

The CTO question is: **which layer does Temporal invest in first?**
- If the priority is a **platform primitive** that all SDKs can use immediately → TemporalZFS
- If the priority is **agent developer experience** with real filesystems → Durable Workspace
- If the priority is **safe execution of untrusted code** → Durable Bash
