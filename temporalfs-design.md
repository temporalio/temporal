# Design: TemporalFS — OSS Implementation with SaaS Extensibility

**Authors:** Temporal Engineering
**Status:** Draft
**Last Updated:** 2026-03-18
**Companion:** [1-Pager](./1-pager-TemporalFS.md) | [PRD](./temporalfs.md)

---

## Table of Contents

1. [Goal](#goal)
2. [Architecture Overview](#architecture-overview)
3. [CHASM Archetype: `temporalfs`](#chasm-archetype-temporalfs)
4. [Proto Definitions](#proto-definitions)
5. [API Surface](#api-surface)
6. [Server-Side Implementation](#server-side-implementation)
7. [Pluggable Storage: FSStoreProvider](#pluggable-storage-fsstoreprovider)
8. [SaaS Extensibility: Walker/CDS Integration](#saas-extensibility-walkercds-integration)
9. [Client SDK: FUSE Mount](#client-sdk-fuse-mount)
10. [Replay Determinism](#replay-determinism)
11. [Garbage Collection](#garbage-collection)
12. [Phased Implementation Plan](#phased-implementation-plan)
13. [Directory Layout](#directory-layout)
14. [Open Questions](#open-questions)

---

## Goal

Implement TemporalFS as a CHASM archetype in the OSS Temporal server (`temporalio/temporal`). A single Go workflow and its activities can create, mount (via FUSE), read, write, and persist a durable filesystem. The FS state lives server-side in the history service, accessed by workers via gRPC.

**The storage layer is designed from day 1 as a pluggable `FSStoreProvider` interface**, so that SaaS (`temporalio/saas-temporal`) can provide a WalkerStore implementation that drops in without any changes to the FS layer or CHASM archetype. This follows the same CDS Multi-DB pattern used for HistoryStore, ExecutionStore, and other persistence backends.

**Scope (P1):**
- New CHASM archetype: `temporalfs`
- Pluggable `FSStoreProvider` interface with PebbleStore implementation (OSS)
- gRPC API for FS operations (frontend → history routing)
- Go SDK `temporalfs` package with FUSE mount
- Single-workflow ownership (multi-workflow sharing deferred to P2)
- Clear seam for SaaS WalkerStore integration (interface defined, not implemented)

**Out of scope (P1):**
- WalkerStore implementation (SaaS, separate repo)
- Multi-workflow concurrent access
- Direct-to-S3 for large chunks
- Python / TypeScript SDKs

---

## Architecture Overview

```
┌─────────────────────────────┐
│  Worker (SDK)               │
│  ┌───────────────────────┐  │
│  │ Activity              │  │
│  │  ┌─────────────────┐  │  │
│  │  │ FUSE Mount      │  │  │
│  │  │ /workspace/     │  │  │
│  │  └────────┬────────┘  │  │
│  │           │ POSIX     │  │
│  │  ┌────────v────────┐  │  │
│  │  │ temporalfs SDK  │  │  │
│  │  │ client          │  │  │
│  │  └────────┬────────┘  │  │
│  └───────────┼───────────┘  │
│              │ gRPC         │
└──────────────┼──────────────┘
               │
┌──────────────v──────────────┐
│  Frontend Service           │
│  TemporalFSHandler          │
│  (validation, routing)      │
└──────────────┬──────────────┘
               │ internal gRPC
┌──────────────v──────────────┐
│  History Service            │
│  ┌────────────────────────┐ │
│  │ CHASM Engine           │ │
│  │  ┌──────────────────┐  │ │
│  │  │ TemporalFS       │  │ │
│  │  │ Archetype        │  │ │
│  │  │                  │  │ │
│  │  │ FS Layer         │  │ │
│  │  │ (inode, chunk,   │  │ │
│  │  │  dir, snapshot)  │  │ │
│  │  │       │          │  │ │
│  │  │  store.Store     │  │ │  ← pluggable interface
│  │  │  (interface)     │  │ │
│  │  │       │          │  │ │
│  │  │  FSStoreProvider │  │ │
│  │  └───────┼──────────┘  │ │
│  └──────────┼─────────────┘ │
│       ┌─────┴─────┐         │
│       │           │         │
│  PebbleStore  WalkerStore   │
│  (OSS)        (SaaS/CDS)   │
└─────────────────────────────┘
```

**Data flow:**
1. Activity mounts FUSE at `/workspace/`
2. Agent writes files normally (`echo "hello" > /workspace/file.txt`)
3. FUSE intercepts syscalls, SDK client translates to gRPC
4. Frontend validates and routes to history shard owning the FS execution
5. History service CHASM engine applies mutation to the TemporalFS archetype
6. FS layer writes inode/chunk/dir keys to PebbleStore
7. On `close()`, FUSE flushes pending writes (close-to-open consistency)

---

## CHASM Archetype: `temporalfs`

### Component Model

```
temporalfs Archetype
│
├── Filesystem (root component)
│   ├── State: FilesystemState proto
│   │   ├── Status          (RUNNING | ARCHIVED | DELETED)
│   │   ├── Config          (chunk_size, max_size, retention)
│   │   ├── Stats           (total_size, file_count, inode_count)
│   │   ├── NextInodeID     uint64
│   │   ├── NextTxnID       uint64
│   │   └── OwnerWorkflowID string (P1: single owner)
│   │
│   ├── Visibility: chasm.Field[*chasm.Visibility]
│   │
│   └── Tasks:
│       ├── ChunkGCTask          (periodic, tombstone-based cleanup)
│       ├── ManifestCompactTask  (flatten diff chains)
│       └── QuotaCheckTask       (enforce size limits)
```

### Root Component: `Filesystem`

```go
// chasm/lib/temporalfs/filesystem.go
package temporalfs

type Filesystem struct {
    chasm.UnimplementedComponent
    *temporalfspb.FilesystemState

    Visibility chasm.Field[*chasm.Visibility]
}

func (f *Filesystem) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
    switch f.Status {
    case temporalfspb.FILESYSTEM_STATUS_ARCHIVED,
         temporalfspb.FILESYSTEM_STATUS_DELETED:
        return chasm.LifecycleStateCompleted
    default:
        return chasm.LifecycleStateRunning
    }
}
```

**Key design decision:** The FS layer data (inodes, chunks, directory entries) is NOT stored as CHASM Fields. It is stored in a dedicated PebbleDB instance managed by the TemporalFS archetype, accessed through the `Store` interface. Only the FS metadata (config, stats, lifecycle) lives in CHASM state. This avoids CHASM's per-field overhead for potentially millions of chunk keys.

### State Machine

```
                    Create
    UNSPECIFIED ──────────> RUNNING
                              │
                    Archive   │   Delete
                 ┌────────────┤────────────┐
                 v            │            v
              ARCHIVED        │         DELETED
                 │            │
                 │  Restore   │
                 └────────────┘
```

```go
var TransitionCreate = chasm.NewTransition(
    []temporalfspb.FilesystemStatus{temporalfspb.FILESYSTEM_STATUS_UNSPECIFIED},
    temporalfspb.FILESYSTEM_STATUS_RUNNING,
    func(fs *Filesystem, ctx chasm.MutableContext, event *CreateEvent) error {
        fs.Config = event.Config
        fs.NextInodeID = 2  // root inode = 1
        fs.NextTxnID = 1
        fs.Stats = &temporalfspb.FSStats{}
        fs.OwnerWorkflowId = event.OwnerWorkflowId

        // Schedule periodic GC task
        ctx.AddTask(fs, chasm.TaskAttributes{
            ScheduledTime: ctx.Now(fs).Add(fs.Config.GcInterval.AsDuration()),
        }, &temporalfspb.ChunkGCTask{})

        return nil
    },
)

var TransitionArchive = chasm.NewTransition(
    []temporalfspb.FilesystemStatus{temporalfspb.FILESYSTEM_STATUS_RUNNING},
    temporalfspb.FILESYSTEM_STATUS_ARCHIVED,
    func(fs *Filesystem, ctx chasm.MutableContext, _ *ArchiveEvent) error {
        return nil
    },
)
```

### Library Registration

```go
// chasm/lib/temporalfs/library.go
package temporalfs

type library struct {
    chasm.UnimplementedLibrary
    config     *Config
    fsService  *FSService  // gRPC service implementation
}

func (l *library) Name() string { return "temporalfs" }

func (l *library) Components() []*chasm.RegistrableComponent {
    return []*chasm.RegistrableComponent{
        chasm.NewRegistrableComponent[*Filesystem](
            "filesystem",
            chasm.WithBusinessIDAlias("FilesystemId"),
            chasm.WithSearchAttributes(
                statusSearchAttribute,
                sizeSearchAttribute,
            ),
        ),
    }
}

func (l *library) Tasks() []*chasm.RegistrableTask {
    return []*chasm.RegistrableTask{
        chasm.NewRegistrablePureTask(
            "chunkGC",
            &chunkGCValidator{},
            &chunkGCExecutor{config: l.config},
        ),
        chasm.NewRegistrablePureTask(
            "manifestCompact",
            &manifestCompactValidator{},
            &manifestCompactExecutor{},
        ),
        chasm.NewRegistrablePureTask(
            "quotaCheck",
            &quotaCheckValidator{},
            &quotaCheckExecutor{},
        ),
    }
}

func (l *library) RegisterServices(server *grpc.Server) {
    temporalfsservice.RegisterTemporalFSServiceServer(server, l.fsService)
}
```

### FX Module

```go
// chasm/lib/temporalfs/fx.go
package temporalfs

var Module = fx.Module(
    "temporalfs",
    fx.Provide(NewConfig),
    fx.Provide(
        fx.Annotate(
            NewPebbleStoreProvider,
            fx.As(new(FSStoreProvider)),  // default binding; SaaS overrides via fx.Decorate
        ),
    ),
    fx.Provide(NewFSService),
    fx.Provide(newLibrary),
    fx.Invoke(func(registry *chasm.Registry, lib *library) error {
        return registry.Register(lib)
    }),
)
```

---

## Proto Definitions

### Internal Service Proto

```
proto/internal/temporal/server/api/temporalfsservice/v1/
├── service.proto
└── request_response.proto
```

```protobuf
// service.proto
syntax = "proto3";
package temporal.server.api.temporalfsservice.v1;

service TemporalFSService {
    // Lifecycle
    rpc CreateFilesystem (CreateFilesystemRequest)
        returns (CreateFilesystemResponse);
    rpc ArchiveFilesystem (ArchiveFilesystemRequest)
        returns (ArchiveFilesystemResponse);
    rpc GetFilesystemInfo (GetFilesystemInfoRequest)
        returns (GetFilesystemInfoResponse);

    // Inode operations (used by FUSE mount)
    rpc Lookup (LookupRequest) returns (LookupResponse);
    rpc Getattr (GetattrRequest) returns (GetattrResponse);
    rpc Setattr (SetattrRequest) returns (SetattrResponse);

    // File I/O
    rpc ReadChunks (ReadChunksRequest) returns (ReadChunksResponse);
    rpc WriteChunks (WriteChunksRequest) returns (WriteChunksResponse);
    rpc Truncate (TruncateRequest) returns (TruncateResponse);

    // Directory operations
    rpc Mkdir (MkdirRequest) returns (MkdirResponse);
    rpc Unlink (UnlinkRequest) returns (UnlinkResponse);
    rpc Rmdir (RmdirRequest) returns (RmdirResponse);
    rpc Rename (RenameRequest) returns (RenameResponse);
    rpc ReadDir (ReadDirRequest) returns (ReadDirResponse);

    // Links
    rpc Link (LinkRequest) returns (LinkResponse);
    rpc Symlink (SymlinkRequest) returns (SymlinkResponse);
    rpc Readlink (ReadlinkRequest) returns (ReadlinkResponse);

    // Special
    rpc Create (CreateFileRequest) returns (CreateFileResponse);
    rpc Mknod (MknodRequest) returns (MknodResponse);
    rpc Statfs (StatfsRequest) returns (StatfsResponse);

    // Snapshots (for replay)
    rpc CreateSnapshot (CreateSnapshotRequest)
        returns (CreateSnapshotResponse);
    rpc OpenSnapshot (OpenSnapshotRequest)
        returns (OpenSnapshotResponse);
}
```

### Archetype State Proto

```
chasm/lib/temporalfs/proto/v1/
├── state.proto
└── tasks.proto
```

```protobuf
// state.proto
syntax = "proto3";
package temporal.server.chasm.temporalfs.v1;

enum FilesystemStatus {
    FILESYSTEM_STATUS_UNSPECIFIED = 0;
    FILESYSTEM_STATUS_RUNNING = 1;
    FILESYSTEM_STATUS_ARCHIVED = 2;
    FILESYSTEM_STATUS_DELETED = 3;
}

message FilesystemState {
    FilesystemStatus status = 1;
    FilesystemConfig config = 2;
    FSStats stats = 3;
    uint64 next_inode_id = 4;
    uint64 next_txn_id = 5;
    string owner_workflow_id = 6;  // P1: single owner
}

message FilesystemConfig {
    uint32 chunk_size = 1;       // default: 256KB
    uint64 max_size = 2;         // quota in bytes
    uint64 max_files = 3;        // max inode count
    google.protobuf.Duration gc_interval = 4;
    google.protobuf.Duration snapshot_retention = 5;
}

message FSStats {
    uint64 total_size = 1;
    uint64 file_count = 2;
    uint64 dir_count = 3;
    uint64 inode_count = 4;
    uint64 chunk_count = 5;
    uint64 transition_count = 6;
}
```

```protobuf
// tasks.proto
syntax = "proto3";
package temporal.server.chasm.temporalfs.v1;

message ChunkGCTask {
    // Tombstone-based GC: scan tombstone prefix, delete orphaned chunks.
    uint64 last_processed_txn_id = 1;
}

message ManifestCompactTask {
    // Flatten manifest diff chain from last checkpoint to current.
    uint64 checkpoint_txn_id = 1;
}

message QuotaCheckTask {
    // Enforce storage quotas. Triggered after writes.
}
```

### Public API Proto (for SDK)

```protobuf
// In temporalio/api repo: temporal/api/temporalfs/v1/service.proto
syntax = "proto3";
package temporal.api.temporalfs.v1;

service TemporalFSService {
    rpc CreateFilesystem (CreateFilesystemRequest)
        returns (CreateFilesystemResponse);
    rpc GetFilesystemInfo (GetFilesystemInfoRequest)
        returns (GetFilesystemInfoResponse);
    rpc ArchiveFilesystem (ArchiveFilesystemRequest)
        returns (ArchiveFilesystemResponse);

    // Mount establishes a session for FUSE I/O.
    // Returns a mount_token used to authenticate subsequent I/O RPCs.
    rpc Mount (MountRequest) returns (MountResponse);
    rpc Unmount (UnmountRequest) returns (UnmountResponse);

    // FUSE I/O operations (authenticated by mount_token)
    rpc FSOperation (stream FSOperationRequest)
        returns (stream FSOperationResponse);
}
```

**Key design decision:** The public API exposes a streaming `FSOperation` RPC for FUSE I/O. This multiplexes all POSIX operations over a single bidirectional stream, reducing connection overhead and enabling server-side batching. Each `FSOperationRequest` contains a `oneof` with the specific operation (lookup, read, write, mkdir, etc.). The frontend demuxes and routes each operation to the internal `TemporalFSService` on the correct history shard.

---

## API Surface

### Frontend Handler

```go
// service/frontend/temporalfs_handler.go
type TemporalFSHandler struct {
    temporalfsservice.UnimplementedTemporalFSServiceServer

    historyClient  historyservice.HistoryServiceClient
    namespaceReg   namespace.Registry
    config         *configs.Config
}
```

The frontend handler:
1. Validates the request (namespace exists, FS execution exists, caller authorized)
2. Resolves the FS execution's shard (same sharding as workflow executions -- by namespace + filesystem ID)
3. Routes to the history service shard via `historyClient`

### History Handler

```go
// service/history/api/temporalfs/api.go
type API struct {
    storeProvider FSStoreProvider  // pluggable: PebbleStore (OSS) or WalkerStore (SaaS)
    chasmEngine   *chasm.Engine
}
```

The history handler:
1. Loads the TemporalFS CHASM execution
2. Acquires a `store.Store` via `storeProvider.GetStore(shardID, executionID)`
3. Creates an `fs.FS` instance bound to that store
4. Executes the requested operation
5. Updates CHASM state (stats, txn ID) if mutation

### Request Flow Example: WriteChunks

```
SDK (FUSE close())
  → gRPC WriteChunks(filesystem_id, inode_id, offset, data)
  → Frontend: validate namespace + auth
  → Frontend: resolve shard for filesystem_id
  → History shard: load CHASM execution
  → History: acquire store.Store via FSStoreProvider.GetStore(shard, execID)
  → History: fs.WriteAt(inodeID, offset, data)
    → codec: build chunk keys
    → PebbleStore: batch.Set(chunk keys, chunk data)
    → PebbleStore: batch.Set(inode key, updated metadata)
    → PebbleStore: batch.Commit()
  → History: update FilesystemState.Stats (size, txn_id)
  → History: commit CHASM transaction
  → Response: success + new txn_id
```

---

## Server-Side Implementation

### FSStoreProvider (Pluggable Interface)

The FS layer communicates with storage through `temporal-fs/pkg/store.Store`. The server obtains a `store.Store` via a pluggable `FSStoreProvider` interface — the sole extension point for SaaS.

```go
// chasm/lib/temporalfs/store_provider.go
package temporalfs

import "github.com/temporalio/temporal-fs/pkg/store"

// FSStoreProvider is the pluggable interface for FS storage backends.
// OSS implements this with PebbleStore. SaaS implements with WalkerStore.
type FSStoreProvider interface {
    // GetStore returns a store.Store scoped to a specific FS execution.
    // The returned store provides full key isolation for that execution.
    GetStore(shardID int32, executionID uint64) (store.Store, error)

    // Close releases all resources (PebbleDB instances, Walker sessions, etc.)
    Close() error
}
```

This is the **only interface SaaS needs to implement**. The FS layer, CHASM archetype, gRPC service, FUSE mount — everything above this interface is identical between OSS and SaaS.

### FS Layer Integration

The existing `temporal-fs/pkg/fs` package is imported as a Go module. The server creates short-lived `fs.FS` instances per request:

```go
// chasm/lib/temporalfs/fs_ops.go

func (api *API) executeRead(ctx context.Context, fsState *Filesystem,
    store store.Store, req *ReadChunksRequest) (*ReadChunksResponse, error) {

    tfs, err := fs.OpenWithState(store, fs.StateFromProto(fsState))
    if err != nil {
        return nil, err
    }
    defer tfs.Close()

    data, err := tfs.ReadAtByID(req.InodeId, req.Offset, int(req.Size))
    if err != nil {
        return nil, err
    }

    return &ReadChunksResponse{
        Data:  data,
        TxnId: fsState.NextTxnId - 1,
    }, nil
}

func (api *API) executeWrite(ctx context.Context, fsState *Filesystem,
    store store.Store, req *WriteChunksRequest) (*WriteChunksResponse, error) {

    tfs, err := fs.OpenWithState(store, fs.StateFromProto(fsState))
    if err != nil {
        return nil, err
    }
    defer tfs.Close()

    if err := tfs.WriteAtByID(req.InodeId, req.Offset, req.Data); err != nil {
        return nil, err
    }

    // Update CHASM state with new stats
    fsState.NextTxnId = tfs.NextTxnID()
    fsState.NextInodeId = tfs.NextInodeID()
    fsState.Stats = statsToProto(tfs.Stats())

    return &WriteChunksResponse{
        TxnId: fsState.NextTxnId - 1,
    }, nil
}
```

**Key adaptation:** The existing `fs.FS` stores `NextInodeID` and `NextTxnID` in a superblock on disk. For the server integration, these values are stored in the CHASM `FilesystemState` proto instead. The FS is opened with pre-loaded state (`OpenWithState`) rather than reading the superblock from PebbleDB. This ensures CHASM is the source of truth for metadata, while PebbleDB stores only inode/chunk/directory data.

### Modified FS Package Interface

The existing `temporal-fs/pkg/fs` package needs a few additions to support server-side use:

```go
// New method: Open FS with externally-provided state (no superblock read)
func OpenWithState(store store.Store, state FSState) (*FS, error)

// FSState holds metadata normally in the superblock,
// but provided by CHASM in server mode.
type FSState struct {
    NextInodeID uint64
    NextTxnID   uint64
    ChunkSize   uint32
    RootInode   uint64
}

// Expose inode-ID-based I/O (already added per plan)
func (f *FS) ReadAtByID(inodeID uint64, offset int64, size int) ([]byte, error)
func (f *FS) WriteAtByID(inodeID uint64, offset int64, data []byte) error
func (f *FS) StatByID(inodeID uint64) (*Inode, error)

// Expose state for CHASM to persist
func (f *FS) NextTxnID() uint64
func (f *FS) NextInodeID() uint64
func (f *FS) Stats() FSStats
```

---

## Client SDK: FUSE Mount

### SDK Package

```go
// In temporalio/sdk-go: temporalfs/client.go
package temporalfs

// Create creates a new TemporalFS execution.
func Create(ctx context.Context, id string, opts CreateOptions) error

// Mount mounts a TemporalFS execution as a local FUSE filesystem.
// Returns the mount path. The mount is automatically unmounted when ctx is cancelled.
func Mount(ctx context.Context, id string, mountPoint string, opts MountOptions) (string, error)

// Unmount explicitly unmounts a TemporalFS mount.
func Unmount(mountPoint string) error

type CreateOptions struct {
    Namespace string
    ChunkSize uint32  // default: 256KB
    MaxSize   uint64  // quota in bytes
}

type MountOptions struct {
    ReadOnly    bool
    SnapshotID  string  // pin to snapshot for replay
    CacheSize   int     // worker-local chunk cache size (bytes)
}
```

### FUSE-to-gRPC Bridge

The SDK FUSE implementation translates POSIX syscalls to gRPC calls:

```go
// temporalfs/fuse_node.go (in sdk-go)
type remoteNode struct {
    gofusefs.Inode
    client   temporalfsservice.TemporalFSServiceClient
    fsID     string
    inodeID  uint64
    cache    *chunkCache
}

func (n *remoteNode) Open(ctx context.Context, flags uint32) (
    gofusefs.FileHandle, uint32, syscall.Errno) {

    return &remoteFileHandle{
        client:  n.client,
        fsID:    n.fsID,
        inodeID: n.inodeID,
        cache:   n.cache,
    }, 0, 0
}

func (n *remoteNode) Lookup(ctx context.Context, name string,
    out *fuse.EntryOut) (*gofusefs.Inode, syscall.Errno) {

    resp, err := n.client.Lookup(ctx, &LookupRequest{
        FilesystemId: n.fsID,
        ParentInodeId: n.inodeID,
        Name:          name,
    })
    if err != nil {
        return nil, toErrno(err)
    }

    child := n.NewInode(ctx, &remoteNode{
        client:  n.client,
        fsID:    n.fsID,
        inodeID: resp.InodeId,
        cache:   n.cache,
    }, gofusefs.StableAttr{
        Ino:  resp.InodeId,
        Mode: resp.Mode,
    })

    fillEntryOut(resp, out)
    return child, 0
}
```

### Write Buffering (Close-to-Open)

The SDK buffers writes in the `remoteFileHandle` and flushes to the server on `close()`:

```go
type remoteFileHandle struct {
    client  temporalfsservice.TemporalFSServiceClient
    fsID    string
    inodeID uint64
    cache   *chunkCache

    // Write buffer: accumulates dirty regions
    mu      sync.Mutex
    dirty   map[int64][]byte  // offset -> data
}

func (fh *remoteFileHandle) Write(ctx context.Context, data []byte,
    off int64) (uint32, syscall.Errno) {

    fh.mu.Lock()
    defer fh.mu.Unlock()
    fh.dirty[off] = append([]byte{}, data...)  // copy
    return uint32(len(data)), 0
}

func (fh *remoteFileHandle) Flush(ctx context.Context) syscall.Errno {
    fh.mu.Lock()
    dirty := fh.dirty
    fh.dirty = make(map[int64][]byte)
    fh.mu.Unlock()

    for off, data := range dirty {
        _, err := fh.client.WriteChunks(ctx, &WriteChunksRequest{
            FilesystemId: fh.fsID,
            InodeId:      fh.inodeID,
            Offset:       off,
            Data:         data,
        })
        if err != nil {
            return toErrno(err)
        }
        fh.cache.Invalidate(fh.inodeID, off)
    }
    return 0
}
```

### Worker-Local Chunk Cache

```go
type chunkCache struct {
    mu       sync.RWMutex
    entries  map[cacheKey][]byte
    size     int64
    maxSize  int64
    lru      *list.List
}

type cacheKey struct {
    inodeID    uint64
    chunkIndex uint64
}
```

The cache is keyed by `(inodeID, chunkIndex)`. Cache invalidation happens on write (local dirty data replaces cached chunks). Cache entries are evicted LRU when `maxSize` is exceeded.

---

## Pluggable Storage: FSStoreProvider

The `FSStoreProvider` interface is the boundary between the FS layer and the storage backend. Everything above it (FS operations, CHASM archetype, gRPC service, FUSE mount) is identical across OSS and SaaS.

### The `store.Store` Interface (from temporal-fs)

This is the interface the FS layer programs against:

```go
// temporal-fs/pkg/store/store.go
type Store interface {
    Get(key []byte) ([]byte, error)
    Set(key, value []byte) error
    Delete(key []byte) error
    DeleteRange(start, end []byte) error
    NewBatch() Batch
    NewSnapshot() Snapshot
    NewIterator(lower, upper []byte) (Iterator, error)
    Flush() error
    Close() error
}
```

Both PebbleStore and WalkerStore implement this exact interface. The FS layer never knows which backend it's talking to.

### OSS: PebbleStoreProvider

```go
// chasm/lib/temporalfs/pebble_store_provider.go
type PebbleStoreProvider struct {
    mu       sync.RWMutex
    shardDBs map[int32]*pebblestore.Store  // shard ID -> PebbleDB
    dataDir  string
}

func (p *PebbleStoreProvider) GetStore(shardID int32, executionID uint64) (store.Store, error) {
    p.mu.RLock()
    db, ok := p.shardDBs[shardID]
    p.mu.RUnlock()
    if !ok {
        return nil, fmt.Errorf("no PebbleDB for shard %d", shardID)
    }
    return store.NewPrefixedStore(db, executionID), nil
}
```

**One PebbleDB per history shard** holds all FS executions for that shard, isolated via `PrefixedStore` (8-byte partition prefix). This avoids exhausting file descriptors with thousands of PebbleDB instances.

**Storage layout on disk:**

```
{data_dir}/temporalfs/
├── shard-1/           # PebbleDB for shard 1
│   ├── MANIFEST-*
│   ├── *.sst
│   └── WAL/
├── shard-2/           # PebbleDB for shard 2
└── ...
```

**PebbleDB tuning for FS workloads:**

```go
func pebbleOptionsForFS() *pebble.Options {
    return &pebble.Options{
        Levels: []pebble.LevelOptions{
            {FilterPolicy: bloom.FilterPolicy(10)},  // bloom filters on all levels
            {FilterPolicy: bloom.FilterPolicy(10)},
            {FilterPolicy: bloom.FilterPolicy(10)},
            {FilterPolicy: bloom.FilterPolicy(10)},
            {FilterPolicy: bloom.FilterPolicy(10)},
            {FilterPolicy: bloom.FilterPolicy(10)},
            {FilterPolicy: bloom.FilterPolicy(10)},
        },
        // Chunks (0xFE prefix) naturally settle to lower levels.
        // Metadata (0x01-0x07) stays in upper levels.
        Cache: pebble.NewCache(256 << 20), // 256MB shared block cache
    }
}
```

### Key Layout (Identical Across Backends)

The FS layer uses the codec from `temporal-fs/pkg/codec`. Physical key layout varies by backend:

**PebbleStore (OSS):** PrefixedStore prepends 8-byte `partitionID`:
```
[partitionID:8B][0x01][inodeID:8B][invertedTxnID:8B]     → inode metadata
[partitionID:8B][0x02][parentID:8B][nameLen:2B][name...]  → dir entry
[partitionID:8B][0x03][parentID:8B][cookie:8B][...]       → dir scan
[partitionID:8B][0xFE][inodeID:8B][chunkIdx:8B][...]      → chunk data
```

**WalkerStore (SaaS):** Walker `wkeys` prepends shard scope:
```
[shardKey][0x01][inodeID:8B][invertedTxnID:8B]            → inode metadata
[shardKey][0xFE][inodeID:8B][chunkIdx:8B][...]             → chunk data
```

The FS layer sees keys without any prefix — both PrefixedStore and WalkerStore strip their prefixes transparently.

---

## SaaS Extensibility: Walker/CDS Integration

This section describes how `temporalio/saas-temporal` will implement `FSStoreProvider` using Walker. **No code in this section lives in the OSS repo** — it's the SaaS extension point.

### Architecture: SaaS Path

```
┌─────────────────────────────────────────────────────────┐
│  History Service (same binary as OSS + SaaS extensions) │
│                                                         │
│  CHASM Engine → TemporalFS Archetype → FS Layer         │
│                                          │              │
│                                    store.Store          │
│                                    (interface)          │
│                                          │              │
│                                  FSStoreProvider        │
│                                          │              │
│               ┌──────────────────────────┤              │
│               │                          │              │
│        PebbleStoreProvider       WalkerStoreProvider    │
│        (OSS, via fx default)     (SaaS, via fx override)│
│               │                          │              │
│          PebbleDB                  ShardClient          │
│          (local SSD)               (datanode gRPC)      │
│                                          │              │
│                                     Datanode            │
│                                     ┌────┴────┐        │
│                                   Pebble    S3 Tiering  │
│                                   (local)   (cold SSTs) │
└─────────────────────────────────────────────────────────┘
```

### WalkerStoreProvider

```go
// In saas-temporal: cds/storage/walkerstores/walker_fs_store_provider.go
package walkerstores

import (
    "github.com/temporalio/temporal-fs/pkg/store"
    "github.com/temporalio/temporal/chasm/lib/temporalfs"
)

type WalkerFSStoreProvider struct {
    shardClientFactory ShardClientFactory
}

func (p *WalkerFSStoreProvider) GetStore(
    shardID int32, executionID uint64,
) (store.Store, error) {
    shardKey := wkeys.NewShardKey(ShardspaceTemporalFS, shardID)
    client, err := p.shardClientFactory.GetClient(shardKey)
    if err != nil {
        return nil, err
    }
    return NewWalkerStore(client, shardKey, executionID), nil
}
```

### WalkerStore Adapter

Maps `store.Store` to Walker's `Reader`/`Writer`/`Batch` interfaces:

```go
// In saas-temporal: cds/storage/walkerstores/walker_fs_store.go
type WalkerStore struct {
    client      ShardClient
    shardKey    wkeys.ShardKey
    executionID uint64         // used as key scope prefix
}

func (s *WalkerStore) Get(key []byte) ([]byte, error) {
    lexKey := s.toLexKey(key)
    return s.client.Get(s.shardKey, lexKey)
}

func (s *WalkerStore) Set(key, value []byte) error {
    lexKey := s.toLexKey(key)
    return s.client.Set(s.shardKey, lexKey, value)
}

func (s *WalkerStore) Delete(key []byte) error {
    lexKey := s.toLexKey(key)
    return s.client.Delete(s.shardKey, lexKey)
}

func (s *WalkerStore) DeleteRange(start, end []byte) error {
    return s.client.DeleteRange(s.shardKey, s.toLexKey(start), s.toLexKey(end))
}

func (s *WalkerStore) NewBatch() store.Batch {
    return &walkerBatch{client: s.client, shardKey: s.shardKey, scope: s}
}

func (s *WalkerStore) NewIterator(lower, upper []byte) (store.Iterator, error) {
    iter := s.client.GetRange(s.shardKey, s.toLexKey(lower), s.toLexKey(upper), false)
    return &walkerIterator{inner: iter, scopeLen: s.scopeLen()}, nil
}

func (s *WalkerStore) NewSnapshot() store.Snapshot {
    // Walker snapshots map to datanode session pinning
    return &walkerSnapshot{client: s.client, shardKey: s.shardKey, scope: s}
}

// toLexKey prepends the executionID scope to produce a Walker wkeys.LexKey.
// This is the Walker equivalent of PrefixedStore's partition prefix.
func (s *WalkerStore) toLexKey(key []byte) wkeys.LexKey {
    return wkeys.NewTemporalFSKey(s.shardKey, s.executionID, key)
}
```

### Walker Key Encoding

```go
// In saas-temporal: walker/wkeys/temporalfs_keys.go

// NewTemporalFSKey constructs a Walker key for TemporalFS data.
// Format: [shardspace prefix][executionID:8B][fs key bytes...]
func NewTemporalFSKey(shardKey ShardKey, executionID uint64, fsKey []byte) LexKey {
    // The fsKey is the raw key from temporal-fs codec (e.g., 0x01 + inodeID + ...)
    // Walker scopes by shardKey; executionID isolates FS instances within a shard.
    buf := make([]byte, 8+len(fsKey))
    binary.BigEndian.PutUint64(buf[:8], executionID)
    copy(buf[8:], fsKey)
    return NewLexKey(ShardspaceTemporalFS, shardKey, buf)
}
```

### CDS Multi-DB Pattern (Dynamic Backend Selection)

Following the established CDS pattern for Cassandra↔Walker switching:

```go
// In saas-temporal: cds/storage/multi_db_fs_store_provider.go
type MultiDBFSStoreProvider struct {
    pebbleProvider *temporalfs.PebbleStoreProvider   // OSS fallback
    walkerProvider *WalkerFSStoreProvider             // Walker path
    isWalker       bool
}

func (m *MultiDBFSStoreProvider) GetStore(
    shardID int32, executionID uint64,
) (store.Store, error) {
    if m.isWalker {
        return m.walkerProvider.GetStore(shardID, executionID)
    }
    return m.pebbleProvider.GetStore(shardID, executionID)
}
```

The `isWalker` flag is driven by `cds.walker.WalkerGlobalMode` dynamic config — the same mechanism used for HistoryStore, ExecutionStore, and other CDS stores.

### FX Wiring (SaaS Override)

The OSS FX module provides `PebbleStoreProvider` as the default. SaaS overrides it:

```go
// OSS: chasm/lib/temporalfs/fx.go
var Module = fx.Module(
    "temporalfs",
    fx.Provide(NewConfig),
    fx.Provide(
        fx.Annotate(
            NewPebbleStoreProvider,
            fx.As(new(FSStoreProvider)),  // default binding
        ),
    ),
    fx.Provide(NewFSService),
    fx.Provide(newLibrary),
    fx.Invoke(func(registry *chasm.Registry, lib *library) error {
        return registry.Register(lib)
    }),
)

// SaaS: cds/temporalfs/fx.go (overrides the default binding)
var Module = fx.Module(
    "temporalfs-cds",
    fx.Decorate(func(
        walkerCfg *config.WalkerConfig,
        shardClientFactory ShardClientFactory,
        pebbleProvider *PebbleStoreProvider,
    ) FSStoreProvider {
        mode, _ := config.GlobalWalkerMode(walkerCfg)
        if mode == config.WalkerModeActive {
            return &MultiDBFSStoreProvider{
                pebbleProvider: pebbleProvider,
                walkerProvider: NewWalkerFSStoreProvider(shardClientFactory),
                isWalker:       true,
            }
        }
        return pebbleProvider
    }),
)
```

### Walker S3 Tiering (Automatic for FS Data)

Walker's S3 tiered storage moves cold SSTs (L4+ by default) to S3 while hot data stays on local SSD. TemporalFS benefits automatically:

```
LSM Level    Contents                              Storage
─────────    ────────                              ───────
L0-L2        Inode metadata (0x01), dir entries    Local SSD (hot)
             (0x02), manifest (0x07)
L3-L6        Chunk data (0xFE) — bulk of storage   S3 via Walker tiering (cold)
```

The FS key layout is **designed for this separation**: low-prefix metadata (0x01-0x07) is small and frequently accessed, staying in upper LSM levels. High-prefix chunk data (0xFE) is large and less frequently accessed, naturally settling into lower levels that Walker tiers to S3. No FS-specific tiering code needed.

### SaaS Directory Layout

```
temporalio/saas-temporal/
├── cds/storage/walkerstores/
│   ├── walker_fs_store_provider.go    # WalkerFSStoreProvider
│   ├── walker_fs_store.go             # WalkerStore (store.Store adapter)
│   ├── walker_fs_batch.go             # walkerBatch
│   ├── walker_fs_iterator.go          # walkerIterator (strips scope prefix)
│   ├── walker_fs_snapshot.go          # walkerSnapshot
│   └── multi_db_fs_store_provider.go  # MultiDB wrapper (Walker/Pebble switch)
│
├── cds/temporalfs/
│   └── fx.go                          # FX override: WalkerStore binding
│
├── walker/wkeys/
│   └── temporalfs_keys.go             # TemporalFS key constructors
│
└── walker/storage/
    └── (existing Walker storage engine — no changes needed)
```

---

## Replay Determinism

### How It Works

1. **Activity mounts FS** and performs file I/O
2. **On mount**, SDK records the current FS `txnID` in the workflow event history as part of the activity's scheduled event
3. **During activity execution**, all reads and writes go to the live FS
4. **On activity completion**, the final `txnID` is recorded in the activity result
5. **On replay**, the SDK sees the recorded `txnID` and mounts a read-only snapshot at that transition

### Workflow Read Access

Workflows can read FS state for branching decisions:

```go
// In a workflow function:
data, txnID, err := temporalfs.ReadFile(ctx, fsID, "/config.yaml")
// SDK records (fsID, "/config.yaml", txnID) in workflow history
// On replay, SDK reads from snapshot at txnID
```

The SDK command (`temporalfs.ReadFile`) is a workflow-side operation that:
1. Makes an RPC to the FS execution to read the file
2. Records the response and `txnID` as a workflow event
3. On replay, returns the recorded response without making the RPC

### Snapshot Retention

Snapshots are retained as long as any workflow references them:
- Activity started at `txnID=5` → snapshot at T=5 retained until activity completes
- Workflow read at `txnID=8` → snapshot at T=8 retained until workflow completes or resets past that point
- CHASM tracks referenced transitions; GC skips tombstones with `txnID >= min_referenced_txnID`

---

## Garbage Collection

### Tombstone-Based GC (CHASM Task)

```go
// chasm/lib/temporalfs/gc_task.go
type chunkGCExecutor struct {
    config *Config
}

func (e *chunkGCExecutor) Execute(
    ctx chasm.MutableContext,
    fs *Filesystem,
    attrs chasm.TaskAttributes,
    task *temporalfspb.ChunkGCTask,
) error {
    store := e.storeProvider.GetStore(ctx, fs)

    gcConfig := fslib.GCConfig{
        BatchSize:         e.config.GCBatchSize,
        MaxChunksPerRound: e.config.GCMaxChunks,
    }

    // Run one GC pass using the existing temporal-fs GC logic
    result, err := fslib.RunGCPass(store, gcConfig, task.LastProcessedTxnId)
    if err != nil {
        return err
    }

    // Update stats
    fs.Stats.ChunkCount -= uint64(result.ChunksDeleted)

    // Reschedule next GC
    ctx.AddTask(fs, chasm.TaskAttributes{
        ScheduledTime: ctx.Now(fs).Add(fs.Config.GcInterval.AsDuration()),
    }, &temporalfspb.ChunkGCTask{
        LastProcessedTxnId: result.LastProcessedTxnID,
    })

    return nil
}
```

### Manifest Compaction (CHASM Task)

Flattens the manifest diff chain when it exceeds a threshold:

```go
func (e *manifestCompactExecutor) Execute(
    ctx chasm.MutableContext,
    fs *Filesystem,
    _ chasm.TaskAttributes,
    task *temporalfspb.ManifestCompactTask,
) error {
    store := e.storeProvider.GetStore(ctx, fs)

    err := fslib.CompactManifest(store, task.CheckpointTxnId, fs.NextTxnId)
    if err != nil {
        return err
    }

    // Reschedule when diff count exceeds threshold again
    // (triggered by write path, not periodic)
    return nil
}
```

---

## Phased Implementation Plan

### Step 1: Proto Definitions

**Files:**
- `chasm/lib/temporalfs/proto/v1/state.proto` — FilesystemState, FilesystemConfig, FSStats
- `chasm/lib/temporalfs/proto/v1/tasks.proto` — ChunkGCTask, ManifestCompactTask, QuotaCheckTask
- `proto/internal/temporal/server/api/temporalfsservice/v1/service.proto` — Internal FS service
- `proto/internal/temporal/server/api/temporalfsservice/v1/request_response.proto` — Request/response types

**Deliverable:** `buf generate` produces Go bindings.

### Step 2: CHASM Archetype Registration

**Files:**
- `chasm/lib/temporalfs/filesystem.go` — Root component
- `chasm/lib/temporalfs/statemachine.go` — State transitions (Create, Archive, Delete)
- `chasm/lib/temporalfs/library.go` — Library registration
- `chasm/lib/temporalfs/fx.go` — FX module
- `chasm/lib/temporalfs/search_attributes.go` — Search attribute definitions

**Deliverable:** `temporalfs` archetype registered in CHASM registry. `go build` passes.

### Step 3: FSStoreProvider + PebbleStore Integration

**Files:**
- `chasm/lib/temporalfs/store_provider.go` — `FSStoreProvider` interface (the SaaS extension point)
- `chasm/lib/temporalfs/pebble_store_provider.go` — PebbleDB lifecycle per shard (OSS default)
- Imports `temporal-fs/pkg/store`, `temporal-fs/pkg/store/pebble`, `temporal-fs/pkg/store/prefixed`

**Deliverable:** `FSStoreProvider` interface defined. `PebbleStoreProvider` creates PebbleDB per shard + PrefixedStore per execution. SaaS can implement `WalkerFSStoreProvider` against the same interface with zero changes to FS layer.

### Step 4: FS Operations API (History Service)

**Files:**
- `chasm/lib/temporalfs/fs_service.go` — gRPC service implementation (registered on history server)
- `chasm/lib/temporalfs/fs_ops.go` — FS operation execution logic
- Imports `temporal-fs/pkg/fs` for the FS layer

**Deliverable:** All POSIX-mapped RPCs implemented. Can create FS, write files, read files, list directories via gRPC.

### Step 5: Frontend Routing

**Files:**
- `service/frontend/temporalfs_handler.go` — Public API handler
- `service/frontend/fx.go` — Wire handler into frontend service

**Deliverable:** Frontend routes TemporalFS RPCs to correct history shard. End-to-end gRPC flow works.

### Step 6: Go SDK + FUSE Mount

**Files (in sdk-go repo):**
- `temporalfs/client.go` — Create, Mount, Unmount
- `temporalfs/fuse_node.go` — FUSE node (POSIX → gRPC)
- `temporalfs/fuse_file_handle.go` — File handle with write buffering
- `temporalfs/chunk_cache.go` — Worker-local LRU cache
- `temporalfs/replay.go` — Workflow-side read command with txnID recording

**Deliverable:** Activities can `temporalfs.Mount()` and use standard file I/O. FUSE translates to gRPC. Close-to-open consistency.

### Step 7: GC Tasks + Quota Enforcement

**Files:**
- `chasm/lib/temporalfs/gc_task.go` — ChunkGC executor
- `chasm/lib/temporalfs/manifest_compact_task.go` — Manifest compaction executor
- `chasm/lib/temporalfs/quota_task.go` — Quota enforcement executor

**Deliverable:** Background cleanup runs. Storage doesn't grow unbounded.

### Step 8: Integration Tests

**Files:**
- `chasm/lib/temporalfs/temporalfs_test.go` — Unit tests for archetype
- `tests/temporalfs_test.go` — Integration tests (create FS, mount, write, read, replay)

**Deliverable:** CI green. Replay correctness verified.

---

## Directory Layout

```
temporalio/temporal/                          # OSS server
├── chasm/lib/temporalfs/
│   ├── filesystem.go                         # Root component
│   ├── statemachine.go                       # State transitions
│   ├── library.go                            # CHASM library registration
│   ├── fx.go                                 # FX module (default: PebbleStoreProvider)
│   ├── search_attributes.go                  # Search attribute defs
│   ├── store_provider.go                     # FSStoreProvider interface ← SaaS extension point
│   ├── pebble_store_provider.go              # OSS default: PebbleDB per shard + PrefixedStore
│   ├── fs_service.go                         # gRPC service (TemporalFSService)
│   ├── fs_ops.go                             # FS operation execution
│   ├── gc_task.go                            # Chunk GC CHASM task
│   ├── manifest_compact_task.go
│   ├── quota_task.go
│   ├── config.go                             # Configuration
│   ├── proto/v1/
│   │   ├── state.proto                       # FilesystemState
│   │   └── tasks.proto                       # Task protos
│   └── gen/temporalfspb/                     # Generated proto code
│
├── proto/internal/temporal/server/api/temporalfsservice/v1/
│   ├── service.proto                         # Internal FS service
│   └── request_response.proto                # Request/response messages
│
├── service/frontend/
│   └── temporalfs_handler.go                 # Frontend routing handler
│
└── service/history/
    └── (CHASM engine routes to temporalfs library automatically)

temporalio/saas-temporal/                     # SaaS extensions (separate repo)
├── cds/storage/walkerstores/
│   ├── walker_fs_store_provider.go           # WalkerFSStoreProvider (implements FSStoreProvider)
│   ├── walker_fs_store.go                    # WalkerStore (store.Store → Walker Reader/Writer)
│   ├── walker_fs_batch.go                    # walkerBatch
│   ├── walker_fs_iterator.go                 # walkerIterator
│   ├── walker_fs_snapshot.go                 # walkerSnapshot
│   └── multi_db_fs_store_provider.go         # MultiDB wrapper (Walker/Pebble switch)
├── cds/temporalfs/
│   └── fx.go                                 # FX override: WalkerStore binding
└── walker/wkeys/
    └── temporalfs_keys.go                    # TemporalFS key constructors

temporalio/sdk-go/                            # Client SDK
├── temporalfs/
│   ├── client.go                             # Create, Mount, Unmount
│   ├── fuse_node.go                          # FUSE → gRPC bridge
│   ├── fuse_file_handle.go                   # Write buffering
│   ├── chunk_cache.go                        # Worker-local LRU cache
│   └── replay.go                             # Workflow-side read commands
```

---

## Open Questions

### Storage & Architecture

1. **FS instance lifecycle vs PebbleDB lifecycle (OSS):** When a shard moves (rebalance), should we transfer the PebbleDB files, or rebuild from CHASM state? Transferring is faster but requires coordination. Rebuilding is simpler but slow for large FS executions.

2. **PebbleDB per shard vs PebbleDB per FS (OSS):** The design uses one PebbleDB per shard with PrefixedStore. An alternative is one PebbleDB per FS execution — simpler isolation but more resource overhead. Need benchmarking to validate the per-shard approach at scale (100+ FS executions per shard).

3. **`temporal-fs` as a Go module dependency:** The server will import `temporal-fs/pkg/fs` and `temporal-fs/pkg/store`. Should `temporal-fs` be vendored into the server repo, or maintained as a separate Go module? Separate module is cleaner but adds a release coordination step.

4. **Superblock elimination:** The design replaces the on-disk superblock with CHASM state. The existing `temporal-fs` code reads/writes a superblock. We need `OpenWithState()` that bypasses superblock I/O. Should this be a new constructor, or should we make the existing `Open()` accept an option to provide state externally?

### Protocol & Performance

5. **Chunk size for gRPC:** The default chunk size is 256KB. gRPC has a 4MB default message size limit. Should we stream chunks for large reads, or is single-message sufficient for most cases? (256KB per chunk × ~15 chunks = ~4MB max per read — close to the limit for moderate files.)

6. **CHASM transaction scope:** Each FS mutation updates CHASM state (stats, txnID). Should we batch multiple FUSE operations into a single CHASM transaction (e.g., batch all writes between `open()` and `close()`), or is one CHASM transaction per flush sufficient?

7. **History shard hot-spotting:** All operations for one FS execution hit the same history shard. For write-heavy FS workloads, this could become a bottleneck. Mitigation options: (a) larger shard count, (b) FS-specific sharding independent of history shards, (c) batched writes with close-to-open consistency (already planned).

### SaaS / Walker Integration

8. **Walker shardspace for TemporalFS:** Should TemporalFS data live in its own Walker shardspace (e.g., `ShardspaceTemporalFS`) or share the existing history shardspace? Separate shardspace enables independent shard scaling and prevents FS chunk data from polluting the history datanode block cache. Shared shardspace is simpler (no new shardspace to manage) but risks noisy-neighbor effects.

9. **Walker session lifecycle:** Walker uses session-per-shard with Lamport clocks. Should the `WalkerStore` adapter maintain a long-lived session per FS execution, or create sessions per request? Long-lived sessions are more efficient (avoid handshake overhead) but need cleanup on shard movement. Per-request sessions are simpler but add latency.

10. **Walker Batch semantics:** Walker's `Batch.Marshal()` serializes for replication (IU creation). The FS layer uses `store.Batch` for atomic multi-key writes. Need to verify that Walker batch commit + IU creation latency is acceptable for the FUSE write path (target: < 100ms for close-to-open flush).

11. **Walker S3 tiering readiness:** WalkerStore depends on Walker S3 tiered storage. Key questions: Can Walker S3 tiering be production-ready in time for TemporalFS SaaS launch? What is the read latency impact for cold chunk data (S3 fetch vs local SSD)? Does TemporalFS's key layout (0xFE chunks in lower levels, 0x01 metadata in upper levels) achieve the expected hot/cold separation in practice?

12. **Store.Snapshot mapping to Walker:** The `store.Store` interface includes `NewSnapshot()` for MVCC reads. Walker's snapshot semantics (datanode session pinning) differ from Pebble's lightweight in-memory snapshots. Need to validate that Walker can support efficient snapshot isolation for TemporalFS read-only mounts and replay.

---

*TemporalFS: Files that remember everything, replay perfectly, and never lose a byte.*
