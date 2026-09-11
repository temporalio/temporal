# Architecture contract

## Objective

Allow a Core-based Worker to execute consecutive Workflow Tasks, timers, registered Activities,
and eligible children against a durable local Temporal server. The bridge synchronizes generated
history with an authoritative upstream server at safe boundaries and relays external interactions
to the current owner.

## Component responsibilities

| Component | Responsibilities |
| --- | --- |
| Application client | Uses the upstream Temporal endpoint for starts, Signals, Updates, Queries, cancellation, and visibility |
| Language SDK | Exposes `LocalFirstOptions`; provides registered workflow and Activity types to Core |
| Temporal Core | Performs capability-based activation, manages the bridge process, points Worker RPCs locally, and implements deterministic child-ID handling |
| Bridge-mode Temporal server | Polls upstream, stores baselines in SQLite and bridge recovery metadata beside it, serves normal Worker APIs, accepts coalesced explicit sync wake-ups, runs sync and lease renewal, relays external messages, and invalidates stale local executions |
| Upstream frontend/matching/history | Acquires and fences executions, persists local ownership in mutable state, accepts synchronized history, buffers external messages, expires leases, and regenerates tasks |
| Temporal CLI | Provides the thin `temporal server start-bridge` process entrypoint |

## Process and connection topology

```text
                         normal client APIs
Application client ------------------------------> Upstream frontend

Language SDK -> Temporal Core -> bridge frontend -> local history/matching
                       |                |
                       | bootstrap      +----------> SQLite
                       |                |
                       |                +----------> upstream Workflow Task poll
                       |                +----------> SyncLocalExecution
                       |                +----------> external-message long-poll
                       |
                       +-> capability discovery on the upstream Worker client
```

All bridge listeners MUST bind to loopback. Core MUST provide upstream connection material and the
registration manifest over an authenticated bootstrap endpoint rather than visible process
arguments.

### Local bridge durability

The local Temporal SQLite database stores workflow history and mutable state. Separately, the
prototype bridge keeps a stable server ID and one atomic JSON recovery record per owned execution
in the same private state directory. Each record contains its import phase, upstream identity,
raw ownership token, fencing epoch, lease expiration, sync interval, and acknowledged cursor; it
contains no workflow history. The bridge writes it before baseline import, updates it after each
accepted sync, and deletes it after release or ownership loss.

The directory is mode `0700`, files are mode `0600`, execution filenames are hashed, and a process
lock prevents concurrent use. Per-execution temp-file/fsync/rename updates are the lowest-effort
prototype store and avoid coupling to Temporal's SQLite schema. They are not transactional with the
local database, so restart recovery reconciles the two stores; a bridge-owned database table is a
reasonable longer-term replacement.

## Activation boundary

- Core MUST start bridge mode only when the Worker has `LocalFirstOptions`.
- Core MUST select the direct Worker path when the system capability, namespace capability, or
  synchronization-interval bounds do not permit local execution.
- Direct fallback MUST preserve existing Worker behavior and MUST NOT be treated as startup
  failure.
- Once bridge mode is active, Core's Workflow and Activity Worker traffic MUST use ordinary Worker
  service APIs against the bridge endpoint.

## Server repository boundary

Bridge behavior belongs in `temporal-server` so it can reuse normal frontend, history, matching,
persistence, and task-generation machinery. The bridge package MUST be selectable through server
construction options and MUST NOT depend on CLI command packages.

`temporal-cli` adds only the bridge command, argument validation, server construction, and process
lifecycle. Protocol and execution logic MUST remain reusable without the CLI.

### Selected History synchronization boundary

`SyncLocalExecution` is a purpose-built active-History operation for both local and global
namespaces. It runs under the workflow lock and uses the ordinary active workflow update as its
single logical commit point. It reuses policy-neutral raw-history decoding, event application, and
mutable-state rebuilding code, but not NDC's passive/conflict-oriented transaction managers.

For a global namespace, the final active transaction generates the same downstream replication
tasks as an ordinary active workflow update. An upstream local namespace needs no promotion or
cluster-wide global-namespace configuration. The prototype bridge server enables global-namespace
support internally so it can reuse `ImportWorkflowExecution` for the baseline, but keeps the
mirrored namespace local.

Protocol v1 accepts one existing workflow run per request. It validates and applies the complete
bounded delta in memory, discards intermediate-state tasks, refreshes final-state tasks according
to retain/release intent, and submits all event batches with one final mutable-state mutation. A
non-empty sync forks history at the acknowledged cursor and publishes that branch only with the
final compare-and-set, so failed attempts cannot obstruct a fresh retry.
Continue-as-new and child execution synchronization require a later protocol version.

## Data authority

- Upstream persistence is authoritative for all synchronized history.
- A valid, unexpired mutable-state lease gives one bridge authority to append a local unsynchronized
  tail.
- SQLite is authoritative for that tail only while its lease epoch remains valid.
- A higher upstream fencing epoch permanently invalidates every tail created under an older epoch.
- Ownership metadata MUST remain outside workflow history.
- Per-execution bridge JSON is recovery/control state only; it does not replace the local Temporal
  database as the authority for the unsynchronized history tail.
- External mutation requests remain durable upstream until synchronized history proves their local
  application.

## Prototype trust boundary

The prototype accepts history from an authenticated official bridge and reuses trusted history
import machinery. The upstream authorization policy MUST gate these APIs independently from normal
Worker access, and the feature MUST be disabled by default.

Cloud and untrusted multi-tenant enablement require a later validation layer or a server-verifiable
command/result journal.

## Repository work map

| Repository | Planned work |
| --- | --- |
| `temporal-api` | Public capabilities, poll extensions, synchronization/message RPCs, failure detail, SDK completion metadata |
| `temporal-api-go` | Generated Go bindings consumed by the server while public API changes are under development |
| `temporal-server` | Persistence proto, acquisition, leases, task suppression, import/sync handler, bridge mode, external-message relay |
| `temporal-cli` | `server start-bridge` command and bridge-mode server startup |
| `temporal-sdk-rust` | `LocalFirstOptions`, process/bootstrap lifecycle, registration manifest, child-ID rewrite/lookahead, C bridge exposure |

## Current steel-thread topology

The focused server regression is intentionally contained in `temporal-server`:

```text
start workflow                          normal Worker APIs
test client ------> upstream               Go test Worker ------> local server
                      ^                                           |
                      | SyncLocalExecution                        |
                      +---------------- raw tail -----------------+
                      |                                           ^
                      +---------------- raw baseline/import ------+
```

`service/localexecution` contains the baseline importer, guarded synchronizer, durable state store,
authenticated bootstrap server, and reusable bridge runtime. The frontend-exposed internal
AdminService carries `SyncLocalExecution`; the owning History shard validates and applies one
bounded delta through the selected copy-on-write active-History transaction.

The focused test is not bridge mode: it has no Core process management, upstream Worker poll,
ownership lease, durable local database, external-message path, or CLI entrypoint. Those boundaries
remain as described above.

The cross-process steel thread adds the Core and deployment boundaries with poll-based execution
acquisition:

```text
Core integration test       bridge process                         upstream process

raw Core Worker ------------> local frontend/history/matching
                                      |             |
                                      |             +-------------------> local.sqlite
                                      |
                                      +--- PollWorkflowTaskQueue -------> upstream frontend
                                      +--- Admin SyncLocalExecution ---> upstream frontend
Core test client -------------------------------------------------------> upstream frontend
```

The same executable has separate `upstream` and `bridge` modes, but the modes run in independent OS
processes. The upstream History shard persists canonical ownership in mutable state; the bridge
also writes the returned credential and cursor to its local recovery record before importing the
baseline. It verifies the import before advertising its local frontend, persists local history in
SQLite, and connects to upstream only through frontend APIs.
Core has no knowledge of acquisition, baseline import, lease renewal, or synchronization and uses
only ordinary Worker APIs against the bridge frontend.

Phase 3 adds the supported process topology around that steel thread. Core conditionally starts the
thin CLI entrypoint, authenticates bootstrap, provides its effective static upstream profile and
final registration manifest, and swaps ordinary Worker traffic to the returned loopback frontend.
The bridge runtime reloads durable ownership records before acquisition polling and revalidates
each unexpired epoch before resuming. Local Workflow Task completion detects unregistered or
differently routed Activities, Nexus operations, and external workflow commands, pauses in the
same transaction, and causes prompt synchronization/release. Released and lost local executions
are invalidated and deleted before same-run reacquisition.

The runtime can also receive an in-process explicit synchronization request for a managed
execution. The request only wakes the existing controlled loop: the bridge still owns pause,
history reads, `SyncLocalExecution`, cursor advancement, lease handling, and resume. The coding-
agent demo exposes that wake-up on a demo-only loopback endpoint and embeds separate official
Temporal UI servers for the local and upstream frontends through a same-origin reverse proxy.

D037 size-driven backlog accounting, inbound external-message and Query relay, force revoke,
children, and successor-run transitions remain unimplemented.

## Deferred architecture

The shared-library path would replace the bridge process with an embedded host while preserving the
same upstream acquisition, synchronization, relay, lease, and fencing protocol. It is not part of
the first prototype.
