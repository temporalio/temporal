# In-Process Local-First Execution with a Shared Go/WASM Kernel

## Status

**Deferred design.** This document records the investigated implementation path and its fallback.
It is not part of the active prototype and MUST NOT be implemented until explicitly selected as a
new project phase.

## Summary

The preferred path is to run the existing Go bridge server inside Core as a long-lived WASI
module. The module retains ownership of Temporal semantics, SQLite persistence, task dispatch,
timers, synchronization, fencing, and external-event relay. Core supplies only lifecycle,
filesystem access, and opaque transport between the Worker, module, and authenticated upstream
connection.

This is plausible because:

- A file-backed SQLite transaction was compiled and run successfully in a Go `wasip1` module
  under Wasmtime. The candidate [`ncruces/go-sqlite3` driver][sqlite-driver] officially tests
  `wasip1/wasm` with dot-file locking.
- Temporal already has an in-memory `net.Conn`-based gRPC factory that can replace internal service
  sockets.
- After replacing `modernc` in a temporary build probe, the next full-server WASI blocker was
  native Prometheus process collection, not History logic.
- A Rust port must recreate at least the approximately 31,000 non-test lines comprising the
  relevant History builder, workflow, Workflow Task, and Activity handlers, plus internal protos
  and semantic-parity validation.

The WASM path has a hard feasibility gate. If the real server cannot run in WASM without moving
execution semantics into Core, the project switches to the Rust/Purego design below.

## Responsibility boundary

The embedded Go server owns:

- The Frontend, History, Matching, and Worker services.
- Acquisition, ownership leases, fencing, expiry, and stale-owner invalidation.
- Per-execution JSON ownership records and the Temporal SQLite database.
- Workflow and Activity task generation, routing, long polls, and timers.
- Periodic and explicit synchronization, limits, retries, and atomic `SyncLocalExecution`.
- External-input relay and decisions to continue locally or hand execution back upstream.

Core owns only:

- Instantiating, monitoring, and terminating the Wasmtime instance.
- Holding an exclusive native lock on the state directory and preopening it into WASI.
- Moving versioned protobuf frames through bounded, nonblocking queues.
- Translating guest-requested upstream RPCs onto Core's authenticated Temporal connection.
- Adapting ordinary `WorkerClient` calls to the guest's local Frontend.
- Surfacing module readiness, logs, traps, and fatal errors.

Core MUST NOT store history, schedule Temporal timers, acquire executions, decide when to
synchronize, interpret ownership state, or recover partially synchronized state.

## Private host protocol

Add a versioned internal protobuf envelope containing a request ID, deadline, cancellation,
metadata, and an opaque RPC payload.

Host-to-guest frames contain startup configuration, local Frontend requests, upstream responses,
cancellation, or shutdown. Guest-to-host frames contain readiness, local Frontend responses,
upstream requests, structured logs, or fatal errors. Readiness reports the ABI version, server
build ID, local server ID, and database schema version. An ABI-major mismatch fails before
acquisition; minor versions may add optional fields.

The module imports three synchronous, nonblocking host functions:

1. Read the size of the next host frame.
2. Copy that frame into guest memory.
3. Copy a guest frame into the host's egress queue.

The guest drains available frames and sleeps for one millisecond when idle. No imported operation
may wait for network input: a probe confirmed that blocking WASI stdin suspends the other Go
goroutines. This limitation is also described by the [Go WASM documentation][go-wasm] and
[WASIp3 proposal][go-wasip3].

The protocol is private to Core and the embedded server. It does not replace or modify the
upstream `SyncLocalExecution` API.

## Worker interface

Replace the required flat `temporal_cli_path` with two backends:

```text
LocalFirstBackend::Embedded
LocalFirstBackend::Process { temporal_cli_path }
```

`LocalFirstOptions` retains the state directory, synchronization interval, and unsynchronized
event/byte limits. Embedded becomes the default only after parity; process mode remains explicitly
selectable during rollout. The backend choice is mirrored through the Core C bridge.

If system or namespace capability is unavailable, Core uses the normal upstream Worker path and
does not instantiate a local backend. Failure to start an explicitly selected embedded backend is
a Worker startup error; Core does not silently switch after ownership may have been acquired.

## Implementation plan

### 1. Pass the WASM feasibility gate

Create a bridge-only `wasip1` build profile in `temporal-server` and require all of the following:

- Compile the real Frontend, History, Matching, Worker, and local-execution packages without
  semantic stubs.
- Start all four services using a production in-memory RPC registry derived from the existing test
  factory, with no TCP, HTTP, metrics, or pprof listeners.
- Select `ncruces/go-sqlite3` under a WASI build tag and adapt driver registration, time formatting,
  error classification, and schema setup.
- Use one SQLite connection and rollback journaling. Keep the existing schema and per-execution
  JSON records entirely in the preopened state directory.
- Replace native-only metrics, process collection, signals, and unrelated datastore registrations
  with bridge-profile no-op or WASI adapters.
- Keep ordinary Go goroutines, server timers, and in-memory gRPC progressing while the guest uses
  only nonblocking host imports.
- Complete a real acquire → Workflow Task → Activity Task → Workflow Task → explicit synchronization
  cycle and restart successfully from the same database.

The WASM path fails the gate if it requires any of the following:

- Reimplementing History, Matching, ownership, synchronization, or timer semantics outside the Go
  server.
- Having Core drive semantic ticks or persist execution data.
- Replacing Temporal's persistence interfaces instead of supplying a compatible SQLite driver.
- Blocking host input that prevents timers or goroutines from progressing.
- Weakening whole-delta synchronization atomicity.

### 2. Package the embedded server

- Add a WASI entrypoint that starts the four-service server, guest transport, and existing bridge
  runtime.
- Add a guest `grpc.ClientConnInterface` for upstream calls. It emits opaque host frames and waits
  on Go channels; the bridge still decides which operations to perform and when.
- Route local Frontend frames through generated WorkflowService clients connected to the in-memory
  Frontend.
- Deny guest network access, preopen only the state directory, and route logs through the host
  protocol.
- Build a compressed, versioned WASM artifact in server CI. Bundle the verified artifact and its
  SHA-256/build metadata into Core; shipped binaries perform no runtime download.
- Preserve SQLite and JSON-record compatibility so process and embedded backends can open the same
  state directory sequentially, never concurrently.

### 3. Add the thin Core host

- Add Wasmtime 44 and WASI Preview 1 support to `sdk-core`, raising its Rust MSRV to 1.92.
- Instantiate the module only when `LocalFirstOptions` selects embedded mode and upstream
  capabilities permit local execution.
- Run it asynchronously with thread-safe ingress/egress queues, bounded memory, and epoch
  interruption for shutdown.
- Translate guest upstream frames through the existing Temporal client. Restrict dispatch to the
  Workflow/Admin methods required by bridge mode while preserving deadlines, cancellation, and
  metadata.
- Implement the current `WorkerClient` surface as a local Frontend proxy. Core treats the returned
  tasks as ordinary Temporal Workflow and Activity tasks.
- Retain the process implementation behind the same backend abstraction through embedded parity
  and burn-in.

### 4. Record and roll out the architecture

If implementation begins, update `architecture.md`, `protocol.md`, `semantics.md`, `decisions.md`,
and `test-plan.md` in the same changes. Initially require explicit embedded-backend selection. Make
it the default only after process and embedded modes pass the same conformance suite and agentic
demo.

## Rust/Purego fallback

If WASM fails the gate, build a separate Rust local-execution engine rather than moving server
responsibilities into Core:

- Create a Rust engine crate containing a deterministic transition layer plus SQLite, task queues,
  timers, ownership, synchronization, and relay runtime.
- Have Core depend on the crate directly and remain a lifecycle/Worker adapter.
- Expose the deterministic transition layer as a panic-contained C ABI using protobuf byte buffers
  and explicit allocation/free functions.
- Build a Linux `cdylib`; load it from the Go server with Purego and initially run it in shadow mode
  beside the existing Go transitions.
- Generate the server's internal persistence, task, History, and state-machine protos in Rust.
- Require exact event, mutable-state, generated-task, retry, and error parity against the Go test
  corpus before making the Rust implementation authoritative.

| Dimension | Go server in WASM | Rust engine through Purego |
| --- | --- | --- |
| Core responsibility | Generic VM and transport host | Generic engine and transport host |
| Semantic reuse | Existing Go server remains authoritative | Relevant server semantics must be ported |
| Persistence | Existing SQL plugin with a WASI driver | New Rust adapter and schema mapping |
| Concurrency | Single-threaded Go/WASM with nonblocking queues | Native Tokio/threading |
| Server integration | Same source used natively and in WASM | Per-platform dynamic library and FFI |
| Distribution | One portable bundled module | One library per OS/architecture |
| Validation | Backend conformance | Shadow execution and semantic equivalence |
| Relative effort | Baseline integration project | Estimated at least 3–5× plus greater validation |

[Purego][purego] supports dynamic FFI on Linux amd64/arm64 but describes itself as beta software.

## Test and acceptance plan

- WASI SQLite: schema creation, commit, rollback, duplicate classification, crash/restart, and
  process↔embedded compatibility.
- Runtime: backpressure, cancellation, idle polling, timer progress, graceful shutdown, forced
  interruption, guest panic, and malformed-frame rejection.
- Topology: prove that the guest uses no socket listener or outbound network capability.
- Backend parity: compare histories, results, handback reasons, ownership records, and upstream
  cursors for identical process and embedded executions.
- Local-first behavior: rapid ordinary Activities, explicit and periodic synchronization, and
  event/byte limits.
- Failures: bounded partition progress, resumed synchronization, lease expiry, takeover, and
  stale-owner rejection.
- Boundaries: unregistered Activities, Nexus, child workflows, and successor runs synchronize and
  hand back according to protocol v1.
- Relay: Signals, Updates, cancellation, and Queries reach the local owner without duplication.
- Atomicity: injected failures expose only the previous or complete new synchronization point.
- Compatibility: unsupported servers bypass local-first, process mode remains functional, and ABI
  mismatches fail before acquisition.
- Demo: run the existing agentic demo in embedded mode, execute ordinary timed Activities, and
  explicitly synchronize at user-input waits.

## Assumptions and defaults

- Linux x86-64 is the first Core host; the WASM artifact remains platform-neutral.
- Protocol v1 remains single-run. Children and continue-as-new/retry/cron successors hand back.
- External Activity operations remain deferred; Signals, Updates, cancellation, and Queries are
  included.
- SQLite remains inside the guest. Core's state-directory lock is only a host-resource guard.
- The guest polls its nonblocking host queue every one millisecond when idle.
- The current 8 MiB and 10,240-event unsynchronized defaults remain dynamically configurable.
- The process backend remains available until embedded conformance and demo acceptance complete.

[go-wasm]: https://go.dev/blog/wasmexport
[go-wasip3]: https://github.com/golang/go/issues/77141
[purego]: https://github.com/ebitengine/purego
[sqlite-driver]: https://github.com/ncruces/go-sqlite3/wiki/Support-matrix
