# Decision log

This file records the current accepted contract. Remove or revise entries when a decision is
reverted; Git history preserves the discarded rationale without leaving contradictory requirements
for future implementation sessions.

## Accepted decisions

### D001: Use an out-of-process server for the first prototype

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** Run a full local Temporal server with durable SQLite before attempting a shared
  Rust/WASM execution library.
- **Reason:** This maximizes reuse of existing server semantics and is the lowest-effort path to a
  meaningful prototype.

### D002: Put bridge behavior in `temporal-server`

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** Implement upstream polling, local persistence, synchronization, relay, and lease
  handling as a reusable server mode. The CLI adds a thin `server start-bridge` command.
- **Reason:** CLI already embeds the server and should not become the owner of server behavior.

### D003: Activate through Worker-local options

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** Start bridge mode only when `LocalFirstOptions` is present in Worker options.
  Application clients continue using upstream.
- **Reason:** Local-first is a Worker execution policy, not a namespace-wide client routing change.

### D004: Fall back when capability is unavailable

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** If the system/namespace capability is absent or interval limits are incompatible,
  use the normal direct Worker path without an error.
- **Reason:** New Core versions must remain compatible with older and feature-disabled servers.

### D005: Support Core-based SDKs first

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** Implement Rust/Core and the C bridge used by TypeScript, Python, .NET, and Ruby.
  Defer Go and Java.
- **Reason:** These SDKs can share process management, routing, replay, and protocol behavior.

### D006: Reuse `PollWorkflowTaskQueue`

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** Add optional local-execution request/response information to ordinary Workflow Task
  polling instead of adding a distinct acquisition RPC.
- **Reason:** Existing task-queue routing, versioning, identity, history delivery, and long-poll
  behavior already match acquisition needs.

### D007: The bridge owns the local-first control protocol

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** The bridge polls upstream, persists the baseline, invokes `SyncLocalExecution`, and
  relays external events. Core processes normal-looking tasks against the bridge.
- **Reason:** This keeps server execution semantics together and minimizes Core-specific protocol
  knowledge.

### D008: Persist ownership only in mutable state

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** Store local owner, token hash, fencing epoch, lease, cursors, and inbox in persistence
  mutable state. Do not emit ownership history events.
- **Reason:** Ownership is server coordination metadata and should not bloat or affect SDK replay
  history.

### D009: Persist the baseline before handing work to Core

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** The bridge drains and stores the upstream baseline in SQLite before starting the
  local Workflow Task and resolving Core's poll.
- **Reason:** A crash cannot leave Core processing work the bridge cannot recover.

### D010: Use safe-boundary synchronization

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** `sync_interval` becomes due on a wall-clock deadline and synchronizes after the
  current atomic history transaction. Event/byte caps and remote operations can require it sooner.
- **Reason:** This bounds unsynchronized history without interrupting a history transaction.

### D011: Derive a renewable lease from the interval

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** Request an ownership lease of exactly `3 * sync_interval`. Every accepted
  ownership-retaining `SyncLocalExecution`, including an empty request, renews it using upstream
  time. A releasing sync clears the lease at its atomic commit.
- **Reason:** Two missed synchronization windows provide takeover tolerance without a second Worker
  configuration value.

### D012: Pause at an unavailable synchronization boundary

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** Once synchronization is required and unavailable, stop dispatching work and
  advancing timers. Resume only after the current epoch is successfully synchronized and renewed.
- **Reason:** Offline progress must stay within the user-selected time/size bound.

### D013: Expire ownership automatically

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** Upstream expires an unrenewed lease, increments the epoch, reapplies retained inputs,
  and regenerates work. Expiration takes the workflow lock, so it serializes with an in-flight
  single-request synchronization. Once the lock is acquired, committed mutable state identifies
  either the complete old sync point or the complete new one; no durable intermediate sync state
  requires recovery. A stale bridge permanently abandons its local tail when informed.
- **Reason:** A dead bridge must not lock a workflow indefinitely, and histories from two owners
  must never merge.

### D014: Keep explicit force revoke

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** Provide an operator API to perform the lease-expiry transition before its timer. It
  takes the workflow lock and therefore waits for any in-flight synchronization request to finish
  or fail before transferring ownership.
- **Reason:** Operators need a deliberate early-recovery path.

### D015: Trust the bridge for the prototype

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** Allow a separately authorized official bridge to submit raw history through guarded
  import machinery. Keep the feature off by default and out of Cloud.
- **Reason:** Full event or command validation would substantially expand the first prototype.

### D016: External mutation calls wait for local application

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** Persist mutations in an upstream inbox, relay them to the owner, and wait for local
  SQLite commit. Retain them upstream until synchronized history proves application.
- **Reason:** This preserves upstream durability while exposing current local execution behavior.

### D017: Queries execute only on the current owner

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** Relay Queries to the bridge and local Worker. Do not execute against synchronized
  upstream history.
- **Reason:** A Query must observe the current workflow state, for which there is exactly one owner.

### D018: Use long-polls for bridge relay

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** Use repeated long-polls plus response RPCs rather than a new streaming transport.
- **Reason:** This matches existing Temporal Worker networking and works through NAT without inbound
  connectivity.

### D019: Allow registered child workflows locally

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** A later child-capable protocol MAY allow eligible child workflows to be created
  locally and MUST append a stable bridge identifier to their physical workflow IDs. Protocol v1
  treats child start as an unsupported local boundary and does not synchronize child executions.
- **Reason:** This extends useful offline progress while avoiding ID conflicts between local
  servers.

### D020: Record the child-ID namespace in WFT completion metadata

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** Store bridge ID and encoding version in the first local
  `WorkflowTaskCompletedMetadata`. Use existing replay lookahead before child-command matching; do
  not add a marker event. This metadata is introduced with the later child-capable protocol, not
  synchronization protocol v1.
- **Reason:** WFT metadata already exists for Core replay behavior and avoids an additional history
  event.

### D021: Limit offline starts to children

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** Top-level workflows start upstream. When a later child-capable protocol is enabled,
  registered children are the only executions that may be created locally. Protocol v1 creates no
  execution locally.
- **Reason:** General offline starts require additional workflow-ID reservation and routing
  semantics.

### D022: Treat metrics and UI as follow-up work

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** Add focused implementation logs but defer production metrics, dashboards, alerts,
  Describe fields, and Web UI changes.
- **Reason:** They are unnecessary for proving execution and recovery semantics.

### D023: Require persistent bridge storage

- **Date:** 2026-09-02
- **Status:** Accepted
- **Decision:** Bridge mode requires file-backed SQLite and a stable state directory/server ID.
- **Reason:** In-memory state cannot uphold recovery or deterministic child-ID contracts.

### D024: Reuse import for the baseline and replication for the steel-thread delta

- **Date:** 2026-09-02
- **Status:** Accepted for the feasibility steel thread
- **Decision:** Read raw history and version history with
  `GetWorkflowExecutionRawHistoryV2`, create the local execution with
  `ImportWorkflowExecution`, then read from the imported event/version cursor and extend the
  upstream current branch with internal `ReplicateEventsV2` calls.
- **Reason:** These existing DataBlob and VersionHistory paths prove that server-produced local
  history can become replayable upstream history without first defining a public API. Importing a
  completed copy over an existing execution was tested and did not advance its current branch;
  replication does.
- **Scope:** This is an unfenced, non-atomic test scaffold. It may remain as implementation evidence
  but MUST NOT define the product sync path. The real protocol requires guarded, all-or-nothing,
  idempotent `SyncLocalExecution` with ownership, limits, and release semantics.

### D025: Use the Go SDK only as the server steel-thread harness

- **Date:** 2026-09-02
- **Status:** Accepted for the feasibility steel thread
- **Decision:** Use the repository's Go SDK dependency to drive ordinary Workflow and Activity
  APIs against the local test server.
- **Reason:** It gives the server spike a small, directly runnable multi-Workflow-Task and
  multi-Activity workload. It does not add local-first behavior to the Go SDK or alter the
  Core-first product rollout in D005.

### D027: Retry ordered replication resend responses

- **Date:** 2026-09-02
- **Status:** Accepted for the feasibility steel thread
- **Decision:** When `ReplicateEventsV2` returns the typed `RetryReplication` failure to request
  event resend, retry the same raw batch in place after a short delay. Recognize both the in-process
  error and its gRPC status-details representation. Do not retry unrelated `ABORTED` failures, send
  a later batch, or advance the source cursor until the current batch succeeds; cancellation stops
  the retry.
- **Reason:** Across a real process boundary, the next synchronous RPC can reach History before the
  preceding replication task has been applied. The existing response explicitly requests resend
  and is not a terminal history conflict.

### D028: Introduce the first sync transport on internal AdminService

- **Date:** 2026-09-03
- **Status:** Accepted for the prototype
- **Decision:** Add experimental `SyncLocalExecution` request and response messages to the
  frontend-exposed internal `AdminService`. The prototype synchronizes one execution, is authorized
  by AdminService access, and validates the ownership token, fencing epoch, lease, previously
  acknowledged event/version cursor, and request fingerprint.
- **Reason:** The standalone bridge and upstream server already share this versioned server API.
  This replaces direct access to the upstream History listener without forcing a public API and
  generated-client rollout across every SDK before the server protocol is proven. A production
  endpoint can move to a dedicated experimental or public service without changing Core's normal
  Worker traffic.

### D031: Use public poll fields and one-second-to-one-minute prototype intervals

- **Date:** 2026-09-03
- **Status:** Accepted for the prototype
- **Decision:** Add `local_execution_options` to `PollWorkflowTaskQueueRequest`,
  `local_execution_info` to its response, a system capability, and namespace capability plus
  minimum/maximum interval limits. Protocol version 1 accepts synchronization intervals from one
  second through one minute and requires a lease of exactly three intervals. Matching forwarding
  must preserve the extension, including across task-queue partitions.
- **Reason:** Reusing ordinary polling retains routing and history delivery while explicit limits
  let a bridge decide whether it can participate before acquisition. These initial bounds are
  conservative and dynamically configurable.

### D032: Store token hashes and drive takeover with Workflow Task timeout tasks

- **Date:** 2026-09-03
- **Status:** Accepted for the prototype
- **Decision:** Return a 32-byte opaque ownership token only in the acquisition response and store
  its SHA-256 digest in `WorkflowExecutionInfo.LocalExecutionInfo`. Create a schedule-to-start
  Workflow Task timer at lease expiration. A stale timer reschedules itself to the renewed
  expiration. The current timer takes the workflow lock, observes either the old or new complete
  synchronization point, clears ownership, and causes the pending Workflow Task to time out and
  regenerate. A matching task that remains available may instead be reacquired after expiry without
  adding the timeout event.
- **Reason:** Mutable-state-only ownership avoids replay-visible coordination events. Reusing the
  existing timer/task regeneration path avoids shard scans and gives process, network, and disk
  loss the same bounded recovery mechanism.

### D034: Release ownership with terminal synchronization

- **Date:** 2026-09-03
- **Status:** Accepted for the prototype
- **Decision:** When the last synchronized event closes the workflow, the standalone bridge sets
  `release` on `SyncLocalExecution`. The all-or-nothing sync commit publishes the terminal delta,
  advances the cursor, records the request fingerprint, clears the token digest and expiration, and
  marks ownership unowned. An exact retry of the release remains idempotent.
- **Reason:** Completed workflows must not retain or renew a useless lease. The same release bit can
  later support running handback after task regeneration is implemented for remote boundaries.

### D036: Use an active-History sync path for both namespace types

- **Date:** 2026-09-04
- **Status:** Accepted
- **Decision:** Implement `SyncLocalExecution` as a purpose-built active-History operation that
  supports both local and global namespaces. Reuse policy-neutral raw-history decoding,
  event-application, and mutable-state rebuilding primitives, but do not route synchronization
  through NDC's passive/conflict-oriented transaction managers. A global namespace uses its normal
  active transaction behavior, including downstream replication-task generation after commit.
- **Reason:** Promoting one namespace is supported, but only after a cluster-wide, one-way global-
  namespace configuration rollout. Promotion also does not remove the required active-owner,
  atomicity, ownership, or final-task-generation work. The active path gives existing namespaces
  easy adoption and models the actual same-cluster ownership transfer.

### D037: Make synchronization limits dynamically configurable

- **Date:** 2026-09-03
- **Status:** Accepted
- **Decision:** Add dynamic-config limits for at least serialized synchronization bytes, event
  count, batch count, and per-execution unsynchronized bytes/events. Use conservative implementation
  defaults initially: 8 MiB serialized history, 10,240 events, and 256 batches per sync; matching
  8 MiB/10,240-event per-execution unsynchronized hard caps; and a bridge early-sync watermark at
  75 percent of either backlog cap. Choosing and tuning those numeric defaults does not require
  another product decision. The bridge MUST attempt synchronization before a hard limit and pause
  at a safe boundary if it cannot synchronize without exceeding one.
- **Reason:** A synchronization interval does not bound work produced during that interval. Dynamic
  limits bound transport, persistence transactions, memory, and disk while allowing deployments to
  tune them as the implementation matures.

### D038: Commit each delta with one bounded active workflow update

- **Date:** 2026-09-04
- **Status:** Accepted
- **Decision:** Protocol-v1 `SyncLocalExecution` validates and applies its complete bounded delta in
  memory under the workflow lock, then calls the ordinary active `UpdateWorkflowExecution` path
  exactly once with every history batch and one final mutable-state mutation. That mutation contains
  the acknowledged cursor, sync ID/hash, final-state tasks, and retained or released ownership.
  History-node appends may precede the mutable-state compare-and-set as they do for ordinary History
  writes. Each non-empty sync forks the committed history branch at the acknowledged cursor and
  appends the delta to that copy-on-write branch. A failed pre-CAS attempt leaves the old mutable
  state, branch token, bounds, and cursor authoritative; the attempted branch remains unreachable
  and is trimmed later. A retry forks afresh from the committed branch. A successful CAS switches
  the branch and exposes the entire new delta. Ambiguous errors evict/reload mutable state, and
  exact retry uses the stored sync fingerprint. Protocol v1 has no durable `pending_sync`,
  `SYNCING`, or `ROLLING_BACK` state.
- **Reason:** A new owner must never replay or extend a partially synchronized local transaction.
  Existing persistence already treats mutable state as the logical commit point, so one bounded
  update provides atomic visibility without a separate staging/recovery state machine.

### D039: Keep synchronization protocol v1 single-run

- **Date:** 2026-09-04
- **Status:** Accepted
- **Decision:** One protocol-v1 `SyncLocalExecution` request contains exactly one existing workflow
  run. It MUST reject new-run payloads and history whose events would create a successor run
  through continue-as-new, retry, or cron. It also MUST reject history that creates or depends on a
  locally created child execution. Those features remain disabled locally until a later protocol
  version defines and tests their same-shard or cross-shard commit semantics.
- **Reason:** The selected logical commit point is one workflow update. Continue-as-new can later
  use the existing current/new-run transaction, while arbitrary children may occupy different
  History shards and need a separate coordination or visibility contract.

### D040: Gate local work with mutable-state-only bridge state

- **Date:** 2026-09-04
- **Status:** Accepted
- **Decision:** Store the local bridge execution state (`RUNNABLE`, `PAUSED`, or
  `OWNERSHIP_LOST`) in local-server mutable state and change it through an internal Admin/History
  operation that takes the workflow lock. Task starts, task completions, and History transfer/timer
  processing consult the state. Pause returns a retryable unavailable error without mutation;
  ownership loss returns not found and is terminal for the local tail. Resuming refreshes tasks.
  The state transitions add no history events and are not included in synchronized raw history.
- **Reason:** The workflow lock defines the safe boundary, while durable state closes races with
  already-issued tokens and timer/transfer tasks without exposing bridge coordination to replay.

### D041: Bootstrap bridge credentials through an authenticated loopback request

- **Date:** 2026-09-04
- **Status:** Accepted for the prototype
- **Decision:** Core creates a cryptographically random one-time bootstrap token in a file readable
  only by the current user and passes only that path to `server start-bridge`. The CLI reads and
  removes the file before accepting a bootstrap request. Core sends the upstream address, static
  headers, API key, TLS roots, and client identity plus effective options and registrations in one
  token-authenticated request to a loopback-only listener. The bridge invalidates the token after
  the configuration has been validated and the local frontend is ready. It keeps upstream
  credential material in process memory and requires Core to supply it again after process restart;
  API keys and TLS private keys are not persisted in bridge metadata or written to normal logs.
- **Reason:** Passing only a protected file path avoids exposing credentials or the bearer token in
  process arguments and environment snapshots. Supplying connection material again on restart
  avoids inventing an at-rest secret store while the existing SQLite/state-directory permissions
  continue to protect durable execution ownership tokens.

### D042: Give each bridge state directory stable identity and exclusive ownership

- **Date:** 2026-09-04
- **Status:** Accepted for the prototype
- **Decision:** Require a private persistent directory for bridge mode. Hold a non-blocking advisory
  lock on it for the bridge process lifetime, store a versioned stable local server ID, reserve its
  local SQLite path, and atomically persist one private ownership/cursor record per acquired
  execution. Hash execution identifiers when deriving filenames. Reject unsupported record
  versions and unsafe file or directory permissions. Upstream connection secrets remain in memory
  and are supplied again through bootstrap after restart.
- **Reason:** Stable identity and durable ownership metadata allow a restarted bridge to distinguish
  resumable local state from a fresh acquisition. Exclusive directory ownership prevents two
  processes from concurrently mutating SQLite or presenting the same bridge identity, while atomic
  record replacement makes each saved cursor a recoverable boundary.

### D043: Activate the bridge during Core Worker validation

- **Date:** 2026-09-04
- **Status:** Accepted for the prototype
- **Decision:** Add experimental `LocalFirstOptions` to Core, the Rust SDK, and the Core C bridge.
  Core evaluates local-execution capabilities and interval bounds during Worker validation and
  silently keeps the existing upstream Worker connection when support is unavailable. When support
  is available and at least one workflow type is registered, Core starts the explicitly configured
  Temporal CLI, authenticates bootstrap with a private one-time token file, sends the effective
  static upstream connection profile and final workflow/Activity registration set, and replaces
  only the Worker's connection with the returned local frontend connection. Application clients
  remain upstream. Unsupported dynamic connection hooks fail bridge startup instead of being
  silently omitted.
- **Reason:** Worker validation is the first point where both upstream capabilities and the final
  registration set are available, and it occurs before pollers are constructed. Keeping fallback
  non-failing preserves compatibility while an explicit CLI path avoids adding download/version
  management to this prototype.

### D044: Hand remote commands back through a prompt release boundary

- **Date:** 2026-09-04
- **Status:** Accepted for the prototype
- **Decision:** The local server compares Workflow Task commands with the bootstrapped task queue
  and Activity registration set inside the Workflow Task completion transaction. An unregistered
  or differently routed Activity, Nexus operation, external Signal, or external cancellation sets
  local bridge state to `PAUSED`; a remote Activity also has eager execution disabled. The bridge
  observes that state through local mutable-state inspection, synchronizes without waiting for the
  ordinary interval deadline, and requests ownership release. After acknowledged release or known
  ownership loss, it invalidates and deletes the obsolete local execution and its durable ownership
  record before allowing that run to be acquired locally again.
- **Reason:** The command transaction is the safe local boundary and already knows whether a command
  requires upstream task machinery. Prompt synchronization avoids needless interval latency, while
  deletion and same-run serialization prevent a newly regenerated upstream task from attaching to
  a stale local tail. The prototype's mutable-state polling can later be replaced by an in-process
  notification without changing the protocol.

### D045: Let callers explicitly wake the controlled synchronization loop

- **Date:** 2026-09-08
- **Status:** Accepted for the prototype
- **Decision:** A bridge caller may request synchronization of a managed execution before its
  interval deadline. The runtime uses a buffered wake-up so repeated requests coalesce, then follows
  the same safe-boundary pause, atomic `SyncLocalExecution`, cursor update, and resume path as an
  interval-triggered synchronization. The interval remains the maximum progress window and lease-
  renewal deadline. The agent demo requests this wake-up after the initial Workflow Task and after
  each completed user turn; its one-minute interval is only a fallback.
- **Reason:** User-input waits are natural durability boundaries for agentic workflows. An explicit
  wake-up makes the demo deterministic and gives applications a useful low-latency durability hint
  without moving synchronization ownership or history transport out of the bridge.

## Resolved questions

| ID | Resolution | Decision |
| --- | --- | --- |
| O001 | Use existing raw `DataBlob` history batches and `VersionHistory` items. The steel thread uses Admin import for creation and History replication for extension; the public sync request should carry these same encodings. Pending-Activity transfer remains part of the later remote-operation slice. | D024 |
| O002 | Use internal AdminService for the first externally reachable prototype transport. Revisit a dedicated/public service after acquisition, ownership, and relay semantics settle. | D028 |
| O003 | Use dynamic prototype bounds of one second through one minute; require the lease to equal three synchronization intervals. | D031 |
| O008 | Use a purpose-built active-History sync operation for both local and global namespaces; reuse policy-neutral event-application primitives rather than NDC transaction policy. | D036 |
| O009 | Use dynamically configurable limits with initial defaults of 8 MiB, 10,240 events, and 256 batches per sync, matching byte/event backlog caps, and a 75 percent early-sync watermark. | D037 |
| O010 | Use one bounded active workflow update as the protocol-v1 logical commit and limit each request to one existing run. | D038, D039 |
| O011 | Persist a local-only execution gate in mutable state and enforce it in task/timer paths. | D040 |
| O006 | Transfer a one-time token through a mode-0600 file and send static upstream credentials through its authenticated loopback bootstrap request; retain those credentials only in memory. | D041 |
| O012 | Use one private, exclusively locked state directory with a stable bridge identity and atomically replaced per-execution ownership/cursor records. | D042 |
| O013 | Activate during Core Worker validation, fall back without error when unsupported, and bootstrap the final registration set before constructing Worker pollers. | D043 |
| O014 | Detect remote commands in the local Workflow Task transaction, synchronize promptly, release upstream, and delete the obsolete local run before reacquisition. | D044 |
| O015 | Explicit synchronization requests wake and coalesce in the controlled bridge loop; they do not bypass safe-boundary or atomic-sync semantics. | D045 |

## Open decisions

Resolve these before implementing the affected slice.

| ID | Question | Needed by |
| --- | --- | --- |
| O004 | Which external Activity operations enter the durable relay inbox in the first implementation? | Phase 4 |
| O005 | What exact physical child-ID encoding and maximum-length hash layout should child-ID encoding version 1 use? | Phase 5 |
| O007 | Which Core-based language SDK is the first end-to-end bridge consumer after Rust? | Phase 3 rollout |
