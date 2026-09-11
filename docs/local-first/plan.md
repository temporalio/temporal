# Local-First Temporal Execution Prototype

## Summary

When `LocalFirstOptions` is configured and supported by the upstream namespace, Temporal Core
starts a local Temporal server in bridge mode and connects the Worker to it. The application client
remains connected to the upstream server.

```text
Application client ------------------------------------> Upstream Temporal
                                                             ^
                                                             | PollWorkflowTaskQueue
                                                             | SyncLocalExecution
                                                             | external-message polling
                                                             |
Language SDK -> Temporal Core -> Local bridge server --------+
                                      |
                                      +-- durable SQLite
```

The bridge server polls upstream, saves the history baseline locally, runs workflows and eligible
Activities locally, and periodically calls `SyncLocalExecution`. Core continues to process normal
Workflow and Activity Tasks.

Local execution uses a renewable upstream lease. During a partition, execution proceeds only until
synchronization is required. It then pauses. If the lease expires, upstream resumes the workflow
with a higher fencing epoch, and the old local execution is invalidated when connectivity returns.

The prototype trusts history submitted by an official local bridge server. It targets development
and trusted self-hosted evaluation, not Temporal Cloud or untrusted multi-tenant environments.

## Execution Model

### Activation and fallback

Worker startup follows these rules:

1. If `LocalFirstOptions` is absent, use the existing direct-to-upstream Worker path without
   starting a local server.
2. If `LocalFirstOptions` is present, Core uses existing system and namespace discovery to check:
   - The server implements local execution.
   - The namespace enables local execution.
   - The requested synchronization interval is within the namespace's advertised bounds.
3. If those conditions hold, Core starts `temporal server start-bridge` and connects the Worker to
   it.
4. If they do not hold, Core uses the normal direct Worker path and emits an informational log.

Capability absence is not an error.

### Reusing Workflow Task polling

Extend `PollWorkflowTaskQueueRequest` with optional `local_execution_options`:

```text
LocalExecutionPollOptions {
    local_server_id
    protocol_version
    sync_interval
    requested_lease_duration
}
```

The bridge server sends this extension when polling upstream. It copies the downstream Worker's
task queue, identity, Worker instance key, and deployment/versioning information into the upstream
poll.

The requested lease duration is exactly three times the configured synchronization interval. The
namespace advertises minimum and maximum supported synchronization intervals derived from server
lease limits.

When History receives an eligible poll:

1. Matching selects work using the normal task-queue and Worker-versioning path.
2. History compare-and-sets ownership in persisted mutable state.
3. It assigns a new fencing epoch and expiration time.
4. The upstream Workflow Task remains scheduled rather than being marked started.
5. The normal poll response returns the workflow history and optional `local_execution_info`:

```text
LocalExecutionTaskInfo {
    ownership_token
    fencing_epoch
    lease_expiration_time
    version_histories
    last_synchronized_event_id
}
```

`LocalExecutionInfo` is stored exclusively in the workflow's persisted mutable state, such as
`WorkflowExecutionInfo`. Acquiring, renewing, or releasing local ownership does not add workflow
history events.

### Baseline persistence before local handoff

The bridge server consumes the extended upstream poll response:

1. Fetch all remaining history pages.
2. Bootstrap the local namespace with the upstream namespace ID, failover version,
   search-attribute mappings, and required configuration.
3. Import the baseline and version histories into SQLite.
4. Persist the ownership token, epoch, expiration, and synchronization cursor.
5. Rebuild the local tasks represented by the pending `WorkflowTaskScheduled` event.
6. Commit the SQLite transaction.
7. Satisfy Core's held local `PollWorkflowTaskQueue` with a normal local task token and locally
   started Workflow Task.

A crash before the SQLite commit exposes no work to Core. A restart after the commit reconstructs
the local task without reacquiring the execution.

### Eligible local operations

The bridge server may continue locally for:

- Workflow Tasks.
- Durable timers.
- Activities registered on a local Worker for the selected task queue.
- Registered child workflow types.
- Activity retries, heartbeats, cancellation, and completion.
- Workflow completion, failure, and cancellation.

Language SDKs supply registered workflow and Activity types through Core's Worker configuration.
Core provides that manifest during bridge bootstrap; regular task polling remains unchanged.

The bridge requests immediate synchronization when it encounters:

- An unregistered or differently routed Activity.
- A Nexus operation.
- An external-workflow command targeting a workflow it does not own.
- Continue-as-new.
- Clean shutdown while connected.
- An unsupported command.
- The configured unsynchronized-history event or byte limit.

Unsupported command events are committed locally, synchronized upstream, and followed by ownership
release. Upstream task regeneration then performs the remote operation.

## Synchronization and Lease Management

### Periodic `SyncLocalExecution`

The configured `sync_interval` is a safe-boundary deadline:

- The bridge calls `SyncLocalExecution` at least once per interval, including when no history
  changed.
- An empty synchronization renews the ownership lease.
- If local history changed, synchronization begins after the current local history transaction
  commits.
- Dynamically configured event, batch, and byte limits may require synchronization sooner.
- New tasks stop being dispatched while a required synchronization is pending.

`SyncLocalExecutionRequest` contains:

- Exactly one existing workflow run in protocol version 1.
- Ownership token and fencing epoch.
- Idempotent `sync_id`.
- Previous and new local cursors.
- Raw history batches since the acknowledged cursor.
- Updated version history for that run.
- Retain-or-release ownership intent.
- Synchronization reason.

The upstream handler implements one logical all-or-nothing synchronization boundary:

1. Validates the token, fencing epoch, previous cursor, unexpired lease, request fingerprint, and
   configured size limits.
2. Applies every history batch to a working mutable state in memory under the workflow lock, while
   preserving upstream-only ownership and inbox fields.
3. Discards tasks generated for intermediate states and refreshes only the final-state tasks.
4. Calls the ordinary active `UpdateWorkflowExecution` path once with every history batch and one
   final mutation. Its mutable-state compare-and-set publishes the complete delta, advances the
   acknowledged cursor, records the sync fingerprint, and renews or releases ownership.
5. Suppresses or regenerates timer, transfer, visibility, and matching tasks according to the
   retain-or-release intent.
6. On failure before the compare-and-set, retains the previous authoritative history/cursor.
   History nodes already appended by the ordinary persistence path remain unreachable and are
   trimmed later.
7. Returns the acknowledged cursor only after the whole boundary commits.

The bridge retains unsynchronized history until acknowledgement. The complete bounded request fits
one History call and is idempotent by sync ID, source cursor, and content fingerprint. Protocol v1
has no staging chunks, partial acknowledgements, or durable intermediate synchronization state.

### Behavior when synchronization fails

When a required synchronization cannot reach upstream:

- The bridge enters a paused state.
- It stops issuing new Workflow Tasks and Activity Tasks and stops advancing timers.
- A task already completing its current local transaction may reach that safe boundary if the
  bridge's last known lease expiration has not passed.
- Later task completions receive a retryable paused error and do not mutate local history.
- The bridge retries `SyncLocalExecution` with bounded backoff.

If synchronization succeeds before lease expiration, the lease is renewed and execution resumes.

### Lease expiration and takeover

Each acquisition creates an upstream timer task for `lease_expiration_time`. Every successful
`SyncLocalExecution`, including an empty one, moves that expiration forward.

When the timer fires and the stored lease has not been renewed, History takes the workflow lock,
which serializes expiration with an in-flight sync. The committed mutable state then represents
either the complete old sync point or the complete new one. History:

1. History increments the fencing epoch.
2. It clears local ownership from mutable state.
3. It reapplies external inputs retained in the upstream inbox.
4. It regenerates the execution's upstream tasks.
5. A direct Worker or another bridge server may acquire and resume the workflow.

A connected bridge receives the revocation through its external-message poll. A disconnected bridge
discovers it through the first `SyncLocalExecution` or message poll after reconnection. The server
returns a typed ownership-lost response containing the newer fencing epoch.

Upon ownership loss, the bridge:

- Stops polling locally for the affected execution.
- Invalidates local task tokens.
- Evicts Core's cached run through normal task-failure and eviction paths.
- Rejects outstanding local completions.
- Marks its unsynchronized history as abandoned and never retries it upstream.
- Retains diagnostic data until normal local cleanup.

This applies uniformly to network partitions, process crashes, machine loss, and local database
loss. `ForceRevokeLocalExecution` remains available for operator-directed takeover before the
normal timeout.

## Child Workflows

Eligible child workflows may begin locally while disconnected only after a post-v1 child-capable
protocol defines their cross-execution commit semantics. Protocol v1 treats child start as an
unsupported local boundary.

### Conflict-free child IDs

Core derives the physical child workflow ID from:

```text
logical child ID + stable local server suffix
```

The transform happens before Core creates either the child state machine or the
`StartChildWorkflowExecution` command. The physical encoding:

- Is deterministic.
- Uses the bridge server ID persisted beside SQLite.
- Includes an encoding version.
- Escapes or hashes oversized IDs to remain within server limits.

The physical suffixed ID is visible through Temporal APIs, history, failures, and the Web UI.
Workflow code continues using its logical ID.

### Replay through Workflow Task metadata

Extend `WorkflowTaskCompletedMetadata` with:

```text
local_execution_server_id
local_child_id_encoding_version
```

Core writes these fields on the first locally completed Workflow Task. They are persisted in the
existing `WorkflowTaskCompleted` event.

During replay, Core's existing history lookahead inspects the completion metadata for the Workflow
Task being replayed. This makes the local server ID available before child-command matching begins,
including when the first local Workflow Task starts a child.

Once recorded, the run continues applying the same child-ID transformation even if later Workflow
Tasks execute upstream. No additional marker event is needed.

### Synchronizing child executions

Protocol version 1 does not carry locally created children or new runs. Until a later protocol
version defines their commit boundaries, a local child-start command or any close that would create
a successor through continue-as-new, retry, or cron is an unsupported boundary and causes handback
before that operation executes locally.

The later child-capable design must import children before parent events that depend on them and
give active children independent ownership records. Because child workflow IDs may map to different
History shards, Phase 5 must explicitly choose a cross-shard publication/coordination contract; it
cannot inherit protocol v1's single-workflow transaction implicitly.

## External Events and Queries

### Durable messages

Add `PollLocalExecutionMessages` and `RespondLocalExecutionMessage`, used by the bridge server.

When a locally owned execution receives a Signal, Update, cancellation, or external Activity
operation:

1. Upstream persists it in a bounded inbox within `LocalExecutionInfo`.
2. The request does not immediately mutate workflow history.
3. The bridge receives it through its long-poll.
4. The bridge applies it to local SQLite.
5. The bridge responds after the local transaction commits.

The initiating upstream RPC waits for local acknowledgement:

- If its deadline expires, the durable inbox entry remains.
- Request IDs make retries idempotent.
- Delivered messages remain upstream until `SyncLocalExecution` proves their resulting history was
  persisted.
- Lease expiration can therefore reapply them upstream.

Signals and cancellation may return after durable local application. Updates preserve their
requested wait stage; reaching accepted or completed triggers immediate synchronization so upstream
can persist that stage before responding.

The message poll also carries revocation notices for executions whose lease expired or was
force-revoked.

### Queries

Queries always execute on the current local owner:

1. Upstream creates a non-durable Query waiter.
2. The bridge receives it through `PollLocalExecutionMessages`.
3. The bridge schedules a normal local Query task.
4. Core processes the Query and responds to the bridge.
5. The bridge relays the result through `RespondLocalExecutionMessage`.

If the bridge is disconnected, the Query waits until the caller deadline and is then discarded.

## Changes by Repository and Subsystem

### Temporal API

Modify existing types:

- Add `local_execution_options` to `PollWorkflowTaskQueueRequest`.
- Add `local_execution_info` to `PollWorkflowTaskQueueResponse`.
- Add the local server ID and child-ID encoding version to `WorkflowTaskCompletedMetadata`.
- Add system and namespace local-execution capabilities.
- Add namespace minimum and maximum synchronization intervals.
- Add a structured ownership-lost failure detail.

Add experimental APIs:

- `SyncLocalExecution`.
- `PollLocalExecutionMessages`.
- `RespondLocalExecutionMessage`.
- Operator-only `ForceRevokeLocalExecution`.

All changes begin in source protos and flow through normal generation.

### Temporal server: upstream behavior

Implement:

- `LocalExecutionInfo` in persistence mutable state, with owner identity, token hash, fencing epoch,
  lease expiration, cursors, state, and external-message inbox.
- Local acquisition in the existing Workflow Task poll path.
- Lease-expiration timer tasks.
- Stale-epoch rejection and takeover.
- Task-generation and task-execution guards while an execution is locally owned.
- `SyncLocalExecution` using existing history import, Activity synchronization, and task-refresh
  mechanisms.
- External-event interception and message routing.
- Query waiters and routing.
- Force revoke.
- Namespace/dynamic-config gates and conservative request-size limits.

Prototype observability consists of focused logs around acquisition, synchronization, pause,
renewal, lease expiration, ownership loss, and takeover. Metrics and UI integration remain future
work.

### Temporal server: bridge mode

Add a reusable bridge-mode component to the server repository. It owns:

- Upstream Workflow Task polling.
- Namespace and history bootstrap.
- SQLite persistence before local task exposure.
- Synchronization scheduling and `SyncLocalExecution`.
- Lease renewal and paused-state behavior.
- External-message polling.
- Local eligibility decisions.
- Child-graph synchronization.
- Ownership invalidation and recovery.

Expose bridge mode through normal server construction options so both CLI startup and future
embedded-library work can reuse it.

### Temporal CLI

Add a thin `temporal server start-bridge` command that:

- Validates that persistent SQLite storage is configured.
- Accepts loopback ports, state directory, and bootstrap-token input.
- Starts the server with bridge mode enabled.
- Omits the Web UI and unrelated development-server setup.
- Handles process lifecycle and human-readable startup errors.

The execution, synchronization, persistence, and upstream communication logic lives in the server
repository.

### Temporal Core and Core-based SDKs

Add:

```text
LocalFirstOptions {
    sync_interval
    state_directory
    max_unsynchronized_events
    max_unsynchronized_bytes
    cli_executable_or_download_options
}
```

Core's responsibilities are:

- Activating the feature only when `LocalFirstOptions` is specified.
- Checking the upstream capability and interval limits.
- Selecting the normal Worker path when support is unavailable.
- Starting and stopping `server start-bridge`.
- Supplying the upstream connection profile and Worker registration manifest through an
  authenticated loopback bootstrap endpoint.
- Pointing Worker traffic at the bridge.
- Applying the child-ID transform and Workflow Task metadata behavior.
- Preventing concurrent reuse of one state directory.

Expose these options through Rust and the Core bridges used by TypeScript, Python, .NET, and Ruby.
Go and Java are deferred.

The prototype supports static upstream addresses, TLS material, API keys, and static headers.
Dynamic credential callbacks are deferred.

## Delivery Sequence

1. **Acquisition and round-trip proof**
   - Extend ordinary Workflow Task polling.
   - Store ownership in mutable state.
   - Persist one baseline locally before dispatch.
   - Run multiple Workflow Tasks and Activities locally.
   - Synchronize through `SyncLocalExecution`.

2. **Lease and recovery**
   - Add renewal, expiration timer tasks, fencing epochs, takeover, pause behavior, and
     ownership-loss invalidation.
   - Add idempotent bounded single-request and size-triggered synchronization.

3. **Bridge command and SDK activation**
   - Add reusable server bridge mode and the thin CLI command.
   - Add `LocalFirstOptions`, capability fallback, authenticated bootstrap, and registration
     manifests.

4. **External interactions**
   - Add the durable inbox and Query relay.
   - Cover deadlines, retries, reconnection, expiration, and revocation notices.

5. **Children and run transitions**
   - Define a post-v1 commit/visibility protocol for same-shard successor runs and cross-shard
     children.
   - Add child-ID suffixing and replay lookahead.
   - Synchronize child graphs.
   - Handle continue-as-new through synchronization, release, and reacquisition.

6. **Security hardening decision**
   - Evaluate validated event upload versus a server-verifiable command/result journal before
     considering untrusted deployment.

## Test and Acceptance Plan

- Without `LocalFirstOptions`, no bridge process starts and Worker behavior is unchanged.
- Missing server or namespace capability selects the normal Worker path without error.
- Local ownership exists only in persistence mutable state and creates no history event.
- A local-enabled `PollWorkflowTaskQueue` leaves the upstream Workflow Task scheduled.
- Competing bridge servers cannot acquire the same execution epoch.
- A crash before the baseline SQLite commit exposes no local Workflow Task.
- A crash after the commit resumes without reacquisition.
- Multiple Workflow Tasks, timers, and registered Activities run between `SyncLocalExecution` calls.
- All acquisition, synchronization, and message-relay requests originate from bridge-mode server
  code.
- Successful empty synchronization renews an idle execution's lease.
- A failed required synchronization pauses new task dispatch and timer advancement.
- Event and byte limits also pause execution until synchronization succeeds.
- Lease expiration regenerates upstream tasks and permits another Worker to resume.
- Reconnection after takeover returns ownership lost and permanently abandons the stale local
  history.
- Outstanding task completions cannot mutate history after local invalidation.
- Duplicate and interrupted sync requests either commit the complete delta exactly once or leave
  the previous synchronization point authoritative; partial deltas never become readable.
- Unsupported Activities, Nexus calls, and remote workflow operations synchronize and release.
- Signals, Updates, and cancellation apply exactly once across retries and lease expiration.
- Queries execute only on the current local owner.
- The first local Workflow Task records child-ID metadata without a marker event.
- Replay lookahead recovers the suffix before matching child commands.
- Child IDs remain deterministic across replay, restart, takeover, and different bridge servers.
- Protocol v1 rejects child/new-run synchronization; later parent/child publication semantics are
  specified before enabling local child starts.
- Continue-as-new does not create duplicate runs.
- Feature-disabled histories remain byte-for-byte compatible with current behavior.

## Shared-Library Approach

The shared-library design retains the upstream protocol: extended Workflow Task polling,
`SyncLocalExecution`, renewable ownership, external-message relay, and takeover semantics.

It replaces the bridge process with an embedded local-server host linked into Core. That host would
service ordinary Worker APIs and own upstream polling, persistence, synchronization, and lease
handling.

A shared Rust/WASM execution kernel would need to implement:

- Mutable-state transitions and history construction.
- Workflow Task command validation.
- Update and external-message state.
- Timers, Activity retry/heartbeat state, child workflows, and run closing.
- Deterministic task-intent generation.
- Stable serialized state, migrations, and a crash-safe local store.

The Go server and embedded Core host would provide persistence, queues, clocks, namespace
configuration, visibility, Nexus, and network adapters. This is a semantic port rather than a small
extraction: the relevant Go workflow/history-builder/Workflow-Task surface is roughly 31,000
non-test lines before matching and queue behavior.

After proving the protocol with bridge mode, introduce the kernel through shadow execution in the
Go server and compare generated events, mutable state, and task intents before making it
authoritative.

## Alternative Worth Testing

A narrower "macro Workflow Task" could allow Core to run locally available Activities and submit
an ordered command/result journal in one bulk operation. This would reduce latency with less local
server machinery and a stronger validation boundary, but would provide less complete offline
execution.

Existing Local Activities and eager Activity/Workflow-Task dispatch should remain performance
baselines. Global-namespace replication is not recommended because it operates at cluster/namespace
granularity and requires a heavier connectivity and operational model.

## Deferred Future Work

- Strict event validation or a command/result journal.
- Production metrics, dashboards, alerts, and Describe/Web integration.
- Dynamic credential callbacks and rotation.
- Go and Java SDK support.
- General offline workflow starts.
- Local Nexus execution.
- Temporal Cloud and multi-tenant security review.
- More sophisticated takeover policies based on Worker health.

## Assumptions

- The official bridge server is trusted for the prototype.
- The requested ownership lease is three times `sync_interval`.
- Local progress pauses whenever synchronization is required but unavailable.
- Lease expiration automatically permits upstream takeover.
- Unsynchronized history from a stale owner is never merged.
- Workflows initially start upstream; eligible children are the offline-start exception.
- Physical suffixed child IDs are user-visible.
- Core-based SDKs are the initial target.
- Queries are never evaluated from synchronized upstream history.
