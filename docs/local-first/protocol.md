# Local-first protocol contract

This is a logical protocol specification. Implemented field numbers are recorded below; names,
invariants, and response semantics are normative and must agree with the current entries in
`decisions.md`.

## Version negotiation

`GetSystemInfoResponse.Capabilities` gains a `local_execution` capability. Namespace description
gains an enablement capability plus minimum and maximum supported synchronization intervals.

Core evaluates these values only when `LocalFirstOptions` is present. A missing capability or an
out-of-bounds interval selects normal direct Worker operation.

Effective per-sync serialized-byte, event, and batch limits plus per-execution unsynchronized
byte/event limits come from namespace-filtered dynamic config. Initial defaults are 8 MiB of
serialized history, 10,240 events, and 256 history batches per sync, with matching 8 MiB and
10,240-event unsynchronized hard caps. The bridge attempts an early sync at 75 percent of either
backlog cap. These are tunable implementation defaults, not protocol constants. The bridge MUST
learn the effective limits before acquisition, either through namespace capability data or the
authenticated bootstrap protocol.

Existing deployment limits still apply independently, including per-event/blob limits,
`system.transactionSizeLimit` for each persisted history node, and total execution-history limits.
The bridge MUST NOT admit a new local transaction whose bounded output cannot fit before the hard
unsynchronized caps. An individual transaction that exceeds a hard cap fails atomically with a
stable resource-exhausted response.

The bridge sends a numeric `protocol_version` on acquisition. Upstream MUST reject unknown versions
without consuming a Workflow Task. The initial version is `1`.

## Workflow Task poll extension

### Request

`PollWorkflowTaskQueueRequest` gains:

```text
optional LocalExecutionPollOptions local_execution_options {
    string local_server_id
    uint32 protocol_version
    Duration sync_interval
    Duration requested_lease_duration // exactly 3 * sync_interval
}
```

The implemented request field number is 11. Protocol version 1 accepts `sync_interval` from one
second through one minute and requires `requested_lease_duration == 3 * sync_interval`.

Only a bridge server sends this field upstream. Existing identity, `worker_instance_key`, task
queue, deployment, and versioning fields identify the downstream Worker represented by the bridge.

### Response

`PollWorkflowTaskQueueResponse` gains:

```text
optional LocalExecutionTaskInfo local_execution_info {
    bytes ownership_token
    int64 fencing_epoch
    Timestamp lease_expiration_time
    int64 last_synchronized_event_id
    int64 last_synchronized_event_version
}
```

The implemented response field number is 20. `GetSystemInfo` capability field 14 advertises server
support. Namespace capability field 18 and namespace limit fields 4 and 5 advertise enablement and
the interval bounds.

For a successful local acquisition:

- `history` contains the baseline through the pending `WorkflowTaskScheduled` event.
- `next_page_token` retains its normal meaning and MUST be fully drained by the bridge.
- `started_event_id` is zero because upstream did not start the Workflow Task.
- The matching task is consumed by the bridge poll and regenerated if the lease later expires.
- `ownership_token` is an opaque secret for subsequent local-execution RPCs.
- The bridge MUST NOT expose the upstream token to Core.

The bridge creates a separate local task token when it starts the imported Workflow Task in its
own server.

### Acquisition transaction

History performs one mutable-state transaction that:

1. Confirms the Workflow Task is still pending and the execution is not already owned.
2. Increments `local_execution_fencing_epoch`.
3. Creates an owned `LocalExecutionInfo` with the token hash and expiration.
4. Records the current version-history and event cursor.
5. Suppresses execution-task creation for that owner/epoch.

No history event is written. If compare-and-set fails, no ownership token is returned.

## Upstream persistence

The internal persistence proto adds fields logically equivalent to:

```text
message LocalExecutionInfo {
    int64 fencing_epoch
    State state                 // OWNED or UNOWNED
    string local_server_id      // empty when unowned
    bytes ownership_token_hash  // empty when unowned
    Timestamp lease_expiration_time
    Duration lease_duration
    int64 last_synchronized_event_id
    string last_sync_id
    bytes last_sync_request_hash
    repeated LocalExecutionMessage pending_messages
}
```

The fencing epoch MUST survive clearing an owner. `pending_messages` MUST be bounded by dynamic
configuration for count and serialized bytes. Query relay state MUST NOT be persisted here.

Protocol v1 has no durable intermediate synchronization state. The workflow lock serializes an
in-flight sync with lease expiration, force revoke, and acquisition. The single final mutable-state
compare-and-set records either the complete old sync point or the complete new one. Physical
history nodes left by a failed pre-CAS append remain unreachable and do not prevent ownership
transfer after the failed handler has released the lock.

History import/rebuild MUST preserve `LocalExecutionInfo`; imported local mutable state cannot
overwrite upstream-only ownership or inbox fields.

## `SyncLocalExecution`

### Request

```text
message SyncLocalExecutionRequest {
    string namespace
    WorkflowExecution execution
    uint32 protocol_version
    string local_server_id
    bytes ownership_token
    int64 fencing_epoch
    string sync_id
    SyncReason reason
    int64 previous_event_id
    int64 previous_event_version
    int64 new_event_id
    int64 new_event_version
    repeated DataBlob raw_history_batches
    VersionHistory version_history
    repeated string applied_message_ids
    bool release
}
```

`SyncReason` initially distinguishes interval, history-event limit, history-byte limit, remote
Activity, Nexus, external workflow operation, continue-as-new, workflow close, clean shutdown, and
manual synchronization. A successor-run or child-related reason describes why the bridge is
releasing/handing back at the preceding safe cursor; it does not permit the v1 delta to contain the
unsupported transition.

Concrete raw-history messages SHOULD reuse existing `DataBlob` and `VersionHistory` encodings
rather than introduce duplicates. Protocol v1 contains exactly one existing run. It rejects
new-run payloads and events that would create a successor run through continue-as-new, retry, or
cron. It also rejects events that create or depend on a locally created child execution. Later
protocol versions must define their commit boundaries before enabling those operations locally.

### Validation order

Before importing any data, upstream MUST validate:

1. Namespace feature enablement and protocol version.
2. Token hash, local server ID, and fencing epoch.
3. The lease has not expired according to upstream time.
4. `previous_event_id` and version-history branch equal the acknowledged cursor.
5. Batch event IDs and versions are contiguous.
6. The delta contains no successor-run transition or locally created child dependency.
7. A repeated `sync_id` has the same execution, cursors, and batch hashes.
8. Per-sync and per-execution size limits.

A validation failure MUST make no history or mutable-state change.

### Application and response

The server applies the complete bounded delta to a working mutable state under the workflow lock.
It clears tasks generated while traversing intermediate events, refreshes only the tasks required
at the final cursor, and suppresses or regenerates executable tasks according to the retain/release
intent. It then makes one ordinary active `UpdateWorkflowExecution` call containing every history
batch plus the final mutable-state mutation, acknowledged cursor, sync fingerprint, applied-message
IDs, and ownership outcome. It returns:

```text
message SyncLocalExecutionResponse {
    string sync_id
    Timestamp lease_expiration_time
    int64 acknowledged_event_id
    int64 acknowledged_event_version
}
```

The mutable-state compare-and-set is the logical publication point. Supported history reads derive
their branch and upper event bound from committed mutable state, so history nodes appended before a
failed CAS are unreachable. A non-empty request first forks the committed branch at the acknowledged
cursor and appends its delta to the new branch. The final compare-and-set switches the committed
branch token together with the cursor and ownership metadata. A failed attempt leaves an orphan
branch for best-effort cleanup, and a retry forks afresh from the still-committed branch. If the CAS
committed but its response was lost, reloading mutable state finds the new cursor and matching sync
fingerprint and returns the committed result.

The complete request MUST fit one bounded History call. Protocol v1 has no staging chunks or
partial acknowledgements. Requested ownership retention or release becomes effective in the same
mutable-state mutation as the new cursor. An empty accepted sync uses the same mutation without
history batches.

An accepted sync, including one with no new history, sets:

```text
lease_expiration_time = upstream_now + lease_duration
```

## Current prototype implementation

The current server slice implements acquisition, ownership, lease renewal/expiry, and the first
externally reachable single-run history-sync transport. Its message shape already matches the
selected protocol-v1 scope:

```text
SyncLocalExecutionRequest {
    namespace
    execution
    protocol_version
    local_server_id
    sync_id
    previous_event_id
    previous_event_version
    new_event_id
    new_event_version
    repeated history_batches
    version_history
    release
    ownership_token
    fencing_epoch
}

SyncLocalExecutionResponse {
    sync_id
    acknowledged_event_id
    acknowledged_event_version
    lease_expiration_time
}
```

The implemented flow is:

1. The bridge adds `LocalExecutionPollOptions` to ordinary upstream Workflow Task polling. History
   stores `LocalExecutionInfo` in mutable state field 117, consumes the matching task without
   adding `WorkflowTaskStarted`, and returns the token, epoch, expiration, history baseline, and
   event/version cursor.
2. `BaselineImporter` drains the raw baseline through `GetWorkflowExecutionRawHistoryV2`, imports
   it locally, and verifies that the imported cursor equals the acquired cursor before advertising
   the local frontend to Core.
3. At the interval deadline, after an explicit synchronization request, or when a Workflow Task
   emits a remote command, the bridge enters its existing synchronization loop. Explicit requests
   use a buffered wake-up and may coalesce; they do not carry history or perform synchronization
   outside the bridge. The bridge changes the local execution to `PAUSED` through
   `UpdateLocalExecutionState`. A remote-command Workflow Task can atomically set the same state
   earlier, and the bridge inspects local mutable state at a short prototype polling cadence so it
   observes that boundary before the ordinary deadline. Local History takes the workflow lock,
   persists the bridge state without adding an event, and gates task starts, task completions, and
   transfer/timer processing. The bridge then reads local history after the cursor, creates a
   unique sync ID, and sends the complete delta and current version history to upstream
   `SyncLocalExecution` through AdminService.
4. The frontend computes a deterministic SHA-256 request fingerprint and delegates the entire
   request once to the owning History shard. Under the workflow lock, History validates the
   persisted owner, token digest, epoch, lease, committed cursor and branch ancestry before decoding
   and validating the bounded delta.
5. A non-empty sync applies every decoded batch to one working mutable state with
   `MutableStateRebuilder`, rejecting successor-run and child-dependent history for protocol v1.
   Once in-memory application succeeds, it forks the committed history branch at the acknowledged
   cursor. Intermediate generated tasks are discarded.
6. One ordinary active workflow update persists every batch on the fork plus one final mutation
   containing the new branch token, cursor, sync ID/hash, lease outcome, and allowed final-state
   tasks. Retained ownership keeps replication and visibility tasks while suppressing executable
   transfer/timer/outbound work; release refreshes ordinary final-state tasks.
7. Empty syncs use the same final mutation without forking history. A terminal or remote-boundary
   history batch sets `release`; the atomic mutation clears ownership. An exact release retry is
   acknowledged idempotently, while reuse of a sync ID with different content fails. After release
   the bridge invalidates and deletes the obsolete local execution, removes its durable record, and
   serializes same-run reacquisition until deletion is observable.
8. The bridge retains the exact prepared request across ambiguous errors. It advances its in-memory
   cursor only after the response echoes its sync ID and exact cursor, then changes local state to
   `RUNNABLE` and refreshes required tasks. Transient failures retry with bounded backoff while the
   execution stays paused. A typed ownership rejection or the last known lease deadline changes the
   local state to terminal `OWNERSHIP_LOST`. The local server remains available so outstanding task
   completions receive the invalidation response.

The focused steel thread covers both local and global upstream namespaces. For a local namespace,
the bridge server enables cluster replication support only so the existing baseline importer is
available while keeping the mirrored namespace local; the upstream namespace is not promoted.
Because version zero is indistinguishable from an omitted raw-history start-version field, the
bridge defensively trims any returned prefix at its explicit acquired cursor. The sync handler
itself is the same purpose-built active-History path for both namespace types.

The current request limits are namespace-filtered dynamic config with defaults of 8 MiB, 10,240
events, and 256 batches. Interval-driven pause/resume, remote-command handback, ownership-loss
invalidation, durable execution records, and restart revalidation are implemented. Per-execution
bridge backlog limits and the 75 percent early-sync watermark from D037 remain future bridge-mode
work. Pending-Activity transfer and inbound external-message handling are not implemented.

The original Core steel thread still uses one test executable with separate `upstream` and `bridge`
modes. The Phase 3 harness starts only the upstream fixture directly: Core discovers its
capabilities, launches the built Temporal CLI with `server start-bridge`, and sends the upstream
profile, effective options, and final Worker registration manifest through the authenticated
bootstrap protocol below. Core then swaps its Worker connection to the returned local frontend;
the application client remains upstream. The reusable bridge runtime loads durable records before
polling, revalidates retained ownership with an empty or pending-tail synchronization, and resumes
only a still-valid epoch.

## Ownership loss

Ownership-sensitive APIs return `FAILED_PRECONDITION` with:

```text
message LocalExecutionOwnershipLost {
    WorkflowExecution execution
    int64 attempted_epoch
    int64 current_epoch
    OwnershipLossReason reason // EXPIRED, FORCE_REVOKED, REPLACED
}
```

This error is terminal for every unsynchronized local change under `attempted_epoch`.

## Lease expiration task

Acquisition and renewal create or replace a timer task keyed by execution, epoch, and expiration.
When it runs, History reloads mutable state and expires ownership only if all three still match.
Stale timer tasks are no-ops.

Expiration performs one execution transaction:

1. Increment the fencing epoch.
2. Clear the owner and token hash.
3. Mark retained mutation messages for upstream reapplication.
4. Refresh Workflow, Activity, timer, child, Nexus, visibility, and matching tasks as appropriate.

The server MUST make takeover possible without an operator action.

## External-message relay

### Durable mutation envelope

```text
message LocalExecutionMessage {
    string message_id
    WorkflowExecution execution
    int64 fencing_epoch
    Timestamp accepted_time
    State state // PENDING, DELIVERED
    oneof body {
        SignalRequest signal
        UpdateRequest update
        CancelRequest cancel
        ExternalActivityOperation activity_operation
    }
}
```

Message IDs use the request's existing idempotency ID when one exists; otherwise frontend creates a
stable random ID before its persistence retry loop.

### Poll

```text
message PollLocalExecutionMessagesRequest {
    string namespace
    string local_server_id
    uint32 protocol_version
    int32 maximum_messages
}

message PollLocalExecutionMessagesResponse {
    repeated LocalExecutionMessage messages
    repeated LocalExecutionRevocation revocations
}
```

History selects only messages whose current mutable-state owner matches `local_server_id`. Delivery
marks mutation messages `DELIVERED` but does not delete them. Redelivery is allowed.

A revocation carries execution, old epoch, new epoch, and reason. Connected bridges SHOULD receive
revocations promptly; correctness continues to rely on epoch validation.

### Response

```text
message RespondLocalExecutionMessageRequest {
    string namespace
    string local_server_id
    bytes ownership_token
    int64 fencing_epoch
    string message_id
    oneof outcome {
        MutationApplied mutation_applied
        UpdateLifecycle update_lifecycle
        QueryResult query_result
        RelayFailure failure
    }
}
```

For mutation messages, acknowledgement means the bridge committed local application to SQLite.
The message remains upstream until a successful `SyncLocalExecution` includes its ID in
`applied_message_ids` and imports the corresponding history.

Update accepted/completed outcomes require immediate synchronization before frontend completes a
waiter for that stage.

## Query relay

Protocol v1 does not yet provide the relay described below. Matching can nevertheless deliver a
legacy Query through the bridge's ordinary upstream Workflow Task poll. The bridge completes that
Query with `QUERY_RESULT_TYPE_FAILED` and a stable unsupported message, forwarding the response's
poller-group ID. This response is best effort: a missing response capability or response RPC
failure leaves the caller to its deadline and does not stop acquisition or local execution. A
non-Query Workflow Task without `LocalExecutionInfo` remains a fatal protocol violation.

Query relay reuses the poll/response transport but not `LocalExecutionInfo.pending_messages`.
Frontend registers an in-memory waiter tied to the caller context and supplies a query relay item to
the owning bridge. The bridge converts it into an ordinary local Query task and returns Core's
result.

Caller cancellation removes the waiter and permits the bridge to discard a late result. Lease
expiration reroutes no existing Query; the caller may retry against the new owner.

## Force revoke

```text
message ForceRevokeLocalExecutionRequest {
    string namespace
    WorkflowExecution execution
    optional int64 expected_epoch
    string reason
}

message ForceRevokeLocalExecutionResponse {
    int64 new_fencing_epoch
}
```

The operator endpoint performs the same transition as lease expiration immediately. Supplying
`expected_epoch` prevents revoking a newer owner accidentally.

## Workflow Task completion metadata

`WorkflowTaskCompletedMetadata` gains:

```text
string local_execution_server_id
uint32 local_child_id_encoding_version
```

Core sets the values on the first local Workflow Task completion for a run. Server history handling
persists them in the existing `WorkflowTaskCompleted` event without interpreting them.

Replay lookahead MUST make the metadata available before Core matches child commands generated by
that Workflow Task.

## Bridge bootstrap protocol

Core creates a cryptographically random one-time bootstrap token in a newly created file whose
permissions admit only the current user. It starts `temporal server start-bridge` with only
loopback listener settings, state paths, and the bootstrap-token file path. The CLI reads and
removes that file before serving the loopback bootstrap endpoint; neither the token nor any
upstream credential appears in process arguments or environment variables. Core then invokes the
loopback-only bootstrap operation with the token in its authorization header and this body:

```text
BridgeConfiguration {
    namespace
    UpstreamConnectionProfile upstream
    LocalFirstOptions effective_options
    WorkerRegistrationManifest registrations
}
```

The initial profile supports address, server name, TLS roots/client identity, API key, and static
headers. The operation validates configuration and starts the local frontend before returning its
address and stable local server ID. The token MUST remain retryable after validation/startup
failure and MUST be invalidated after the first successful bootstrap; every concurrent or later
request is rejected. A request body is size bounded before decoding.

Static upstream credentials remain only in bridge process memory. On bridge process restart Core
creates a new token and resupplies the profile. Durable bridge metadata contains the stable server
ID, execution ownership records, and synchronization cursors, but not the upstream API key or TLS
private key. Bootstrap requests, authorization headers, and credential fields MUST NOT be written
to normal logs.

### Durable bridge process state

Bridge mode requires a persistent state directory accessible only to the current user. One bridge
process holds an advisory lock on that directory for its lifetime; a second process MUST fail
before opening or changing the local database. The directory contains:

- a versioned identity record with a stable, randomly generated local server ID;
- the file-backed local Temporal database; and
- one versioned record per locally owned execution containing namespace and execution identity,
  the raw ownership token, fencing epoch, lease deadline, effective synchronization interval, and
  last acknowledged event/version cursor.

Identity and execution records use private files and same-directory atomic replacement followed by
directory synchronization. Execution identifiers are hashed for filenames. A record is removed
after ownership is released or known lost. Startup MUST load these records before acquiring new
work; resumption must revalidate ownership and may expose work to Core only after the corresponding
local baseline is durable. Static upstream credentials are never part of this state.

## Bridge-local state machine

| State | Entry | Allowed exit |
| --- | --- | --- |
| `IMPORTING` | Upstream acquisition returned | `RUNNABLE` after SQLite commit; restart resumes import |
| `RUNNABLE` | Baseline committed and lease valid | `SYNC_REQUIRED`, `OWNERSHIP_LOST` |
| `SYNC_REQUIRED` | Interval, size, or remote-operation boundary reached | `SYNCING`; no new task dispatch |
| `SYNCING` | `SyncLocalExecution` in flight | `RUNNABLE`, `PAUSED`, `OWNERSHIP_LOST` |
| `PAUSED` | Required sync cannot reach upstream | `SYNCING` on retry, `OWNERSHIP_LOST` at known expiry/error |
| `OWNERSHIP_LOST` | Revocation, expired local deadline, or typed server rejection | Terminal for the local tail |

The local server represents these states in mutable state only. `UpdateLocalExecutionState` takes
the workflow lock, so entering `PAUSED` follows any already-running atomic transaction. Transfer
and timer tasks retry while paused, task completions return a retryable error without mutation, and
resumption refreshes required tasks. On entering `OWNERSHIP_LOST`, task starts and completions return
not found and transfer/timer tasks can no longer advance the abandoned tail.

## Open protocol details

These require code-level investigation before their implementing change. They MUST be resolved in
`decisions.md` rather than implicitly in code:

- The exact existing pending-Activity message reused by `SyncLocalExecution`; raw history uses
  `DataBlob` batches plus `VersionHistory` items per D024.
- The internal mechanism for scheduling revocations to a bridge with no pending message poll.
- The first prototype subset of external Activity operations.
- Eventual aggregate bridge buffering limits beyond the per-execution limits in D037.
- The protocol version and commit/visibility contract for continue-as-new and cross-shard child
  groups; protocol v1 rejects both.
