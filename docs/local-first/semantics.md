# Local-first execution semantics

This document is normative for externally observable prototype behavior.

## Terms

- **Upstream**: the authoritative Temporal cluster used by application clients.
- **Bridge**: the local Temporal server running in bridge mode.
- **Local execution**: an execution with an active upstream ownership lease held by a bridge.
- **Synchronized history**: history acknowledged by `SyncLocalExecution` and persisted upstream.
- **Unsynchronized tail**: locally committed history after the last acknowledged synchronization
  cursor.
- **Safe boundary**: the end of the current atomic local history transaction.
- **Epoch**: a monotonically increasing fencing number for one workflow execution.
- **Lease**: time-bounded permission for one bridge and epoch to submit a local history tail.

## Configuration and fallback

- A Worker without `LocalFirstOptions` MUST behave exactly as it does today.
- `LocalFirstOptions` MUST NOT change application-client routing; application clients continue to
  use upstream.
- Unsupported server/namespace capabilities or interval limits MUST select direct Worker operation
  without an error.
- Persistent local storage is mandatory. Bridge mode MUST reject an in-memory database.
- Static upstream credentials MUST enter the bridge through a one-time authenticated loopback
  bootstrap request. Tokens, API keys, and TLS private keys MUST NOT appear in process arguments,
  environment variables, normal logs, or durable bridge metadata. Core MUST resupply the
  in-memory connection profile when restarting the bridge process.

## Ownership invariant

At most one bridge and fencing epoch may own an execution at a time.

Ownership MUST be persisted in upstream mutable state and MUST NOT emit workflow history events.
Every ownership-sensitive request MUST carry an opaque token and epoch. Upstream MUST reject a
token/epoch mismatch before applying any history or mutable-state changes.

The requested lease duration is `3 * sync_interval`. A successful acquisition or
`SyncLocalExecution`, including an empty one, renews the lease using upstream server time.

## Acquisition visibility

- An upstream local-execution poll MUST leave the Workflow Task scheduled; it MUST NOT add a
  `WorkflowTaskStarted` event.
- The bridge MUST save all baseline history pages, version histories, and ownership data in one
  recoverable local state before starting the task locally.
- Core MUST receive only a normal local Workflow Task after that commit.
- Failure before the commit MUST be recoverable without exposing duplicate work.

## Local progress

The bridge MAY execute the following while its lease is valid:

- Workflow Tasks and durable timers.
- Activities matched by its current registration manifest and task-queue routing.
- Registered child workflow types.
- Activity retry, heartbeat, cancellation, and completion transitions.
- Workflow completion, failure, and cancellation.

The bridge MUST request immediate synchronization for an unsupported or remote operation. It MUST
release upstream ownership when upstream execution is needed to perform that operation.

For protocol v1, unregistered or differently routed Activities, Nexus operations, external
Signals, and external cancellations are remote boundaries. The local Workflow Task transaction
commits their initiated/scheduled history while atomically pausing the execution. Such an Activity
MUST NOT be eager-dispatched by the local server.

Top-level workflow starts require upstream connectivity. Eligible child workflow starts are the
only offline-start exception in the prototype.

## Synchronization deadline and pause

The synchronization interval is a maximum local-progress window, not a best-effort reporting
interval.

- The bridge MUST attempt `SyncLocalExecution` by the interval deadline, even with no new history.
- It MUST attempt earlier when an event/byte limit or remote-operation boundary is reached.
- A caller MAY explicitly request synchronization before the deadline. Repeated requests MAY
  coalesce, but an accepted request MUST wake the same safe-boundary synchronization loop and MUST
  NOT bypass its pause, ownership, atomic-commit, or resume rules.
- Once synchronization is required, the bridge MUST stop dispatching new Workflow Tasks and
  Activity Tasks and MUST stop advancing timers.
- A task already entering its final local transaction MAY commit through the safe boundary while
  the bridge's last known lease remains valid.
- Subsequent task completions MUST return a retryable paused error without mutating history.
- The bridge MAY resume only after upstream accepts `SyncLocalExecution` for the current epoch.
- Local pause/resume/ownership-loss coordination MUST be mutable-state-only. It MUST NOT add events
  to the local history that could be uploaded or replayed upstream.

This bounds offline progress to the first synchronization deadline or configured size limit.

## Lease expiry and stale progress

Upstream MUST expire an unrenewed lease automatically. Expiration and revocation take the same
workflow lock as `SyncLocalExecution`, so they wait for an in-flight handler to return or for its
process/shard ownership to end. After acquiring the lock, committed mutable state identifies either
the complete previous sync point or the complete new one; a pre-CAS failure needs no durable
rollback. Expiration MUST then:

1. Increase the fencing epoch.
2. Clear the previous owner from mutable state.
3. Reapply retained external messages that are not represented by synchronized history.
4. Regenerate upstream tasks.

After expiration, another direct Worker or bridge MAY resume the execution.

When the old bridge reconnects, its first ownership-sensitive response MUST report ownership loss.
The bridge MUST then persist local `OWNERSHIP_LOST` under the workflow lock, causing task starts and
outstanding completions to return not found without mutation, evict the cached workflow run, and
permanently abandon the unsynchronized tail. Old and new tails MUST never merge.

Process crash, machine loss, network partition, and local database loss all converge through this
same timeout behavior. Operator force-revoke MAY trigger it early.

After an acknowledged handback release or known ownership loss, the bridge MUST invalidate and
delete its obsolete local execution before allowing that workflow run to be acquired locally
again. The upstream synchronized history remains authoritative; deletion is local cleanup and is
not part of the synchronization transaction.

## Synchronization guarantees

- Synchronization MUST be idempotent by `sync_id` and source cursor.
- Protocol v1 MUST contain exactly one existing workflow run. It MUST reject new-run payloads and
  events that would create a successor run through continue-as-new, retry, or cron, as well as
  events that create or depend on a locally created child execution.
- One synchronization boundary MUST commit the complete accumulated history delta or none of it.
- Until commit, the readable current history branch and acknowledged cursor MUST remain at the last
  completed synchronization point.
- History MUST validate and apply the bounded delta in memory under the workflow lock, then make one
  ordinary active workflow update containing every history batch and the final mutable-state
  mutation.
- The mutable-state compare-and-set MUST atomically publish the complete delta, advance the
  acknowledged cursor, store the sync fingerprint, and renew or release ownership.
- A non-empty sync MUST fork the committed history branch at the acknowledged cursor before
  appending. History nodes appended before a failed compare-and-set remain on an unreachable orphan
  branch and are cleaned up by normal history trimming. Their physical cleanup does not delay a
  fresh retry or ownership transfer after the failed handler releases the workflow lock.
- The bridge MUST retain data through acknowledgement and MAY resend an acknowledged request.
- Cursor gaps, cursor regression, expired epochs, and conflicting batch hashes MUST be rejected.
- Expiration, revocation, and competing acquisition MUST take the same workflow lock as sync. After
  acquiring it, they observe either the complete previous sync point or the complete committed new
  one; protocol v1 has no durable intermediate sync state to recover.
- Retaining local ownership MUST suppress newly regenerated upstream execution tasks.
- Releasing ownership MUST regenerate every task needed to continue from synchronized history.

## External mutations

While an execution is locally owned:

- Signals, Updates, cancellation, and supported external Activity operations MUST first be durable
  in an upstream inbox.
- They MUST NOT mutate upstream workflow history before local application or takeover.
- The initiating call waits for local durable application, subject to its normal deadline.
- A caller timeout does not delete a durable message; retries use the existing request ID where
  available.
- Upstream MUST retain a delivered message until synchronized history proves it was applied.

Signals and cancellation MAY return after local SQLite commit. Update accepted/completed wait stages
MUST cause immediate synchronization before upstream reports that stage.

## Query semantics

Before Phase 4 Query relay is enabled, a legacy Query delivered to the bridge's upstream
acquisition poll MUST be completed as unsupported on a best-effort basis and MUST NOT terminate
the bridge. If that response cannot reach upstream, the Query caller reaches its own deadline while
local execution continues. This temporary behavior permits observational clients such as Temporal
UI to inspect history without destabilizing the owned execution.

Once Query relay is enabled:

- Queries MUST execute on the current local owner.
- Upstream MUST NOT answer from synchronized history or an upstream Worker while a valid local lease
  exists.
- Query relay state is non-durable and bounded by the caller's context.
- A disconnected owner causes the Query to wait until its deadline, then discard the relay request.

## Child workflow IDs

These semantics apply only after a later child-capable protocol version is specified and enabled;
protocol v1 treats child start as an unsupported local boundary.

- Core MUST transform a logical child ID before creating its child state machine and start command.
- The transform MUST include the stable bridge server ID and an encoding version.
- Oversized physical IDs MUST use a deterministic truncated-and-hashed representation.
- The physical ID is intentionally user-visible in the prototype.
- Core MUST record the bridge ID and encoding version in the first locally produced
  `WorkflowTaskCompletedMetadata`.
- Replay MUST use Workflow Task history lookahead to obtain that metadata before matching child
  commands. It MUST NOT require an additional marker event.
- Once recorded, the run MUST continue using the same transformation after returning upstream.

## Child synchronization

- Protocol v1 MUST NOT synchronize locally created child executions. A child-start command is an
  unsupported local boundary until a later protocol version is enabled.
- A later child-capable version MUST define task fencing, publication order, and failure visibility
  across executions before local child starts are enabled. Child workflow IDs may map to different
  History shards, so the single-run atomicity result cannot be assumed to apply to a parent/child
  group.

## Prototype implementation status

This section is non-normative and records the narrower feasibility harness currently implemented.
It does not weaken the prototype requirements above.

- The focused Go harness still explicitly copies a two-event baseline into a second local test
  server so it remains a small history-replication regression. The standalone bridge harness now
  acquires through ordinary extended Workflow Task polling before importing that baseline.
- A normal Worker connected only to the local server executes three ordinary Activities. These are
  durable Activity Tasks, not SDK local Activities, and produce the usual scheduled, started,
  completed, and intervening Workflow Task history events.
- Local execution reaches 23 events while upstream remains at the original 2 events. A 100 ms
  periodic callback then transfers the raw tail, after which both servers expose the same 23 event
  protobufs, upstream reports the execution completed, and an upstream client reads the same
  workflow result.
- A repeat at the synchronized event/version cursor transfers no additional batches and leaves
  upstream history unchanged.
- A stale cursor sent after upstream advances is rejected with `FAILED_PRECONDITION` and leaves
  upstream history unchanged. Malformed, gapped, regressive, and version-inconsistent envelopes
  are rejected before applying any batch.
- To keep the focused Go harness deterministic, its 100 ms ticker starts after the local loop
  completes. Standalone bridge mode starts its 3 second ticker after acquisition/baseline import
  and renews even when no new history exists.
- The focused test covers local and global namespace types. A local bridge namespace stays local;
  its test server enables cluster replication support only to make the existing baseline import API
  available. The upstream namespace requires no promotion or history migration.
- The production bridge still requires file-backed SQLite even though this isolated feasibility
  test intentionally uses in-memory stores.

The cross-process Core harnesses add the following implementation evidence:

- The upstream server, bridge server, and Core test run in three independent OS processes. The
  bridge's local server is file-backed, and it reaches the upstream server only through its
  frontend address.
- A Worker built directly on `temporalio-sdk-core` connects only to the local frontend, handles
  Workflow Activations itself, and polls/completes ordinary Activity Tasks. It schedules exactly
  three Activity lifecycles and at least four completed Workflow Tasks.
- Before synchronization, upstream contains only the two-event start baseline. The 3 second timer
  starts after baseline import. Core pauses after its first Activity until the bridge has
  synchronized an exact prefix while the workflow remains open, then continues; a later tick makes
  upstream history protobuf-identical to local and exposes Core's result.
- The bridge calls the frontend `SyncLocalExecution` API; it is never given an upstream History
  listener address. It obtains the execution, token, epoch, expiration, and cursor from ordinary
  Workflow Task polling and verifies them against its configured test execution.
- Upstream mutable state contains ownership and the acknowledged sync fingerprint but no ownership
  history event. Ordinary upstream Worker starts are rejected while the lease is active. Each
  non-empty sync applies its full delta to a forked history branch and publishes the branch, cursor,
  fingerprint, and renewed or released ownership in one active workflow update.
- Focused acquisition tests prove that an old expiration task follows a renewed expiration, an
  unrenewed lease regenerates work, the next bridge receives epoch 2, and the old token/epoch is
  rejected. A leftover matching task can be reacquired after expiry without first writing a
  Workflow Task timeout; otherwise the lease timer uses the existing schedule-to-start timeout and
  task-regeneration path.
- Core can instead begin with its normal upstream Worker connection, discover local-execution
  support during validation, start `temporal server start-bridge`, bootstrap it with the final
  registration set, and receive normal Workflow/Activity Tasks from the local frontend. Missing
  capabilities select the direct upstream path without starting the configured CLI executable.
- The CLI accepts only durable state, consumes a private one-time token, and starts the reusable
  bridge runtime after authenticated configuration. A second process cannot share the state
  directory. Retained execution records are loaded and ownership is revalidated before resumption.
- Cross-process tests prove registered Activities remain local, an unregistered Activity is not
  eager-dispatched and becomes available to an upstream Activity poller after release, and an
  external Signal command executes upstream only after release. Focused tests cover the equivalent
  Nexus and external-cancellation boundaries and prompt observation of local `PAUSED` state.

## Explicit prototype limitations

- The bridge is trusted to submit valid history.
- Static upstream credentials and headers are supported; dynamic credential callbacks are not.
- Nexus always requires upstream execution.
- Go and Java SDK Workers are out of scope.
- General offline top-level starts are out of scope.
- Production metrics, Web UI integration, and Temporal Cloud enablement are deferred.
- Per-execution byte/event backlog enforcement and the 75 percent early-sync watermark are not yet
  implemented; current early synchronization covers remote-operation boundaries only.
- Inbound external mutation relay, Query relay, force revoke, grouped child synchronization, and
  successor-run transitions remain unimplemented.
