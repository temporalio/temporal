# Steel-thread review

## Current result

Both steel threads now use the frontend `SyncLocalExecution` API. The original in-process Go
harness remains a focused server regression. The external demo runs upstream, bridge, and raw Core
Worker in three separate OS processes. The bridge acquires its execution through extended ordinary
Workflow Task polling, imports the baseline, and only then exposes file-backed local Temporal.
Core connects only to that local server, executes
three normal Activities across multiple Workflow Tasks, and builds local history while upstream
begins from a two-event upstream baseline. At the configured 3 second boundary, the bridge
synchronizes an exact history prefix while local execution remains active. Empty/non-empty syncs
renew the lease, and a later terminal synchronization releases ownership while making the upstream
result and history identical to local.

The bridge now enters a mutable-state-only local `PAUSED` state at each interval boundary before it
reads the delta. Task starts, Workflow/Activity completions, and transfer/timer processing cannot
advance that execution until the same epoch synchronizes and returns to `RUNNABLE`. Ambiguous
requests retain their sync ID and exact payload. Transient failures retry with bounded backoff; a
typed ownership rejection or the known lease deadline persists terminal `OWNERSHIP_LOST` and makes
outstanding task tokens fail without mutating history. The Core demo injects two synchronization
failures and resumes successfully.

The selected D036/D038/D039 server path is implemented. Focused regressions cover bounded-envelope
validation, exact-repeat idempotence, stale-cursor rejection, local and global namespaces, v1
successor/child rejection, final-task filtering, and a failure after history append but before the
mutable-state CAS. That failure leaves normal history and the cursor at the prior sync point, and a
retry succeeds on a fresh history fork. The standalone three-process Core demo also passes through
the new path. Final focused Go tests, local/global integrations, changed-code lint, protobuf diff
checks, and the Core demo pass.

Phase 3 is implemented for the Rust/Core prototype. Core activates bridge mode during Worker
validation only when `LocalFirstOptions` and both capabilities permit it, otherwise it retains the
upstream Worker connection without error. It launches `temporal server start-bridge`, transfers the
static upstream profile and final registration set through the one-time authenticated loopback
bootstrap, and then receives ordinary Worker tasks from the local frontend. The reusable runtime
persists acquisition before import, recovers and revalidates retained records, and uses the
bootstrapped manifest for local eligibility.

Unregistered or differently routed Activities, Nexus calls, and external workflow commands now
form handback boundaries. The Workflow Task transaction pauses local execution, remote Activities
cannot eager-dispatch, and the bridge observes the pause promptly, synchronizes, releases, and
deletes the obsolete local copy before same-run reacquisition. Cross-process tests cover registered
local execution, capability fallback, state-directory exclusion, unregistered-Activity dispatch
upstream, and external-Signal execution upstream.

An interactive browser demo now drives a three-turn coding-agent workflow. Each turn runs three
ordinary configurable-duration Activities whose summary metadata names a pretend coding operation.
After the local waiting Workflow Task completes, the demo explicitly wakes the bridge's controlled
synchronization loop; the normal pause and atomic `SyncLocalExecution` path still owns the transfer.
The configured one-minute interval is only a safety fallback. Two official Temporal UI instances,
connected independently to the local and upstream frontends, are reverse-proxied under the demo
origin and embedded side by side on the same workflow History page. The next turn remains locked
until the histories converge, and smoke mode verifies final convergence and summary preservation.
Until Phase 4 implements inbound relay, the demo submits its canned Signals directly to the local
frontend. Temporal UI's automatic legacy workflow-metadata Query is explicitly failed until Query
relay exists; receiving that Query, or failing to deliver its response, does not stop the bridge.

## Decisions made

- Reuse `GetWorkflowExecutionRawHistoryV2` plus `ImportWorkflowExecution` to materialize the initial
  local execution.
- Reuse raw `DataBlob` batches, `VersionHistory` items, and the event/version pair as the history
  synchronization representation.
- Expose an experimental single-execution `SyncLocalExecution` through internal AdminService. The
  frontend fingerprints and delegates the whole request once; the owning History shard validates,
  applies, and commits it through one active workflow update.
- Keep protocol and execution machinery in `temporal-server/service/localexecution`; the CLI owns
  only the thin `server start-bridge` entrypoint and server process lifecycle.
- Use the Go SDK only to generate an ordinary server workload for this test. Product SDK work
  remains Core-first.
- Use a purpose-built active-History synchronization path for local and global namespaces. Reuse
  policy-neutral event-application/rebuild primitives, but not NDC's passive/conflict transaction
  managers. Existing local namespaces require no promotion.
- Run upstream and bridge as independent processes. The bridge acquires from the configured task
  queue and verifies the selected execution against the steel-thread fixture.
- Keep the older per-batch `ReplicateEventsV2` behavior only as historical feasibility evidence; it
  is no longer part of the guarded synchronization path.
- Extend ordinary Workflow Task polling with local execution options and ownership response data.
  Preserve the extension across Matching partition forwarding and never return the upstream task
  token to Core.
- Store only a token digest plus epoch, expiration, acknowledged cursor, sync ID, and deterministic
  request hash in upstream mutable state. Renewal/release, branch selection, cursor, fingerprint,
  and final-state tasks are written in the same final mutation.
- Reuse Workflow Task timeout timers for bounded lease takeover. Fence ordinary upstream starts
  while owned and release ownership with terminal history.
- Keep the bridge execution gate in local mutable state and change it through an internal
  Admin/History operation under the workflow lock. Do not add coordination history events.
- Allow explicit synchronization requests to coalesce and wake the existing controlled loop; retain
  the interval as the maximum progress window and keep history transfer inside the bridge.

## Architecture-spike decisions

Both architecture spikes are complete and recorded in
[`spike-namespace-history-layer.md`](spike-namespace-history-layer.md) and
[`spike-atomic-synchronization.md`](spike-atomic-synchronization.md).

1. **Namespace/History layer — Option A selected:** add a purpose-built active-History sync path for
   both local and global namespaces. Promotion remains documented evidence but is not a local-first
   prerequisite. Reuse policy-neutral NDC/workflow event-application primitives without inheriting
   passive replication conflict policy.
2. **Atomic synchronization — Option A selected:** apply one bounded delta in memory and submit all
   history batches plus one final mutation through the ordinary active workflow update. Existing
   persistence appends history nodes before its mutable-state CAS, while supported reads use the
   committed branch and cursor. Failed-attempt nodes therefore remain unreachable, and exact retry
   resolves ambiguous commit outcomes from the cursor and sync fingerprint.
3. **Version-1 scope confirmed:** one existing workflow run per sync. Protocol v1 has no durable
   intermediate sync states or staging chunks and rejects successor-run transitions
   (continue-as-new, retry, or cron) and locally created child dependencies. Their same-shard and
   cross-shard contracts remain Phase 5 work for a later protocol version.

Resolved: synchronization and per-execution backlog limits will use conservative initial defaults
and dynamic config. The atomic-sync spike proposes 8 MiB, 10,240 events, and 256 batches per sync,
matching byte/event backlog caps, and a 75% bridge watermark. These remain tunable implementation
defaults and coexist with the existing per-event, persistence-node, and total-history limits.

## Known temporary omissions

Inbound external-event and Query relay remain Phase 4 work; current external commands are outbound
handback boundaries, and pre-relay Queries receive a best-effort unsupported response. Only the
per-request D037 byte/event/batch limits are implemented, so bridge
backlog caps and the 75 percent early-sync watermark remain future work. Force revoke, child
synchronization, successor runs, production observability, and non-Rust end-to-end SDK exposure are
also deferred.
