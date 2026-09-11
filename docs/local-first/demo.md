# Local-first steel-thread demos

The launchers coordinate sibling `temporal-server`, `temporal-cli`, and `temporal-sdk-rust`
worktrees containing the prototype branches. By default, they use the parent of the current server
worktree; set `LOCAL_FIRST_WORKSPACE_DIR` if those worktrees share a different parent directory.

## Interactive coding-agent demo

From the `temporal-server` worktree, run:

```bash
./docs/local-first/run-agent-demo.sh
```

Open the printed loopback URL. The launcher builds the standalone Go server fixture and runs a
Rust SDK Worker plus a small browser application. It offers three canned coding-agent turns:

1. Investigate a timeout parsing bug.
2. Implement and verify a fix.
3. Perform a final review.

Each turn schedules three ordinary registered Temporal Activities. An Activity only sleeps for a
configurable duration; its persisted Activity summary supplies the pretend coding operation shown
in the agent transcript and on the corresponding history event. The agent does not make an LLM
call, and no pretend project or subprocess tooling is involved.

The right side embeds two official Temporal UI instances side by side. One connects to the local
bridge frontend and the other connects to the upstream frontend, and both open the same workflow
run's History page. The launcher starts the UI servers with distinct public paths and the Rust demo
reverse-proxies those paths through its loopback origin, allowing the pages to be framed under
their same-origin frame policy.

After the Workflow Task that returns the agent to its user-input wait completes locally, the demo
explicitly wakes the bridge synchronization loop. That loop follows the normal pause, atomic
`SyncLocalExecution`, and resume path; the demo does not copy history itself. The next prompt
unlocks only after the two histories are identical. The configured one-minute interval remains a
safety deadline, but is deliberately too long to drive normal turn progress. The workflow
therefore runs bursts of normal Activities with local-Activity-like latency, then reaches a visible
durable boundary between user turns.

Run the non-interactive acceptance path with:

```bash
./docs/local-first/run-agent-demo.sh --smoke
```

Use `--activity-delay-ms <milliseconds>` to change the default one-second Activity duration. Smoke
mode executes all nine Activities, waits for explicitly triggered upstream convergence after each
turn, verifies that every configured summary survived in synchronized history, and first issues the
same metadata Query used by Temporal UI to prove that it cannot stop the bridge. A faster validation
run is:

```bash
./docs/local-first/run-agent-demo.sh --smoke --activity-delay-ms 50
```

The browser currently submits each canned turn as a Signal directly to the bridge's local
frontend. Inbound upstream-to-local Signal relay is Phase 4 work, so this is deliberate demo
scaffolding rather than the proposed product-facing Signal path. History synchronization, local
workflow execution, Activity dispatch, and both embedded Temporal UI history views all use the
real prototype path. Temporal UI sends a legacy `__temporal_workflow_metadata` Query while
rendering a running execution. Until Query relay exists, the bridge returns an unsupported Query
result on a best-effort basis and continues running; this does not affect either UI's History view.

## Core Worker and standalone server

From the `temporal-server` worktree, run:

```bash
./docs/local-first/run-core-demo.sh
```

The script builds `local-first-demo-server` and the Temporal CLI, then runs all five manual
`local_first_bridge_tests` cases by default. Pass an explicit test-name filter to run one case. A
successful run includes:

```text
bridge synchronized an intermediate 8-event prefix while local execution remained active
Core built 23 local events across 3 full Activities
upstream returned the Core-produced workflow result after synchronization
local and upstream histories are protobuf-identical
test local_first_core_bridge ... ok
Core-started bridge completed and synchronized the workflow
test local_first_core_activation ... ok
```

The exact event and batch totals are diagnostic rather than contractual. The assertions require
three scheduled/started/completed Activity Task lifecycles, at least four completed Workflow Tasks,
no upstream progress before the interval, the same result, and protobuf-identical final histories.

The original steel-thread case launches the standalone binary twice: once in `upstream` mode and
once in `bridge` mode. These are independent processes. The bridge owns the file-backed local
server and calls
`SyncLocalExecution` through the upstream frontend; it is never given the upstream History
listener. Before becoming ready, it acquires the configured execution through ordinary Workflow
Task polling, imports the acquired two-event baseline, and verifies the event/version cursor. Core
is a third process and connects only to the local frontend.

The Phase 3 cases instead let Core launch `temporal server start-bridge` after capability discovery.
They cover direct fallback, authenticated bootstrap, final registration delivery, exclusive state
directory ownership, registered local Activities, unregistered-Activity handback, and outbound
external-Signal handback.

## Focused server-only regression

From the `temporal-server` worktree, run:

```bash
GOCACHE=/tmp/local-first-go-cache go test -tags test_dep ./temporaltest \
  -run '^TestLocalFirstSteelThread$' -count=1 -v
```

The significant log milestones are:

```text
imported upstream baseline through event 2
ExecuteActivity ... ActivityType local-first-demo-activity
ExecuteActivity ... ActivityType local-first-demo-activity
ExecuteActivity ... ActivityType local-first-demo-activity
local history reached 23 events across three full Activities while upstream remained at 2
periodic sync copied 11 history batches through event 23; upstream now has 23 events
```

Workflow and run IDs vary. The test also verifies that the synchronized event protobufs are
identical, upstream status and result are correct, and a second sync at event 23 transfers zero
batches.

These are full Activities: local history contains three `ActivityTaskScheduled`,
`ActivityTaskStarted`, and `ActivityTaskCompleted` lifecycles with Workflow Tasks between loop
iterations. No SDK local-Activity API is used.

The bridge's demonstration interval is 3 seconds and starts after baseline import. Every interval,
including one with no new events, authenticates and renews the upstream lease. The Core driver
pauses after its first Activity until an exact upstream history prefix is visible, proving that the
ticker operates while the execution remains active, then continues locally to completion.

The demos prove the history round trip, Core Worker path, independent upstream/bridge process
boundary, durable local history storage, poll acquisition, token/epoch fencing, lease renewal,
terminal release, frontend sync transport, cursor validation, and periodic batching seam. Focused
server tests additionally cover wrong-token rejection, old-expiration rescheduling, automatic
takeover, stale-owner rejection, upstream Worker fencing, task regeneration, safe-boundary pause,
and durable restart revalidation. Inbound external-event/Query relay, size-driven backlog limits,
and force revoke are not yet demonstrated.
