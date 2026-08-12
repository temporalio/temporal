# Umpire — Entity Identity & Correlation

How the umpire ties an *observed* fact to the *driver-known* entity it belongs to. The driver acts
on entities it names (a namespace, a workflow, an operation); the Monitor observes RPCs and spans
that carry ids of varying provenance. Correlation is the seam between them — and it is not one
mechanism but three, because the ids have three different origins. For the whole-system pitch read
[`UMPIRE_SPEC.md`](./UMPIRE_SPEC.md); for the observer read [`UMPIRE_MONITOR.md`](./UMPIRE_MONITOR.md);
for the driver read [`UMPIRE_ACTIONS.md`](./UMPIRE_ACTIONS.md); the rejection resolution this
generalises is in [`UMPIRE_ERR.md`](./UMPIRE_ERR.md).

> **Status: component reference; implemented (all four phases).** Run identity and lineage come entirely from
> observation: the server emits run-lifecycle telemetry with lineage at every run-creation site
> (first / continue-as-new / reset / retry-cron); the Monitor builds a typed run graph
> (`WorkflowRun` nodes keyed by RunID under `Workflow`, edges labelled continued_as_new / retry /
> cron / reset, with `continued_as_new` terminals); and the action model drives + reconciles
> multi-run scenarios with `LinkedFrom` refs bound *by observation* (no driver-side successor
> RunID). `TestProbeWorkflowContinueAsNewGenerated` proves the whole loop. Namespace stays pre-seed
> throughout. Remaining follow-ups are enumerated under *Open questions* (reset/retry terminals,
> failed/canceled/timed-out run states, child-workflow edges).

## Why

A driver binds a variable to an entity it created — `op`, `wf`, `run` — and later asks the model
"what state is `run` in?" To answer, the Monitor must have routed the observed facts about that run
to the same identity the driver bound. That only works if the driver's key and the observed key
**agree**. Whether they can agree depends entirely on **who mints the id and when**:

| Id | Minted by | Known to the driver | Carried by observation |
|----|-----------|---------------------|------------------------|
| **Namespace** | driver (creates it) | before the drive | name only (frontend) / id (history, span) |
| **First RunID** | server (on start) | only from the start *response* | id (span/RPC), possibly *before* the response |
| **CAN / reset / retry RunIDs** | server (mid-flight) | **never directly** — no client call returns them | id + lineage (server events only) |

Each row needs a different correlation strategy. Conflating them is where the races live.

## Namespace — pre-seed (implemented, correct)

The driver owns the namespace: it created it, so it knows `name→id` before issuing any RPC. So it
**pre-seeds** the Monitor (`SetNamespaceID(name, id)` in `NewCtx`), and observation resolves
synchronously: a frontend request carries only the namespace *name*, and `RecordFact` /
`RecordRejection` resolve it to the scoping id via the seeded map, dropping the fact if the
namespace was never seeded. There is no race — the seed *happens-before* any RPC, and resolution is
per-namespace, so every workflow/run/op in that namespace resolves. This is the mechanism
[`UMPIRE_ERR.md`](./UMPIRE_ERR.md) already uses for synchronous rejections.

Pre-seed works **only** because the driver knows the id a priori. It cannot be stretched to RunIDs.

## Run identity — the lineage graph (the core design)

### Why the driver-side schemes fail

- **Pre-seed** — impossible: the server mints RunIDs.
- **Bind-from-response** — the client's `ExecuteWorkflow` handle gives only the *first* run's id,
  and even that can be *raced* by internal RPCs/spans that carry the RunID before the response
  reaches the client. Worse, **continue-as-new / reset / retry successors have no client response at
  all** — they are created server-side, mid-flight. There is no driver-side moment at which their
  ids become known.

The successor RunIDs *and the relationships between runs* exist in exactly one place: the server's
own run-lifecycle events. So run identity must be **discovered from observation**, and correlation
must ride the **lineage links** the server emits.

### The run graph

Model runs as a graph, reconstructed purely from observed telemetry:

- **Nodes** — `WorkflowRun` keyed by `RunID`, a child of `Workflow(WorkflowID)` (the logical
  handle / graph root). The parent is auto-created when a child fact routes (the registry's
  `getOrCreateRecord` already does this — see how the embedded Nexus op nests under its caller
  workflow).
- **Edges** — `continued_as_new: A→B`, `reset: base→fork`, `retry: A→B`, read from each run's
  lineage attributes (`ContinuedExecutionRunId`, `FirstExecutionRunId`, reset base). Multiple runs
  of one WorkflowID become a chain (CAN/retry) or tree (reset).

Each run's own lifecycle (`created → started → completed/failed/canceled/terminated/timed_out/
continued_as_new`) rides on this node — the run-precise version of today's `Workflow`-by-id model.

### Correlate by relationship, not by a pre-known id

The driver never names a future RunID. It names runs by **relationship**, resolved against the
observed graph:

- **first run** — the chain root (`FirstExecutionRunId`, or the start response's RunID)
- **current run** — the live leaf (the run with no successor yet)
- **the CAN successor of run X** — the run whose `ContinuedExecutionRunId == X`
- **the reset fork of X at event N**

These are deterministic *once the linking fact is observed*, and they never depend on the driver
having learned a RunID in advance.

### Bind-on-observation for new-run effects

A CAN/reset/retry action produces a new run. Its effect declares a run **linked to a predecessor**:

```
Effects:
  { Ref: run@current,  Event: continue_as_new }          // the current run closes
  { Ref: run2, Fresh,  Event: start, LinkedFrom: run }    // a new run, child of the same workflow
```

The framework binds `run2` to the run the observed lineage edge points to, **when it appears**.
This is "reconcile later", but keyed by the server's explicit link — not a heuristic name match — so
it is deterministic. And it is race-free by construction: `Drive` already polls until an effect /
precondition holds, so it simply waits for the linked run to be observed before resolving `run2`.

## Why not a blind stash-and-reconcile

A generic "buffer every unmatched fact and flush when some mapping is learned" is tempting but
weaker than either mechanism above:

- **Namespace** doesn't need it — the driver knows the id (pre-seed).
- **Runs** don't need it — the server hands us the *edges*; correlation is by explicit lineage link,
  not by guessing which stashed fact belongs to which id.

Stash-and-reconcile only earns its place for genuinely *unkeyed* early observation — a fact seen
before *any* correlating structure exists. With lineage telemetry in place, runs never fall in that
bucket. (If we later observe entities the driver didn't start and whose lineage we can't yet read, a
bounded stash keyed by the correlation attribute — flushed when that attribute is learned — is the
fallback. It is a last resort, not the primary design.)

## What's missing today (telemetry)

The design's foundation does not exist yet. Today the server emits `EventWorkflowExecutionCompleted`
(at `service/history/api/respondworkflowtaskcompleted/workflow_task_completed_handler.go`), carrying
`AttrWorkflowID` / `AttrRunID` / `AttrNamespaceID` — so the *completion* of a run is observable with
its RunID (that is what the current single-run `WorkflowRun` grounds on). But there is:

- **no `EventWorkflowExecutionStarted` span event** — so a run's *start* (and thus a run appearing at
  all before it completes) is unobserved; and
- **no lineage attributes** — `ContinuedExecutionRunId`, `FirstExecutionRunId`, reset base are not
  emitted anywhere, so the *edges* of the run graph cannot be reconstructed.

Everything else (multi-fact-per-event decoding via `registerSpanFactAs`, parent auto-creation,
child entities keyed under a parent) is already in place.

## Mapping onto existing code

| Piece | Exists as | Change |
|-------|-----------|--------|
| Namespace resolve | `Monitor.SetNamespaceID` / `resolveNamespaceID` / `RecordRejection` | none — reused |
| Run node | `model.WorkflowRun` (keyed by RunID, child of `Workflow`) | add lineage fields + non-completed transitions |
| Run edges | — | new: capture lineage from a start event; link nodes |
| Multi-fact per event | `FactDecoder.registerSpanFactAs` | reused for the start/lineage fact |
| Parent auto-create | `ModelState.getOrCreateRecord` | reused |
| Driver ref | `umpire.Ref{Type, Var, Fresh}` + `bindFresh` | add relationship refs + `LinkedFrom` |
| Ref resolution | `action.Oracle.find` / `entityID` | resolve run refs by graph relationship, not raw RunID |
| Wait/settle | `umpire.Drive` (polls preconditions/effects) | reused — gives bind-on-observation for free |

## Plan

1. **Run-lifecycle telemetry with lineage** (server-side). Emit `EventWorkflowExecutionStarted`
   carrying `RunID` + `AttrFirstRunID` + `AttrPreviousRunID`, mirroring the completion `AddEvent`.
   This is the prerequisite — without it there is no successor id or edge to correlate to.
   - **done: first-run emit** — `service/history/api/startworkflow/api.go` (`prepareNewWorkflow`),
     consumed by `fact.WorkflowRunStarted` → `model.WorkflowRun` (start + lineage). A first run is
     its own chain root with no predecessor.
   - **done: continue-as-new emit** — `handleCommandContinueAsNewWorkflow` emits the successor's
     start with `AttrPreviousRunID` = the run performing the CaN and `AttrFirstRunID` = the chain
     root. `TestProbeWorkflowContinueAsNew` proves the Monitor models both runs of one WorkflowID,
     linked by lineage — the first graph *edge* (a chain).
   - **done: reset emit** — `workflowResetterImpl.ResetWorkflow` emits the forked run's start with
     `AttrPreviousRunID` = the base run. `TestProbeWorkflowReset` proves the fork edge (a tree).
   - **done: retry / cron emit** — `SetupNewWorkflowForRetryOrCron` emits the successor's start with
     its lineage (covers both retry and cron). Builds green; a dedicated functional test is a
     nice-to-have follow-up (same emit path as cron).

   All four successor topologies (first / chain / tree / retry-cron) carry lineage. **(done)**
2. **Run-graph model.** **(done)** `WorkflowRun` nodes keyed by RunID under `Workflow`, with a typed
   edge (`AttrRunInitiator` → `WorkflowRun.Initiator` / `PreviousRunID` / `FirstRunID`) and a
   `continued_as_new` terminal for the CaN predecessor (`fact.WorkflowRunContinuedAsNew`). The
   decoder emits several facts per event (`registerSpanFactAs`). `TestProbeWorkflowContinueAsNew`
   asserts the typed edge and both node states.
3. **Relationship refs + bind-on-observation.** **(done)** `Ref.LinkedFrom` (generic) + the
   `LineageOracle` (`Successor(type, predecessorID)`); `Drive`/`awaitState` bind a `LinkedFrom` ref
   lazily to its predecessor's observed successor, and `bindFresh` skips it. The Temporal `Oracle`
   resolves it over the run graph.
4. **CAN / reset / retry actions.** **(done for CaN)** `action.RunContinueAsNew` drives a
   continue-as-new chain and reconciles *both* runs, the successor bound by observation.
   `TestProbeWorkflowContinueAsNewGenerated` proves it end-to-end. Reset / retry actions follow the
   same shape (a `LinkedFrom` ref with the matching initiator) — a small follow-up.

## Open questions

- **Lineage source.** A start *span event* (server-side, like completion) is the cleanest carrier —
  no interceptor gap, and it can carry the lineage attrs directly. Confirm the emission site(s):
  first start, CAN successor start, reset fork, retry attempt.
- **Reset semantics.** Reset forks a *tree*, not a chain; the ref language needs "reset of X" plus a
  point (event id). Model the fork edge distinctly from CAN.
- **Retry vs CAN.** Both create a successor run under the same WorkflowID; the edge label (and any
  rule that reasons about "should this have retried?") may need to tell them apart.
- **`started` observation for the by-id `Workflow`.** Independently useful (see
  [`UMPIRE_ACTIONS.md`](./UMPIRE_ACTIONS.md)); the start telemetry above subsumes it if the event
  carries what the frontend-request path would have.
- **Cross-namespace / child workflows.** A child workflow is a run under a *different* WorkflowID;
  its parent link is another lineage edge worth modeling once the single-workflow graph works.
