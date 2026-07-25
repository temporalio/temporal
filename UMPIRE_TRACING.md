# Umpire — Tracing & Trace-Derived Fault Injection

How Umpire can learn, cache, and fault-test the *underlying operations* a model transition
actually performs — **without hard-coding either the faults or the gRPC/persistence calls they
target**. The calls are **observed** from a happy-path run over OTEL + interceptors, **checked
into source control** as golden footprints, and then **replayed under fault** with the existing
rulebook as the oracle. For the whole-system pitch read [`UMPIRE_SPEC.md`](./UMPIRE_SPEC.md); for
the model this rides on read [`UMPIRE_MONITOR.md`](./UMPIRE_MONITOR.md); for the drive side read
[`UMPIRE_DRIVER.md`](./UMPIRE_DRIVER.md) / [`UMPIRE_PLANNER.md`](./UMPIRE_PLANNER.md).

## The one idea: a transition's *operational footprint*

The Monitor already knows a transition's **semantic** contract — `WorkflowUpdate: admitted →
accepted` is a legal edge with predicted next-state/timestamps ([`UMPIRE_MONITOR.md`](./UMPIRE_MONITOR.md),
the SAA "complete edge contract" in [`UMPIRE_PRIOR_ART.md`](./UMPIRE_PRIOR_ART.md#what-umpire-can-learn-from-the-saa-behavioral-model)).
It does **not** know that edge's **operational** contract: *which* frontend RPCs, internal-service
calls, and persistence writes the server performs to realize it. That set — observed, normalized,
and pinned — is the transition's **footprint**.

```
semantic edge contract  (have)   :  admitted → accepted, sets AcceptedAt, no reject
operational footprint   (this)   :  RespondWorkflowTaskCompleted → history.RecordWorkflowTaskCompleted
                                     → persistence.UpdateWorkflowExecution → matching.AddWorkflowTask
```

Once a footprint is known and trusted, fault injection is **derived, not authored**: for each
operation in the footprint, drive the SUT to the pre-transition state, arm a fault on *that*
operation, drive the transition, and let the Monitor judge. No hand-written fault list, no
hand-written "this call must happen" assertion — both fall out of the footprint.

## Why this shape (mapping to the user's constraints)

| Want | Mechanism |
|---|---|
| Don't hard-code faults | derive them per-operation from the footprint (cross-product below) |
| Don't hard-code the underlying gRPC/persistence calls | never written by hand — observed from a happy-path trace |
| But *know* those calls | the footprint **is** that knowledge, made explicit |
| Store & check in; ensure true & cached | golden footprint files + a **conformance/drift** test that re-derives and diffs |
| Use them to fault-inject & flag issues | footprint → fault plan → inject → judge with the existing rulebook |
| Rely on gRPC interception + OTEL, mostly OTEL | capture is span-first; the imported gRPC `RPCFaultGenerator` + the persistence-interceptor seam realize faults |
| Full tracing to associate traces with each step | needed for the *faithful* version; a useful version works from **entity-tagged spans** without full cross-service propagation (see Feasibility) |

## The pipeline

```
 happy path ──▶ CAPTURE ──▶ NORMALIZE ──▶ STORE(golden) ──▶ CONFORM(drift) ──┐
   (Planner)     (OTEL +      (fingerprint    (checked-in)     (re-derive &   │
                  intercept)    volatile out)                    diff)         │
                                                                               ▼
                       JUDGE ◀── INJECT ◀── DERIVE faults ◀────────── footprint catalog
                    (rulebook)  (fault seam)  (op × fault-kind)
```

1. **Capture.** Drive a happy path (Planner → Driver). Every transition already emits an
   entity-identified `chasm.transition` span event (`common/telemetry`, wired for the Monitor).
   Collect the **operation spans** — gRPC server/client spans, internal-service interceptor
   spans, persistence spans — that belong to the same transition (see *Association*, below).
2. **Normalize.** Reduce each operation to a stable identity (`service/method` or
   `persistence.Op`, plus a small set of semantic attrs) and drop volatile fields (timestamps,
   request IDs, host, attempt counters bucketed) — the SAA `Fingerprint`/`mask` discipline
   ([`UMPIRE_PRIOR_ART.md`](./UMPIRE_PRIOR_ART.md#what-umpire-can-learn-from-the-saa-behavioral-model)). A footprint must be **deterministic** to be checked in.
3. **Store.** Serialize per entity to a reviewed golden file
   (`tests/umpire/footprints/<entity>.json`), one entry per edge. Human-diffable.
4. **Conform (drift).** A test re-derives footprints from a fresh happy path and diffs against
   the golden. A diff is either an intended behavior change (update the golden in the same PR,
   reviewed) or an accidental one (a new persistence write crept into the update path) — caught
   as a reviewable change, exactly like generated code.
5. **Derive faults.** For each operation in a footprint, enumerate the fault kinds the
   environment supports: `drop→error`, `delay`, `corrupt/modify-response`, and (later) timing.
   The fault plan is the cross-product `edge × operation × fault-kind`.
6. **Inject.** Drive to the pre-transition state, arm one fault scoped to that operation
   (imported `RPCFaultGenerator` for gRPC; the persistence-interceptor seam for stores), drive
   the transition.
7. **Judge.** The **same** safety/liveness rulebook. No new per-fault assertions — the faults
   manufacture rare states and the existing rules judge them (the SPEC's "close the loop").

## Association — the crux, and why "mostly OTEL"

A model transition is provoked by server work that spans **multiple asynchronous units**: a
client RPC writes history and schedules tasks; those tasks are persisted and processed *later*,
in a different goroutine/shard/trace. In-process OTEL context does **not** survive a task
boundary, so a single naïve trace does not cover a transition end-to-end. Two ways to bridge it:

- **Entity correlation (Umpire-native, feasible now).** Tag every unit-of-work span (and the
  operation spans under it) with the entity it serves — `namespaceID / workflowID / runID /
  scheduledEventID`. The footprint of an edge is then *all operations tagged with this entity
  that occurred between the previous observed transition and this one*. This needs **entity tags
  on spans + a transition-delimited window**, not cross-service context propagation. It is
  co-occurrence, not proven causality — good enough because the Planner drives **one entity in an
  isolated namespace**, so ambiguity is low. This is the same routing-by-`EntityPath` the Monitor
  already does, lifted to operation spans.
- **Causal linking (faithful, needs a server change).** Persist the originating trace/span id
  into the task payload and start the task span with an OTEL **Link** back to it. This is the
  "full tracing setup" — it captures true causality across persistence/task boundaries. It is a
  real effort (context must be threaded through task creation and rehydration) and is the honest
  answer to *"we likely need a full tracing setup."*

**Recommendation:** build entity-correlation first (it reuses `EntityPath` and the
`chasm.transition` identity that already exist), and add span-links only where an operation
carries no entity identity (shard/membership/global calls) and the trace tree can't attribute it.
"Mostly OTEL" holds: capture is span-first; interceptors fill the two gaps OTEL doesn't cover
natively — **persistence** (`common/persistence/intercept`) and **fault realization** (the gRPC
`RPCFaultGenerator`).

### What counts as an "operation"

| Source | Observe via | Capability | Notes |
|---|---|---|---|
| Frontend / internal gRPC (history, matching) | OTEL server+client spans / internal-RPC interceptor | `traces` | `service/method` is the identity |
| Persistence reads/writes | `intercept.PersistenceInterceptor` (seam exists, wired `nil` today) | `internals` | `methodName` is the identity; the STAMP channel ([`UMPIRE_PRIOR_ART.md`](./UMPIRE_PRIOR_ART.md#what-umpire-can-learn-from-stamp)) |
| CHASM/HSM transitions | `chasm.transition` span events (built) | `traces` | the transition **delimiters** themselves |
| Timers / task scheduling | task-queue + timer spans | `traces`/`internals` | needed for timing faults (later) |

Footprint capture therefore lives in the `traces`+`internals` environments (`local-rpc`, and
`cicd` for the gRPC-only subset). It is **not** a canary capability — canary sees only `rpc`
(SPEC, *Environments & capabilities*).

## Storage format (sketch)

```jsonc
// tests/umpire/footprints/workflow_update.json  (checked in, reviewed)
{
  "entity": "WorkflowUpdate",
  "edges": {
    "admitted->accepted": {
      "operations": [
        { "kind": "grpc",        "id": "workflowservice/RespondWorkflowTaskCompleted" },
        { "kind": "grpc",        "id": "historyservice/RecordWorkflowTaskCompleted"   },
        { "kind": "persistence", "id": "UpdateWorkflowExecution"                       },
        { "kind": "grpc",        "id": "matchingservice/AddWorkflowTask"              }
      ],
      "capturedUnder": ["traces", "internals"],   // provenance: what saw each op
      "faultable":     ["grpc", "persistence"]     // what the environment can fault
    }
  }
}
```

Golden files are the deliverable the user asks to "check in as cached truth." Their **diff** is
the drift signal; their **content** is living documentation of what a feature transition costs at
the syscall level — reviewable, unlike an opaque trace.

## Judging under fault — the real oracle question

Under a fault the expected result is **not** "same happy path." The oracle is still the existing
rulebook, but it must encode *recovery expectations*:

- **Safety must always hold** — no illegal transition, no cross-entity invariant broken, even
  mid-fault. This is free (the rules already run).
- **Liveness must eventually hold** — a dropped `AddWorkflowTask` should be retried and the
  update should still reach `accepted`; if it stalls, `EntityProgress` fires. Also free.
- **The hard case: which faults are *supposed* to fail?** A non-retryable error on a terminal
  write may legitimately abort the operation; the model must know the difference between
  "degraded-but-recovers" and "broke an invariant." This is the one place new modeling is
  needed — likely a per-edge/per-fault *expected outcome* annotation (recovers ∣ aborts-cleanly ∣
  must-not-happen), the fault analog of the SAA `Outcome`. A **flagged issue** = a safety
  violation, an unmet liveness obligation, or an outcome that disagrees with the annotation.

## Feasibility

Ranked from "already true" to "genuine effort."

1. **Transitions are already delimited (built).** `chasm.transition` emits an entity-identified
   span event per CHASM edge; update-lifecycle spans do the same for updates. The Monitor already
   receives every span as a `SpanProcessor`. So the *delimiters* and the *ingestion path* exist —
   this doc's work is capturing the operations *between* delimiters, not building tracing from
   zero.
2. **The fault seam exists (built).** The imported `RPCFaultGenerator`
   (`common/rpc/faultinjection`, PR #9076) matches by request type + namespace across all
   services and can drop→error or modify responses, and it **fails the test if a registered fault
   never fires** — the SPEC's "no silent no-op" discipline, already enforced. Delay/corrupt fit
   the same callback.
3. **The persistence seam exists but is dormant.** `common/persistence/intercept.PersistenceInterceptor`
   is wired into every service in onebox as `nil` today (exactly how the `FaultInjector` sat
   before it was activated). Turning it on gives both the *observe* (persistence operations) and
   the *fault* (drop/delay a write) halves of the `internals` capability. Effort: real, and broad
   (a lot of generated store surface — the STAMP caveat).
4. **Entity-correlated capture is feasible without full tracing.** Because the Planner drives one
   entity in an isolated namespace, tagging operation spans with the entity and windowing by
   transition delimiters attributes most operations without cross-trace context propagation. Gaps
   (entity-less shard/membership calls) are the minority.
5. **Full causal tracing is the genuine effort.** Threading trace context through task
   persistence + rehydration (so a background task's span links to the request that scheduled it)
   is a server change touching hot paths. It is *not required* for a first useful version, only
   for faithful causality — decouple it and defer.
6. **Determinism is the sharpest risk.** Footprints must be stable to be checked in. Retries,
   sharding, host counts, timing-dependent branches, and map ordering all perturb the observed
   op-set. Mitigation is the SAA normalization discipline (drop volatile attrs, bucket attempt
   counts, canonical ordering) plus a **match policy** (exact vs. allowed-superset) per edge. Some
   transitions may be inherently unstable and only footprintable as a subset invariant, not an
   exact set — that must be an explicit, per-edge decision, not a silent flake.

**Verdict:** feasible incrementally. The gRPC-only slice (capture gRPC ops via `traces`, fault
via the imported generator) is reachable now on top of existing seams; the persistence slice
needs the dormant interceptor activated; the faithful cross-trace slice needs a server change and
should be deferred.

## Practicality — build order & cost

0. **Op capture (gRPC only).** Extend the Monitor's span ingestion to record, per entity-window,
   the gRPC operation spans between transition delimiters. Output: an in-memory footprint. No
   faults yet, no persistence. Reuses `traces` + `EntityPath`.
1. **Golden store + drift test.** Serialize footprints; add a conformance test that re-derives and
   diffs. **Shippable value with zero fault work** — living docs + drift detection for the
   call-level behavior of every modeled transition.
2. **Derive + inject gRPC faults.** For each captured gRPC op, generate `drop`/`delay` scenarios;
   drive-to-precondition (Planner) + arm (imported generator) + judge (rulebook). First real
   trace-derived chaos.
3. **Activate the persistence interceptor.** Add persistence ops to footprints and persistence
   faults to the plan (`internals`).
4. **Timing + full causal tracing (deferred).** Span-links across task boundaries; timer/deadline
   faults.

**Cost & placement.** This is a heavy, `traces`+`internals`, cluster-backed mode — **nightly /
opt-in, never per-PR** (SPEC, *Cheap*). The cross-product `edge × op × fault-kind` is large and
needs prioritization; absent the deferred guided-fuzz strategist, start with **one fault kind per
op** and a fixed budget, and `log()` what was skipped (no silent truncation).

## Potential

- **Faults become derived, not authored** — the user's primary goal. Every operation of every
  modeled transition gets systematic fault coverage that no one hand-listed, and the coverage
  denominator (footprint ops) is a natural coverpoint set (`UMPIRE_PLAN.md`).
- **The operational edge contract** complements the SAA *semantic* edge contract: together, an
  edge is pinned in both what-it-means and what-it-does. A footprint diff catches an accidental
  extra persistence write or a dropped matching call that no semantic rule would notice.
- **Living, drift-checked docs** of real call-level behavior — reviewed in source control, the
  thing the user wants "cached and true."
- **Regression capture** — a fault that once produced a violation freezes into a fixed
  `edge × op × fault-kind` regression, replayable deterministically (Driver *run/eval* split).
- **Portability honesty** — footprints are tagged with the capability that observed each op, so a
  `cicd` run knows precisely which operations it can and cannot fault, and reports the rest as an
  explicit skip rather than false confidence.

## Risks & open questions

1. **Footprint determinism** (sharpest). Some edges may only be stable as subset invariants; that
   must be explicit per edge. → normalization + per-edge match policy.
2. **Association fidelity.** Entity-correlation is co-occurrence, not causality; rare interleavings
   (two operations of the same entity in flight) can mis-attribute an op. → isolated namespace per
   run mitigates; span-links resolve it faithfully (deferred).
3. **Fault scoping precision.** The imported generator matches request-type + namespace; faulting
   "the *Nth* AddWorkflowTask of *this* transition" may need occurrence-counting in the callback
   (it already receives `ctx`, `fullMethod`, `req`). → extend the callback, don't rebuild it.
4. **Combinatorial explosion.** `edge × op × fault-kind × timing` is large. → budgeted, and the
   natural consumer of the deferred coverage-guided strategist.
5. **Oracle under fault.** Distinguishing "expected degradation" from "bug" needs per-edge
   expected-outcome annotations — the one piece of genuinely new modeling.
6. **Persistence interceptor breadth.** Activating it touches a large generated surface; scope to
   the stores footprints actually reference first.
7. **Event-time ordering** (the standing `UMPIRE_PLAN.md` gap) bites here too: windowing
   operations between transition delimiters needs order-sound timestamps, not observation-time
   `time.Now()`.

## Status

Nothing here is built. The **seams** it stands on are: `chasm.transition` delimiters + Monitor
span ingestion (built), the gRPC `RPCFaultGenerator` (built, PR #9076), the persistence-interceptor
seam (present, `nil`), and `EntityPath` routing (built). The first shippable unit is **Phase 0–1**
(gRPC footprint capture + golden drift test), which delivers living docs and drift detection with
no fault machinery at all.
