# What Umpire can learn from the SAA behavioral model

Notes from reading `main...dan/saa-model-tests-w-transitions-conformance-explorer`
([compare](https://github.com/temporalio/temporal/compare/main...dan/saa-model-tests-w-transitions-conformance-explorer)).
The branch adds a **behavioral model + conformance explorer** for the CHASM *activity*
archetype. It overlaps Umpire's territory but attacks it from the opposite direction, and on
several of Umpire's open problems (`UMPIRE_PLAN.md`) it is simply further along. This doc ranks
the ideas by how directly they hit our gaps. For the other design references see
[`STAMP_IMPORT.md`](./STAMP_IMPORT.md) and [`PITCHER.md`](./PITCHER.md).

## What the SAA model is

A pure, **server-free, executable spec** of how one archetype should behave, plus a harness that
drives a real implementation against it.

- **`model/vocabulary.go`** — the event alphabet (`Event`/`EventKind`), the start-time `Config`,
  and `AbstractState`, the projection of observable internal state.
- **`model/model.go`** — `Transition(cfg, state, event) -> Outcome`, a **total transition
  function**, plus response predictors `ExpectedDescribe` / `ExpectedHeartbeatFlags`. `Outcome`
  is the full API contract for an edge: next state, reject kind, and two task-invalidation bools.
- **`model/explore.go`** — pure graph helpers: `Fingerprint`, `Reachable(cfg, events)` (the whole
  reachable cell set computed from `Transition` alone, no server).
- **`model/validate/`** — Tier-1 static checks of the model against the *code's* declared state
  machine, no server.
- **`activity_conformance_test.go`** / `tests/activity_standalone_*` — Tier-2/3 explorers (BFS +
  random walk) that realize each event against a real engine and check every step against the model.
- **`tests/activity_parity_test.go`** — SAA↔WFA differential tests: the same trace through two
  surfaces of the same archetype, WFA as oracle.

It runs at three tiers, **cheapest first, one model reused across all three**:

| Tier | Environment | Cost | Checks |
|---|---|---|---|
| 1 | no server | ~1s | model is total (no unexpected panics); every model edge is reachable in the code |
| 2 | in-process CHASM engine + **virtual clock** | ~1s | BFS traversal + random walk incl. timeouts/backoff |
| 3 | onebox, real timers | slow | same explorers + operator commands + directed wall-clock traces + SAA↔WFA parity |

## The core inversion: an oracle, not a rulebook

This is the one idea to internalize. **Umpire's rules are a *partial* predicate** — a bag of
"this specific bad state must not happen" checks. **The SAA model is a *total* function** — for
every `(config, state, event)` it predicts the exact next state, the exact API error, and the
exact side effects. The harness then asserts equality at every step.

The consequence is decisive:

- A partial rulebook can only catch the badness someone thought to write a rule for. It passes
  vacuously on everything else — which is why `UMPIRE_PLAN.md` keeps circling "are our rules even
  exercised?", the dead-rule report, and scenario coverage as *the whole game*.
- A total oracle has **no vacuous pass**. Any deviation — a state you didn't anticipate, a wrong
  error code, a missing task invalidation — is a diff against the model, automatically. The
  "prove the rules aren't dead" problem doesn't exist because the model *is* the coverage target.

Umpire deliberately kept only the observe/model/judge half and split the invariant-per-rule style
across `safety`/`liveness`. The SAA branch shows the payoff of going all the way to an executable
model instead.

## Side-by-side

| | Umpire | SAA model |
|---|---|---|
| **What the model is** | entity FSMs + partial invariant rules | total `Transition` function + response predictors |
| **Judging** | rules fire on *bad* states (anticipated) | equality vs predicted `Outcome` every edge (exhaustive) |
| **Active side** | deferred to Pitcher (unbuilt) | **built**: BFS traversal + random walk generate the scenarios |
| **Coverage** | planned Scenario/Coverage subsystem (phased) | intrinsic: `Reachable()` *is* the target; BFS covers it |
| **State source** | reconstructed from gRPC + OTEL (fidelity gap) | read directly via `ReadComponent` (exact) |
| **Time** | `time.Now()` at fact-processing (the event-time gap) | **virtual clock**; timeouts realized by advancing it |
| **Scope** | multi-entity, cross-correlations | single archetype, deep |
| **Deployment** | rides along on the whole functional suite, per-PR, canary-aspirational | drives its own scenarios; white-box, functional-only |
| **Contract checked** | mostly state | state + reject kind + Describe + heartbeat flags + task-invalidation |

## What it does better (ranked by value to Umpire)

### 1. Total transition function kills the vacuous-pass problem
`Transition` is total over the RPC domain and every non-modeled cell is an explicit
`panic("unreachable")` (validated by `TestModelDecisionCoverage`). Umpire's central anxiety —
rules that pass because their precondition never fired — is structurally impossible here. This
subsumes several of our rules at once: an oracle that predicts the next state *is*
`StageMonotone` + `EntityTransitionLegality` + `StateConsistency` + "any unexpected transition",
for free and with a specific diff.

### 2. The active side already exists, and coverage falls out of it
`Reachable(cfg, events)` computes every reachable `(state, event)` cell purely from the model.
`traverse()` does a fingerprint-deduped, depth-bounded BFS over it against a real engine;
`randomWalk()` drives deep interaction sequences the bounded BFS never reaches. This is exactly
what `UMPIRE_PLAN.md` wants from **Pitcher** (the generator) *and* the **Scenario/Coverage**
subsystem (seed the catalog from the model, report unreached cells) — but derived from the model
for free instead of hand-curated and bolted on as a third subsystem. `TEMPORAL_SAASPEC_COMPLETENESS=1`
prints reachable-but-unexercised cells: the dead-rule report, already shipping.

### 3. Virtual clock answers the event-time / settle gap
Umpire's #2 fidelity gap is observation-time timestamps, and the `settleWorkflows` teardown hack
exists only because we can't cleanly observe time-based closure. The SAA Tier-2 explorer sidesteps
time entirely: `clock.EventTimeSource`, and timeouts/backoff realized by *advancing the clock to
the deadline*. It explores start-to-close, heartbeat, schedule-to-close, and retry backoff — the
wall-clock behavior that is prohibitively slow at Tier 3 — in ~1s and deterministically. For the
in-process functional tier, this is a direct, proven fix for our timestamp problem.

### 4. Direct state read removes the false-positive class
The harness reads `AbstractState` straight out of the component via `ReadComponent` and maps it
with `Abstract(Observed)`. There is no reconstruction, so there is no reconstruction-fidelity gap
— and `UMPIRE_PLAN.md` calls false positives from imperfect reconstruction "the whole game."
Trade-off: this is white-box and cannot run in canary. But it shows the honest high-fidelity end
of the spectrum our tier framework is supposed to span (see the unifying insight below).

### 5. Tier-1 static validation of the model against the code
`TestModelEdgesReachableInCode` reflects over the code's declared `activity.Transition*`
descriptors (`Sources`/`Destination`), builds the reachability graph, and asserts every status
change the model accepts is reachable in the code. `TestModelDecisionCoverage` asserts totality.
Both run with **no server in ~1s**. Umpire has nothing that cross-checks its FSMs against the
server's own state-machine declarations — this is a cheap, high-confidence layer we could copy
directly against the CHASM/HSM transition tables.

### 6. Differential testing across surfaces (parity)
`activity_parity_test.go` drives the *same* model trace through SAA and WFA and asserts identical
public info, with WFA as oracle. Two implementations of one archetype that must agree is a bug
magnet, and differential testing needs no hand-written expected values — the other surface
supplies them. Umpire has no equivalent. As Temporal grows archetypes with a shared contract
(activity-as-workflow-child vs standalone), this pattern gets more valuable.

### 7. Richer contract than state alone
An `Outcome` pins next state **and** reject `ErrorKind` (NotFound / FailedPrecondition / …)
**and** `AttemptTasksInvalidated` / `ScheduleToCloseTaskInvalidated`, checked as stamp deltas
across the edge — the model even documents that a task invalidation "is observable only as a
change in the underlying stamp." Plus `ExpectedDescribe` and `ExpectedHeartbeatFlags` predict the
public projections. Umpire mostly judges state; it barely models API responses despite having a
`ResponseRecorder`. Modeling the *whole* observable contract per edge catches error-code and
side-effect bugs our state-only rules miss.

### 8. Observability discipline at field granularity (`mask` / `SameObserved`)
`SameObserved` compares only the fields that are *live in the current status*; `mask()` zeroes
the rest (e.g. the pending-reset flags matter only in `ResetRequested`), and `Dispatchability` is
treated as latent — never asserted directly, only verified by whether a poll dispatches. This is
the same "what is actually observable here" concern our black/grey/white tiering raises, but
operationalized precisely: the oracle refuses to assert mechanism state nobody can observe.
`Fingerprint` also buckets `AttemptCount` at `min(n,3)` so retry loops converge to a finite graph
— the kind of care our reachability/coverage work will need.

## Where Umpire's approach is genuinely different (and still right)

Not everything transfers — the two tools sit at different points on a coverage/effort curve.

- **Portability.** Umpire reads only gRPC + spans precisely so a rule set can run in canary/Cloud
  where you cannot read internal state. The SAA model is white-box (`ReadComponent`, internal
  handlers, virtual clock) and is functional-only by construction. Our tier framework's whole
  point — push each invariant to the lowest tier that can express it — is a real axis the SAA
  branch doesn't address.
- **Ride-along breadth.** Umpire enforces over the *entire existing functional suite* per-PR
  without those tests knowing it exists: broad, shallow, opportunistic coverage for near-zero
  marginal cost. The SAA harness drives its *own* scenarios; it does not judge arbitrary tests.
- **Cross-entity correlations.** Umpire's most interesting rules are relational — speculative task
  ↔ update, update ↔ its workflow's close, worker poll ↔ update — with a registry, routing, and
  parent chains. The SAA model is one FSM in isolation; it has no cross-entity story. That is
  exactly where Umpire says "the interesting bugs live."
- **Authoring cost.** A total model per archetype is real, ongoing hand-authoring that must be
  kept in sync (the branch is honest about this — note the many `TODO(dan)` open questions *in
  the model itself*). Umpire's partial rules are cheaper per unit and accrete incrementally.

The blunt summary: **SAA = deep, exhaustive, expensive, one archetype, white-box.
Umpire = broad, opportunistic, cheap, many entities, portable.** Complementary, not competing.

## What to steal (concrete, tied to `UMPIRE_PLAN.md`)

1. **Give each entity an executable transition function, not just a Lifecycle FSM.** ✅ **Done.**
   `Lifecycle` now carries a pure, three-valued transition function `Classify(event) → Outcome`
   (`Advance` / `NoOp` / `Illegal`), mirroring the SAA model's advance/noop/reject; `Fire` is
   defined over it. Modelling benign duplicate/late/out-of-order/post-terminal spans as `NoOp`
   (instead of lumping every non-edge into "illegal") removed the exact false-positive that had
   kept the generic conformance rule unregistered. The generic `EntityTransitionLegality` is now
   registered and **replaces** `WorkflowUpdateStageMonotone` (deleted) — a regression is just not
   a legal edge — checking every `Lifecycled` type at once. `States`/`Events`/`Reachable`/
   `Validate` expose the graph for **Tier-1 static validation** (`tests/umpire/model` proves each
   default lifecycle is sound and `Classify` total, server-free in ms — the analog of the SAA
   `validate` package). Predicting the *API result* per edge (item 5) was considered next but does
   **not** fit: SAA's events *are* API calls, so an edge has an API outcome; Umpire's events are
   observed lifecycle **spans**, and the interceptor drops errored responses entirely
   (`interceptor.go` records only when `err == nil`). Porting it would mean restructuring facts to
   be API-call-centric — a different architecture, not an increment. See `UMPIRE_PLAN.md`.

2. **Derive the scenario/coverage catalog from the model with `Reachable`.** Phase 2 of the
   Scenario plan wants to seed the catalog from rule preconditions and emit a dead-rule report.
   A reachability walk over the entity FSMs produces that target set for free — no hand-curated
   equivalence classes. ✅ **Foundation built:** `Lifecycle.Reachable()` + `Cells()` give the
   per-entity coverage denominator (the decision table) with zero server, and
   `tests/umpire/model` renders each model as a living-doc table and asserts no dead events. Still
   to do: track *exercised* cells at runtime and aggregate across purges (the process-global
   `Coverage` sink) to turn the denominator into a real coverage/dead-rule report. Adopt
   `Fingerprint`-style bucketing so retry/attempt loops converge.

3. **Introduce a virtual clock in the in-process functional tier.** Directly addresses the
   event-time gap (#2) and dissolves the `settleWorkflows` hack: timeouts and closure become
   deterministic clock advances instead of teardown-timing guesses. This may be the single
   highest-leverage borrow for the false-positive triage that `UMPIRE_PLAN.md` calls priority 0.

4. **Add a Tier-1 static cross-check of our FSMs against the server's transition tables.**
   Reflect over the CHASM/HSM transition declarations the way `validate/` reflects over
   `activity.Transition*`, and assert every transition our model accepts is reachable in the code
   (and vice-versa). Server-free, ~1s, catches model drift before any cluster spins up.

5. **Model the full edge contract, not just state.** Model side effects (task invalidation) as
   observable deltas, as the SAA `Outcome` does. Note the caveat under item 1: predicting reject
   *kinds* per edge needs an API-call-centric fact model Umpire doesn't have (the interceptor
   doesn't even observe errored responses today), so it is a re-architecture, not a quick win —
   pursue only if API-error bugs prove worth a dedicated error-observation channel.

## The unifying insight

The SAA branch and Umpire's tier framework are the *same idea seen from two ends*. Umpire already
argues (`UMPIRE_PLAN.md`, "Observation tiers") that entities and rules should stay identical
across tiers while only the **fact source** changes — black-box reconstruct-from-wire for canary,
white-box direct-read for functional tests. The SAA model is precisely the **white-box,
direct-read, exact-fidelity** end of that spectrum, fully realized. It validates our tier thesis
and hands us a working reference for its high-fidelity extreme.

So the target architecture that reconciles both: **one abstract model per entity, expressed as a
total transition function**, checked by

- a **direct-read driver** (white-box, virtual clock, BFS/random-walk generated) for functional
  tests — maximum fidelity and exhaustiveness, à la SAA; and
- a **reconstruct-from-wire driver** (black-box, ride-along) for canary/Cloud — maximum
  portability, à la today's Umpire —

against the *same* model. That collapses Umpire's rulebook, the deferred Pitcher generator, and
the planned Scenario/Coverage subsystem into a single construct, and it is the direction the SAA
branch is pointing.
