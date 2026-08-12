# Umpire — Prior Art

External designs Umpire draws from, consolidated. Each section is a self-contained reading of
one prior effort, ranked by value to Umpire; the table below is the synthesis — what we adopted
from each and what we deliberately left behind. For the system itself read
[`UMPIRE_SPEC.md`](./UMPIRE_SPEC.md).

| Source | What it is | Adopted | Left behind |
|---|---|---|---|
| **SAA behavioral model** | a server-free, total transition function + conformance explorer for one CHASM archetype | the *oracle inversion* (executable model per entity → no vacuous pass); model-derived coverage; a virtual clock for the event-time gap | nothing fundamental — white-box & single-archetype, complementary to Umpire's broad/portable stance |
| **STAMP** | a prototype scenario→action→model→property harness (the full loop) | wire-derived update state (no server spans); persistence interception; observe response+error; `doc:` tags | the heavy generics/reflection model core; `Verify()`-only judging; marker-only state (keep the FSMs) |
| **Omes** | kitchen-sink workflows that drive arbitrary Temporal behaviour in every SDK from one declarative `TestInput` | the workload substrate + `TestInput` format; the `project` gRPC harness; the replayability discipline | the load-shaped `GenericExecutor` (no predicate-guarded / model-directed steps) |
| **Alex's schedule PBT** | deep, narrow property-based analysis of the scheduler with `rapid` | property-analysis rigor; explicit fault-model auditing; auditable coverage/triage | nothing — deep-narrow vs broad-live are complementary, not redundant |


---

## What Umpire can learn from the SAA behavioral model

Notes from reading `main...dan/saa-model-tests-w-transitions-conformance-explorer`
([compare](https://github.com/temporalio/temporal/compare/main...dan/saa-model-tests-w-transitions-conformance-explorer)).
The branch adds a **behavioral model + conformance explorer** for the CHASM *activity*
archetype. It overlaps Umpire's territory but attacks it from the opposite direction, and on
several of Umpire's open problems (`UMPIRE_PLAN.md`) it is simply further along. This doc ranks
the ideas by how directly they hit our gaps. For the other design references see
[the STAMP notes](#what-umpire-can-learn-from-stamp) and [`UMPIRE_PLANNER.md`](./UMPIRE_PLANNER.md).

### What the SAA model is

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

### The core inversion: an oracle, not a rulebook

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

### Side-by-side

| | Umpire | SAA model |
|---|---|---|
| **What the model is** | entity FSMs + partial invariant rules | total `Transition` function + response predictors |
| **Judging** | rules fire on *bad* states (anticipated) | equality vs predicted `Outcome` every edge (exhaustive) |
| **Active side** | the Planner (planning core built; drive+fuzz WIP) | **built**: BFS traversal + random walk generate the scenarios |
| **Coverage** | planned Scenario/Coverage subsystem (phased) | intrinsic: `Reachable()` *is* the target; BFS covers it |
| **State source** | reconstructed from gRPC + OTEL (fidelity gap) | read directly via `ReadComponent` (exact) |
| **Time** | `time.Now()` at fact-processing (the event-time gap) | **virtual clock**; timeouts realized by advancing it |
| **Scope** | multi-entity, cross-correlations | single archetype, deep |
| **Deployment** | rides along on the whole functional suite, per-PR, canary-aspirational | drives its own scenarios; white-box, functional-only |
| **Contract checked** | mostly state | state + reject kind + Describe + heartbeat flags + task-invalidation |

### What it does better (ranked by value to Umpire)

#### 1. Total transition function kills the vacuous-pass problem
`Transition` is total over the RPC domain and every non-modeled cell is an explicit
`panic("unreachable")` (validated by `TestModelDecisionCoverage`). Umpire's central anxiety —
rules that pass because their precondition never fired — is structurally impossible here. This
subsumes several of our rules at once: an oracle that predicts the next state *is*
`StageMonotone` + `EntityTransitionLegality` + `StateConsistency` + "any unexpected transition",
for free and with a specific diff.

#### 2. The active side already exists, and coverage falls out of it
`Reachable(cfg, events)` computes every reachable `(state, event)` cell purely from the model.
`traverse()` does a fingerprint-deduped, depth-bounded BFS over it against a real engine;
`randomWalk()` drives deep interaction sequences the bounded BFS never reaches. This is exactly
what `UMPIRE_PLAN.md` wants from the **Planner** (the generator) *and* the **Scenario/Coverage**
subsystem (seed the catalog from the model, report unreached cells) — but derived from the model
for free instead of hand-curated and bolted on as a third subsystem. `TEMPORAL_SAASPEC_COMPLETENESS=1`
prints reachable-but-unexercised cells: the dead-rule report, already shipping.

#### 3. Virtual clock answers the event-time / settle gap
Umpire's #2 fidelity gap is observation-time timestamps, and the `settleWorkflows` teardown hack
exists only because we can't cleanly observe time-based closure. The SAA Tier-2 explorer sidesteps
time entirely: `clock.EventTimeSource`, and timeouts/backoff realized by *advancing the clock to
the deadline*. It explores start-to-close, heartbeat, schedule-to-close, and retry backoff — the
wall-clock behavior that is prohibitively slow at Tier 3 — in ~1s and deterministically. For the
in-process functional tier, this is a direct, proven fix for our timestamp problem.

#### 4. Direct state read removes the false-positive class
The harness reads `AbstractState` straight out of the component via `ReadComponent` and maps it
with `Abstract(Observed)`. There is no reconstruction, so there is no reconstruction-fidelity gap
— and `UMPIRE_PLAN.md` calls false positives from imperfect reconstruction "the whole game."
Trade-off: this is white-box and cannot run in canary. But it shows the honest high-fidelity end
of the spectrum our tier framework is supposed to span (see the unifying insight below).

#### 5. Tier-1 static validation of the model against the code
`TestModelEdgesReachableInCode` reflects over the code's declared `activity.Transition*`
descriptors (`Sources`/`Destination`), builds the reachability graph, and asserts every status
change the model accepts is reachable in the code. `TestModelDecisionCoverage` asserts totality.
Both run with **no server in ~1s**. Umpire has nothing that cross-checks its FSMs against the
server's own state-machine declarations — this is a cheap, high-confidence layer we could copy
directly against the CHASM/HSM transition tables.

#### 6. Differential testing across surfaces (parity)
`activity_parity_test.go` drives the *same* model trace through SAA and WFA and asserts identical
public info, with WFA as oracle. Two implementations of one archetype that must agree is a bug
magnet, and differential testing needs no hand-written expected values — the other surface
supplies them. Umpire has no equivalent. As Temporal grows archetypes with a shared contract
(activity-as-workflow-child vs standalone), this pattern gets more valuable.

#### 7. Richer contract than state alone
An `Outcome` pins next state **and** reject `ErrorKind` (NotFound / FailedPrecondition / …)
**and** `AttemptTasksInvalidated` / `ScheduleToCloseTaskInvalidated`, checked as stamp deltas
across the edge — the model even documents that a task invalidation "is observable only as a
change in the underlying stamp." Plus `ExpectedDescribe` and `ExpectedHeartbeatFlags` predict the
public projections. Umpire mostly judges state; it barely models API responses despite having a
`ResponseRecorder`. Modeling the *whole* observable contract per edge catches error-code and
side-effect bugs our state-only rules miss.

#### 8. Observability discipline at field granularity (`mask` / `SameObserved`)
`SameObserved` compares only the fields that are *live in the current status*; `mask()` zeroes
the rest (e.g. the pending-reset flags matter only in `ResetRequested`), and `Dispatchability` is
treated as latent — never asserted directly, only verified by whether a poll dispatches. This is
the same "what is actually observable here" concern our black/grey/white tiering raises, but
operationalized precisely: the oracle refuses to assert mechanism state nobody can observe.
`Fingerprint` also buckets `AttemptCount` at `min(n,3)` so retry loops converge to a finite graph
— the kind of care our reachability/coverage work will need.

### Where Umpire's approach is genuinely different (and still right)

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

### What to steal (concrete, tied to `UMPIRE_PLAN.md`)

1. **Give each entity an executable transition function, not just a Lifecycle FSM.** ✅ **Done.**
   `Lifecycle` now carries a pure, three-valued transition function `Classify(event) → Outcome`
   (`Advance` / `NoOp` / `Illegal`), mirroring the SAA model's advance/noop/reject; `Fire` is
   defined over it. Modelling benign duplicate/late/out-of-order/post-terminal spans as `NoOp`
   (instead of lumping every non-edge into "illegal") removed the exact false-positive that had
   kept the generic conformance rule unregistered. The generic `EntityTransitionLegality` is now
   registered and **replaces** `WorkflowUpdateStageMonotone` (deleted) — a regression is just not
   a legal edge — checking every `Lifecycled` type at once. `States`/`Events`/`Reachable`/
   `Validate` expose the graph for **Tier-1 static validation** (`tests/umpirev1/model` proves each
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
   `tests/umpirev1/model` renders each model as a living-doc table and asserts no dead events. Still
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

### The unifying insight

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

against the *same* model. That collapses Umpire's rulebook, the Planner (generator), and
the planned Scenario/Coverage subsystem into a single construct, and it is the direction the SAA
branch is pointing.

---

## What Umpire can learn from STAMP

Notes from reading `main...stephanos/stamp`
([compare](https://github.com/temporalio/temporal/compare/main...stephanos/stamp)).
STAMP is a single prototype commit, 1755 commits behind `main` — treat it as a design
reference, not code to cherry-pick.

### What STAMP is

**S**cenario **T**ests by sending **A**ctions to **M**odels and checking their **P**roperties
(`common/testing/stamp/stamp.go`). It's the fuller version of the same idea Umpire pursues —
but STAMP covers the whole loop, where Umpire deliberately kept only the observe/model/judge
half:

| STAMP | Umpire analogue |
|---|---|
| `Model[T]` + typed `Prop[T]`/`Marker` + `Verify()` | `Entity` FSM + `Flag` + `Rule` |
| `mdl_router.go` routes RPCs to model instances | `ModelState.RouteFacts` |
| **Actions / Scenarios / Patterns / Generators** (`act.go`, `test_scenario.go`, `tests/acceptance/patterns/`, `Gen[T]`) | *(none — Umpire's deferred "active" side)* |
| **Persistence interception** (`common/persistence/intercept/`) | *(none — Umpire observes only gRPC + OTEL)* |

Umpire is the more focused, further-along observe/judge engine (generation dirty-tracking,
namespace-scoped `Check`/`Purge`, per-PR default-on enforcement, explicit safety/liveness).
STAMP is broader but largely a prototype — its `Verify()` methods and most properties are
`TODO`. So the value is in **ideas**, ranked below by how directly they hit Umpire's current
gaps (see `UMPIRE_PLAN.md`).

### Worth importing (high value)

#### 1. Derive the update lifecycle from the wire, not from server spans
`tests/acceptance/model/workflow_update.go` gets the full update lifecycle **without any
server-side instrumentation**:
- `OnRespondWorkflowTaskCompleted` unmarshals the protocol `Messages` in the request body
  (`updatepb.Acceptance` → `Accepted`, `Rejection` → `Rejected`, `Response` → `Completed`),
  filtering by `ProtocolInstanceId == updateID`.
- It reads **response errors** to infer state: a `NotFound "workflow execution already
  completed"` or `ResourceExhausted "...workflow is closing"` sets `Aborted`.

This is the direct antidote to Umpire's #1 structural risk — the hand-placed OTEL emits in
history/matching hot paths (`emitUpdateLifecycleEvent`, `WorkflowExecutionCompleted`, …).
Most of what Umpire added production spans for is recoverable from the request messages and
response errors the interceptor already sees. **Action:** prototype a request+response decoder
for `RespondWorkflowTaskCompleted` and delete the corresponding server emits; keep spans only
for state with no wire signal at all.

#### 2. Persistence interception as an observation (and fault) channel
`common/persistence/intercept/` wraps every persistence store (gowrap-generated) behind a
`PersistenceInterceptor func(methodName, fn, params...) error`. That gives a second, high-
fidelity observation channel Umpire lacks — you see actual writes (speculative task queue
usage, workflow close, registry mutations) directly, rather than inferring them from spans.
It's also the natural fault-injection seam (the dormant `FaultInjector` Umpire stubs).
**Action:** consider a persistence interceptor as the fidelity fix for the close-signal and
"observation-time" gaps — e.g. observe the real close from an execution-store write instead
of only the `CompleteWorkflowExecution` command. Weigh against its breadth (it's a lot of
generated surface).

#### 3. Observe request **and** response (and the error) per RPC
STAMP models react to `IncomingAction[Req]` returning `func(OutgoingAction[Resp])`, so every
handler sees the request, the response, **and `ResponseErr`**. Umpire's decoder is largely
request-only (`ImportRequest`), with one response case (poll). A lot of truth lives in
responses and errors (aborts, resource-exhausted, deadline-exceeded). **Action:** make the
response/error path first-class in the decoder, not a special case.

#### 4. Self-documenting markers (`doc:` tags)
STAMP markers carry documentation: ``Accepted stamp.Marker `doc:"accepted by the update
validator on the worker"` ``. That feeds both the "tests as living docs" goal and generated
names. Cheap to adopt for Umpire's `Flag`s. **Action:** add `doc:` tags to entity flags and
surface them in violation output / a generated property list.

### Worth considering (medium value)

#### 5. Typed scope hierarchy
`Model[T]` + `Scope[*WorkflowExecution]` gives compile-time parent/child (Update ⊂ Execution ⊂
Workflow). Umpire's `EntityPath.Ancestors` is stringly-typed by comparison. A thin typed
wrapper over `EntityPath` would catch mis-parenting at compile time. Low urgency.

#### 6. The active side, when Umpire is ready for it
STAMP's actions/scenarios/patterns/generators (`Gen[T]`, `GenName`, `util_names.go`,
`tests/acceptance/patterns/*`) are a concrete reference design for Umpire's explicitly-deferred
"drive scenarios / fuzz" half. When Umpire's judging is trusted (enforcement green across the
suite), this is the blueprint for the generative next step — including `poll_update_until_admitted`
-style reusable patterns and deterministic name generation.

### Deliberately *not* importing

- **The heavy generics + reflection model core** (`mdl.go`, `mdl_set.go`, `mdl_router.go`).
  Umpire's plainer `Entity`/`ModelState`/`Rule` is easier to read and already does routing and
  dirty-tracking well. STAMP's model machinery is more abstract than its proven payload
  (`Verify()` is mostly TODO) — don't trade Umpire's clarity for unproven abstraction.
- **`Verify()`-on-the-model as the only judging path.** STAMP attaches verification to each
  model; Umpire's separate safety/liveness rulebook with generation dirty-tracking and
  namespace-scoped teardown is more capable and already enforced. Keep the rulebook.
- **Marker-only state (no FSM).** STAMP represents state as a bag of bool markers set
  reactively. Umpire's explicit `looplab/fsm` gives transition-legality and monotonicity for
  free — keep the FSMs (and see the "shared FSM" note in `UMPIRE_PLAN.md` discussion).

### Bottom line

The two ideas that would most improve Umpire *today* are **wire-derived update state**
(item 1) and **persistence interception** (item 2) — together they attack the biggest
structural weakness (dependence on bespoke production spans) and the model-fidelity gap. The
active/generative half (item 6) is the long-term direction but should wait until Umpire's
enforcement is proven false-positive-free.

---

## What Umpire can learn from Omes' kitchen-sink approach

Notes from reading the [Omes](../omes) load generator, focused on its **Kitchen Sink**
workflow and the machinery around it. Omes is Temporal's load/benchmark generator, but the
part that matters to us is orthogonal to load: it is the one place in the ecosystem that has
already solved **"drive arbitrary Temporal behaviour, in every SDK language, from a single
declarative description."** That is exactly the workload substrate the active side wants
(as `UMPIRE_SPEC.md` notes: *"reuse Omes kitchensink workflows as the
workload rather than a bespoke DSL"*), and it is the language-agnostic execution surface the
SAA model's "drivers" abstraction ([the SAA notes](#what-umpire-can-learn-from-the-saa-behavioral-model)) needs to reach beyond one
white-box engine. For the other design references see [`UMPIRE_PLAN.md`](./UMPIRE_PLAN.md).

### What Omes is

A CLI (`cmd/omes`) that runs three things, separately or together:

| Command | Role |
|---|---|
| `run-worker` | starts an SDK worker (Go/Java/TS/Python/.NET/Ruby) on a task queue |
| `run-scenario` | runs an **Executor** that *produces* load (workflows/activities/RPCs) |
| `run-scenario-with-worker` | both, plus an optional embedded dev server — the local all-in-one |

- **Scenario** — a named Go file in [`scenarios/`](../omes/scenarios) that picks an Executor
  and configures it. The file name *is* the scenario name.
- **Executor** — produces concurrent iterations of the workload. `GenericExecutor`
  (`loadgen/generic_executor.go`) is the engine: it spins goroutines up to `MaxConcurrent`,
  runs each iteration's `Execute(ctx, *Run)` in a retry loop, and honours
  `Iterations`/`Duration`/`MaxIterationsPerSecond`. `KitchenSinkExecutor` wraps it for the
  Kitchen Sink workflow.
- **Worker** — a per-language harness process that registers the Kitchen Sink workflow +
  activities and polls. SDK versions live in `versions.env`; workers can be built into Docker
  images pinned to a specific SDK version.

The whole design turns on one decision (README, "Design decisions"): **the workload is a DSL,
not hand-written workflows.**

### The Kitchen Sink workflow

> The Kitchen Sink workflow accepts a DSL generated by the `kitchen-sink-gen` Rust tool,
> allowing us to test a wide variety of scenarios without having to imagine all possible edge
> cases that could come up in workflows. Input may be saved for regression testing, or hand
> written for specific cases.

One workflow, implemented **identically in every SDK language**
([`workers/go/workerlib/kitchensink/kitchen_sink.go`](../omes/workers/go/workerlib/kitchensink/kitchen_sink.go),
plus `python/`, `typescript/`, `java/`, `ruby/`), that interprets a protobuf program. It is a
**deep module**: a tiny, stable interface (one protobuf message) hiding a large, growing space
of behaviours.

#### The DSL — one protobuf, the full contract

[`workers/proto/kitchen_sink/kitchen_sink.proto`](../omes/workers/proto/kitchen_sink/kitchen_sink.proto)
is the entire contract. A `TestInput` is *"everything needed to reproduce the test"*:

```
TestInput
├── WorkflowInput          # what the workflow does from the inside
│   ├── initial_actions     : []ActionSet     # run at startup
│   └── expected_signal_ids                    # signal dedup / CAN bookkeeping
├── ClientSequence         # what the client does from the outside
│   └── action_sets         : []ClientActionSet
└── WithStartClientAction  # signal-with-start / update-with-start
```

Two symmetric halves — this is the important structural point:

- **`WorkflowInput.initial_actions`** — the *inside* of the workflow. An `Action` is a oneof
  over `timer`, `exec_activity`, `exec_child_workflow`, `nexus_operation`, `send_signal`,
  `continue_as_new`, `set_patch_marker`, `upsert_search_attributes`/`memo`, `return_result`,
  `return_error`, `await_workflow_state`, `await_pending_actions`, nested `ActionSet`… An
  `ActionSet` runs sequentially or `concurrent`; nesting gives arbitrary trees.
- **`ClientSequence.action_sets`** — the *outside*. A `ClientAction` is a oneof over
  `do_signal`, `do_query`, `do_update`, `do_describe`, `do_standalone_nexus_operation`,
  `do_standalone_activity`, nested sets. These are **direct frontend RPCs the client issues**,
  independent of any worker — `DoStandaloneActivity` starts an activity via
  `StartActivityExecution` and polls it; `DoStandaloneNexusOperation` drives Nexus directly.

Cross-cutting knobs make each command an equivalence-class generator rather than a single
path:

- **`AwaitableChoice`** — attached to every awaitable command (timer, activity, child,
  nexus, signal). One oneof selects `wait_finish` / `abandon` / `cancel_before_started` /
  `cancel_after_started` / `cancel_after_completed` / `wait_started`. One command definition,
  six interaction shapes — this is where the interesting concurrency/cancellation edges come
  from.
- **Activity variants** — `noop`, `delay`, `payload`, `resources`, plus *behavioural* ones
  built for failure modes: `retryable_error` (fail N attempts then succeed), `timeout`
  (exceed start-to-close for N attempts), `heartbeat` (skip heartbeats for N attempts). These
  deliberately exercise retry/timeout/heartbeat machinery.
- **`is_local` vs `remote`**, retry policies, priorities, per-command task queues, versioning
  intent — all in the DSL.

#### How a worker interprets it

`KitchenSinkWorkflow` (Go, mirrored per language) sets up a `report_state` query, a
`do_actions_update` update handler, and a `do_actions_signal` signal handler, then runs
`initial_actions`. `handleActionSet` walks the tree — sequential sets run in order,
`concurrent` sets fan out into `workflow.Go` coroutines and await. `handleAction` is the big
oneof dispatch. `withAwaitableChoiceCustom` is the shared helper that implements the six
`AwaitableChoice` behaviours uniformly across activities, children, timers, nexus, and
signals. The signal/update handlers pipe the *same* `ActionSet` grammar back through
`handleActionSet`, so an update or signal can do anything an initial action can.

The client side has a mirror interpreter,
[`loadgen/kitchensink/client_action_executor.go`](../omes/loadgen/kitchensink/client_action_executor.go):
`ClientActionsExecutor` turns `ClientAction`s into `SignalWorkflow` / `UpdateWorkflow` /
`QueryWorkflow` / `DescribeWorkflowExecution` / `StartActivityExecution` calls, with
`with_start` mapping onto `SignalWithStartWorkflow` / `UpdateWithStartWorkflow`.

**The contract is language-independent; the observation surface is the wire.** Every worker
must produce identical behaviour for a given `TestInput`. That is the property Omes leans on
for cross-SDK differential testing, and it is precisely what makes the DSL a candidate common
workload for us.

#### The generator — `kitchen-sink-gen`

[`loadgen/kitchen-sink-gen`](../omes/loadgen/kitchen-sink-gen) is a Rust tool that emits random
`TestInput`s (the `fuzzer` scenario). Key operational facts that matter for reproducibility:

- Output is a `last_fuzz_run.proto` **binary**, replayable with `--option input-file=…`.
- A run prints its **seed**; `--option seed=…` reproduces — *but only if the config is byte-identical*, so the project prefers saved binaries to seeds.
- Interesting cases are curated into `scenarios/fuzz_cases.yaml`; the README keeps a **fuzzer
  trophy case** of real SDK/Core bugs it found. This is generative testing that has already
  paid out.

#### The `project` harness — native SDK code, driven over gRPC

Beyond the DSL, Omes has a **second execution mode**
([`workers/proto/harness/api/api.proto`](../omes/workers/proto/harness/api/api.proto)) for
driving **hand-written, Temporal-native** code in any language. Where Kitchen Sink has Omes own
the workflow (a generic interpreter) and drive it through *its own* client, `project` mode
**hands both the workflow and the driver logic to the project** and lets Omes own only the
*cadence* (run one iteration N times) and the *transport* (a gRPC contract):

```
service ProjectService {         // language-agnostic; each SDK implements the server side
  rpc Init(InitRequest)       returns (InitResponse)       // build client, project-specific setup
  rpc Execute(ExecuteRequest) returns (ExecuteResponse)    // one iteration: start + drive + verify
}
```

**One program, two roles.** A project builds into a single language program (selected with
`--app <name>` via `workers/<lang>/apps/registry.*`) that runs in two modes, dispatched on the
subcommand (`harness.Run(app)` on `argv[0]`):

- **`worker`** — a normal SDK worker: registers the app's workflows/activities and polls the task
  queue (`app.Worker`).
- **`project-server`** — a gRPC server implementing `ProjectService` (`app.Project`).

A full run has both processes; because they are the same program, the workflows the driver starts
are exactly the ones the worker serves — coherent by construction.

**The call sequence.** The runner (`scenarios/project/`, always Go, speaks gRPC to any language):

```
1. validate --option language / project-name / [version | prebuilt-project-dir | project-config-file]
2. build (workerctl.Builder) or load the project program
3. findAvailablePort(); exec  <prog> --app <name> project-server --port <p>   (startProjectProcess)
4. TCP-poll until listening (project-server-ready-timeout, 15s), then dial gRPC
5. Init(InitRequest{ execution_id, run_id, task_queue, ConnectOptions{ns, address, TLS, auth},
                     config_json })                                   ← once
6. a steady-rate GenericExecutor calls Execute(ExecuteRequest{iteration, task_queue}) per iteration
   → Omes's normal Iterations / Duration / MaxConcurrent / rate machinery drives the loop
7. teardown: close conn; SIGINT the project-server process group
```

On the harness side, `Init` builds an SDK client from `ConnectOptions` via the app's
`ClientFactory` and runs the app's optional `ProjectHandlers.Init` (search attributes, Nexus
endpoints, …); `Execute` calls the app's `ProjectHandlers.Execute(client, ctx)` — the user's
per-iteration code (start a workflow, drive it, verify). Returning an error is the only failure
channel (→ gRPC `Internal` → the iteration fails). `helloworld.App` is the reference: `Execute`
runs `HelloWorldWorkflow("World")` and checks the result.

| | Kitchen Sink mode | `project` mode |
|---|---|---|
| Workflow code | Omes (generic DSL interpreter) | **the project** (native, real-ish app) |
| Driver / client actions | Omes DSL (`ClientActionsExecutor`) | **the project** (`Execute`, native SDK client) |
| Load pattern & concurrency | Omes | Omes (**steady-rate only**, today) |
| Cross-language mechanism | one workflow reimplemented per SDK | one gRPC contract (`Init`/`Execute`) per SDK |

**How it fits the marriage.** `project` mode is the third realizer (`project adapter →
ProjectService.Execute` in the seam diagram above), and its role is specific:

- It is the vehicle for **Mode 2's intent-carrying menu** (see "Two modes" below): a real
  application's own signal/update state machine for the driver to walk, in any language, with
  Umpire observing on the wire — model-based testing against realistic code rather than a generic
  interpreter.
- It is the only realizer that exercises a **specific SDK's client plumbing** (update-with-start,
  signal-with-start, …), because the client calls run through the app's real SDK client in its
  language — not the Go raw-RPC driver.
- **The one generalization it needs to become reactive.** As-is, `Execute` is coarse — "do one
  whole iteration and verify," open-loop within the iteration. A reactive Mode-2 driver (the SAA
  explorer / Planner, guarding over the Umpire model) needs to choose the *next single action*
  after observing. That is the *"extend the `project` harness into a streaming `Step`-per-action
  RPC"* note under "Reconciling…": generalize `Execute` → a `Step(action) → effect` stream, the
  runner picking each action from the model and the harness realizing it through the SDK client.
  The `Init` / gRPC / two-role skeleton is already the right shape; only the granularity of the
  drive RPC changes.

### Why this matters to Umpire / Planner & Driver / SAA

Omes solves three problems the active side (Planner & Driver) and the model side (SAA) would otherwise
have to re-solve:

1. **A declarative, replayable workload.** `TestInput` is data: hand-write it for a targeted
   case, generate + save it for fuzzing, replay a binary for regression. A Play in
   `UMPIRE_DRIVER.md` is *"ordered/guarded []Action + input parameters"* — structurally the same idea
   at the client layer. Reusing the DSL means not inventing a second workflow-authoring
   grammar.
2. **One workload, every SDK, for free.** The Kitchen Sink is implemented once per language
   and kept behaviourally identical. Any driver that emits `TestInput` immediately drives
   Go/Java/TS/Python/.NET/Ruby workers. This is the only cheap path to *"SDK execution across
   any language"* — the SAA model's drivers otherwise reach exactly one (white-box, in-process)
   engine.
3. **Client-only RPC actions already exist.** `DoStandaloneActivity` /
   `DoStandaloneNexusOperation` / `do_query` / `do_describe` are frontend RPCs with **no
   workflow or worker involved** — black-box by construction, canary-portable. They line up
   directly with the SAA *standalone-activity* archetype (raw `StartActivityExecution` +
   poll) and with the Driver's black-box actions.

#### The seam that unifies it (see the marriage design)

The SAA driver states its own architecture in a comment: *"The event DSL is the archetype
model package; this file is the SAA adapter that realizes it… realizing each event as the
corresponding frontend RPC / poll / wall-clock wait."* So there is already an **event →
realizer** split. Omes supplies a second realizer target for that split:

```
                       ┌───────────────────────────────── realizers ─────────────┐
 SAA model.Event  ──▶  │  RawRPC adapter   → frontend gRPC (SAA standalone; canary) │
 (abstract trace)      │  Omes adapter     → TestInput → any-language SDK worker    │
                       │  project adapter  → ProjectService.Execute (native code)   │
                       └──────────────────────────────────────────────────────────┘
                                              │ all observed on the wire
                                              ▼
                                    Umpire (observe / model / judge), tier-gated
```

- **What the model owns:** the abstract event alphabet + total `Transition` (oracle,
  generator, coverage target).
- **What the realizer owns:** turning one abstract event into a concrete action — a raw
  frontend RPC (black-box, runs in canary) *or* an Omes `TestInput` action executed by an
  SDK worker in any language *or* a `ProjectService.Execute` call.
- **What Umpire owns:** observing whatever ran (gRPC/OTEL) and judging it against the *same*
  model — reconstruct-from-wire (black-box) for canary, direct-read (white-box) for functional.

That triangle — **model / realizer / observer** — is how Umpire and SAA marry while keeping
both raw-RPC and cross-language-SDK execution. It is written up separately; this doc is the
reference for the Omes half of it.

### Reconciling the ahead-of-time DSL with reactive RPCs

The central tension when the two realizers coexist: an Omes `TestInput` is **specified ahead
of time** (hand it a program, walk away — open-loop), whereas model-guided raw-RPC driving is
inherently **reactive** (observe the model → decide → act — closed-loop, the SAA driver's
`driveTrace`/`driveEvent` already work this way, reading state back between steps). If a
`TestInput` were "the whole test," the two could not both hold.

#### The tension is asymmetric between the two DSL halves

"Ahead of time" is not one property — it binds the two halves of the DSL differently:

| Half | Why it looks static | Is it really? |
|---|---|---|
| `WorkflowInput.initial_actions` (the *inside*) | workflow code must be deterministic — you cannot inject new commands from outside mid-run | **genuinely constrained** — except via signals/updates |
| `ClientSequence` (the *outside*) | a pre-declared list `ClientActionsExecutor` walks | **incidentally static** — it is ordinary live code in the Omes process; it *can* be reactive |

So the client plane can be made fully reactive; the workflow plane is reactive only at
signal/update granularity. Design around the asymmetry instead of fighting it.

#### The move: Kitchen Sink is an *interpreter*, not a *script*

Stop treating one `TestInput` as the test. The fixed, pre-compiled-in-every-language thing is
the **instruction set** (the DSL grammar); the unit specified ahead of time is **one
instruction**, and instructions can be minted reactively and streamed in at runtime.
Reactivity becomes planning latency, not a new language. Three mechanisms, all already
present:

1. **Long-lived servant workflow, fed by signal/update (per-round reactive planning).** The
   workflow already loops on the `do_actions_signal` channel and the `do_actions_update`
   handler, each carrying a *fresh* `ActionSet` piped through the same `handleActionSet`. The
   driver loops: observe the Umpire model → plan the next `ActionSet` → deliver it via signal
   (fire-and-forget) or update (synchronous, returns a value) → observe → repeat. "Ahead of
   time" shrinks from *the whole test* to *one round's actions*, each round chosen reactively.
   The workflow ends when a round returns a result/error/CAN.
2. **Compile locally-decidable guards into the DSL.** Kitchen Sink already encodes reactivity
   declaratively: `AwaitableChoice` (`cancel_after_started`, `wait_started`) is *"observe my
   command reached STARTED, then act"*; `AwaitWorkflowState` blocks on a k/v predicate;
   `await_pending_actions`, `wait_for_current_run_to_finish_at_end`. These are predicate-guarded
   actions evaluated *at the interpreter*, driven by the SUT's own signals — deterministic and
   cheap, no round trip.
3. **A separate reactive raw-RPC / fault plane for everything else.** Cross-entity guards
   (*"wait until this update is admitted, then drop the WFT carrying it"*), precise-instant
   timing, malformed requests, and fault injection cannot be baked into a deterministic
   workflow anyway. They stay in the SAA-style closed-loop driver, fired as black-box frontend
   RPCs (canary-portable) or grey/white-box injections.

#### The static/reactive line (a decision rule)

Per action, ask: **can the decision to fire it be made from information one SUT participant
already has locally?**

```
 statically known               → initial_actions        (open-loop, portable, replayable)
 depends on this participant's   → DSL blocking primitive  (AwaitableChoice / AwaitWorkflowState)
   own runtime state               reactive-at-interpreter, deterministic
 depends on cross-entity model,  → reactive driver plane   (raw RPC / inject, planned per-round,
   a precise instant, or a fault    delivered via signal/update or issued directly)
```

Maximize the top two — that is what buys any-language execution and replay; use the bottom only
where you must — those are the "interesting" pitches. This is the TigerBeetle/VOPR shape
`UMPIRE_DRIVER.md` gestures at: a cheap **declarative background plane** (Kitchen Sink `TestInput`,
open-loop, any language) carries the boring ambient traffic, while the interesting scenario is a
**reactive foreground plane** of predicate-guarded raw-RPC pitches on top. The two planes share
`EntityPath` addressing and deterministic IDs, so a raw-RPC pitch can target an entity the
Kitchen Sink workload created.

#### Two consequences

- **Cross-language reactivity is nearly free on the client side.** Most reactive client pitches
  — signal, update, describe, cancel, terminate, standalone activity — are just frontend RPCs
  the *driver* fires in Go against a raw client, regardless of worker language. The worker's
  language governs only workflow-*internal* behaviour. Extending the `project` harness into a
  streaming `Step`-per-action RPC (generalizing `ClientActionsExecutor` from "interpret a fixed
  list" to "serve one action on request") is needed only when a client action must run through a
  *particular SDK's* client plumbing.
- **Replay the realized trace, not the plan.** Reactive decisions depend on observed timing, so
  the *plan* is not reproducible but the *sequence that actually fired* is. Record the driver's
  decisions + the `TestInput`/`ActionSet`s it delivered and replay that exact trace — Omes' own
  discipline (*"save the binary, not just the seed"*, since a seed only reproduces under an
  identical config) and `UMPIRE_DRIVER.md`'s *"separate run from eval."*

The tension dissolves once Kitchen Sink stops being the whole test and becomes a **pre-installed,
any-language interpreter fed reactively**, with a principled line between declarative and reactive
and replay anchored on the realized trace.

#### Two modes: randomize the program vs. choose the path

The declarative and reactive planes correspond to two ways of using the SDK worker, and they are
**duals**:

- **Mode 1 — randomize the *program*, observe one fixed path.** Generate a random `TestInput`
  (à la `kitchen-sink-gen`), run it open-loop, judge it. Generative/fuzz; breadth.
- **Mode 2 — fix the *program* as a branch space, reactively choose the *path*.** Install a worker
  that exposes a menu of behaviours (all the signals/updates it *could* accept), and let the driver
  pick a path through it at runtime by which interactions it sends. Model-directed exploration;
  depth.

Mode 1 pins the trajectory and varies the code; Mode 2 pins the code and varies the trajectory —
same machinery, opposite knob.

| | Mode 1 — random behaviour | Mode 2 — chosen path over a branch space |
|---|---|---|
| **Loop** | open-loop (bake `TestInput`, walk away) | closed-loop (driver navigates — "interpreter not script") |
| **Plane** | declarative background | reactive foreground — *this is* predicate-guarded driving |
| **Who chooses** | generator, ahead of time | the driver, at each observed step |
| **Intent** | unknown per run | known per path |
| **Oracle it needs** | **must** be the total-function model — a random program has no hand-written expected values, so only a total oracle can judge (this is where "total function kills the vacuous pass" matters most) | model oracle *plus* the path's own known intent as a second, independent check, and cross-SDK parity |
| **Coverage** | breadth by luck, measured after the fact | intrinsic: the branch space *is* `Reachable()`; choosing paths *is* `traverse()`/`randomWalk()`; unreached branches = coverage gap |
| **Replay** | save the binary (`TestInput`) | save the realized trace (the choices made) |
| **Maps to** | SAA fuzzer / `UMPIRE_PLANNER.md` P4; the Omes fuzzer *with an oracle attached* | SAA explorer (BFS/random-walk) realized through the any-language worker; `UMPIRE_PLANNER.md` P2/P3 |

**Mode 2 is the SAA explorer generalized to drive the any-language worker instead of only the
in-process engine.** SAA's `traverse()`/`randomWalk()` walk the model graph and realize each event
against a real engine; Mode 2 installs a worker that exposes the whole event menu, then lets that
same explorer pick the path and realize it via signals/updates. Umpire observes on the wire, so it
stays language-agnostic. It is also the honest resolution of the ahead-of-time/reactive tension:
the **capability** is installed ahead of time (the full branch space, deterministic, compiled into
every SDK), while the **selection** is reactive (the driver picks the branch by which signal/update
it sends) — the worker's guards (`AwaitWorkflowState`, `AwaitableChoice`) offer the choices; the
driver's guard over the Umpire model decides which to take when.

The Mode-2 "menu" can carry more or less intent:

- **Generic Kitchen Sink interpreter** — `do_actions_signal`/`do_actions_update` run *any*
  `ActionSet` sent, so the menu is the whole grammar, unbounded and implicit. Maximal flexibility,
  but the worker carries no intent — Mode-1 breadth applied reactively.
- **A `project`-harness app** — a real-ish application with specific named handlers. The menu is
  bounded and intent-carrying, and the driver walks the app's own signal/update state machine
  (classic model-based testing). This is what makes "observe behaviour along that path" meaningful
  for a *real application's* state space rather than an arbitrary interpreter.

### What to steal (concrete)

1. **Adopt `TestInput` as the SDK realizer's target format**, not a new grammar. A Driver
   "Omes adapter" compiles an abstract trace (or a Play) into `TestInput` and hands it to
   `KitchenSinkExecutor` / a language worker. Free multi-language reach.
2. **Reuse the standalone/client RPC actions as black-box actions.** `DoStandaloneActivity`
   and friends are already worker-free frontend calls — the canary-portable action set, and the
   direct analogue of the SAA standalone driver.
3. **Copy the replayability discipline.** Saved binary programs (not just seeds) + a curated
   `fuzz_cases.yaml` + a trophy case. This is exactly the *"separate run from eval, capture the
   run, re-check offline"* constraint in `UMPIRE_DRIVER.md`, already proven in Omes.
4. **Reuse the `project` gRPC harness shape** for driving native SDK code the Driver doesn't
   own — an existing, language-agnostic "external orchestrator drives a worker process over
   gRPC" contract.
5. **Treat the per-language behavioural-equivalence of Kitchen Sink as a differential oracle.**
   The same `TestInput` across two SDKs should look identical on the wire — the same
   surface-parity idea SAA's `activity_parity_test.go` uses (SAA↔WFA), lifted to cross-SDK.

### Where Omes stops (and Umpire/SAA continue)

- **Omes drives; it barely judges.** A scenario fails only if an `Execute` returns an error
  (README, "Scenario Failure"): correctness is on the author. Omes has no model, no
  invariant rulebook, no oracle. That is the entire Umpire/SAA contribution — Omes is the
  *arm*, not the *eyes*.
- **The DSL is workflow-authoring-level, not archetype-internal.** Kitchen Sink actions are
  "run an activity / send a signal"; SAA events are "heartbeat / start-to-close elapses /
  reset with keep-paused." A realizer must *lower* an archetype event to one-or-more DSL
  actions (or to a raw RPC when no workflow is involved) — the DSL is a target, not a drop-in
  replacement for the event alphabet.
- **No fault injection.** Omes cannot drop/delay/corrupt an RPC; it only issues well-formed
  calls. The grey/white-box actions (`FaultInjector`) remain ours to build.
- **Load-shaped, not exploration-shaped.** `GenericExecutor` is a steady-rate concurrency
  driver; it has no notion of predicate-guarded steps or model-directed BFS. The
  guard-over-the-model driving (`UMPIRE_PLANNER.md`) and the `Reachable`-derived traversal
  (the SAA notes) sit above Omes, using it only as the execution backend.

---

## Alex's schedule property-testing approach vs. Umpire

Notes from reading `main...sch-property` on `chaptersix/temporal`
([compare](https://github.com/chaptersix/temporal/compare/main...sch-property)).
The branch adds a **property-based analysis harness** for the Temporal *scheduler's*
matching-time computation. It overlaps Umpire's territory — both are model-first,
deterministic, evidence-producing test systems — but attacks it from the opposite end of
the scope/depth axis. This doc summarizes the branch, then compares it against Umpire
(`UMPIRE_SPEC.md` / `UMPIRE_MONITOR.md` / `UMPIRE_PLAN.md` / `UMPIRE_DRIVER.md`). For the
other design references see [the SAA notes](#what-umpire-can-learn-from-the-saa-behavioral-model) and
[the STAMP notes](#what-umpire-can-learn-from-stamp).

### What the branch is

An **analysis-only, test-only** property campaign against one narrow, deep computation:
`ListScheduleMatchingTimes` — "given a `ScheduleSpec` and a query range, what times
match, and how much work does it take?" It is explicitly *not* a production change: nothing
is wired into the legacy or CHASM scheduler, no proto changes, no guard is selected. The
deliverable is **evidence** — findings, decision records, and a replayable corpus — for a
*later, separate* implementation project.

113 files, all under `service/worker/scheduler/propertytest/` (plus a top-level spec doc
and `go.mod`/`go.sum` for `pgregory.net/rapid`). The shape:

- **`schedule-matching-times-property-analysis-spec.md`** (903 lines) — the execution
  spec: scope, the 12 questions the analysis must answer, the calculator contract, the
  error taxonomy, the semantic model, generator rules, and library choice.
- **Copied computation** — the production matching computation is *copied* into
  `_test.go`-only files (an `analysis` prefix where names would clash) so it can be
  instrumented and its signatures changed without touching production and without being
  importable by production packages.
- **`calculator_*` / `spec_copy_test.go` / `calendar_copy_test.go`** — the copied
  calculator plus a `ComputeMatchingTimes(ctx, spec, start, end, jitterSeed, options)`
  contract returning `ComputeResult{Times, Work}` where `Work` is a `WorkBreakdown`
  (next-time calls, calendar search steps, interval/exclusion checks, excluded-candidate
  retries, result-loop steps).
- **`generators_test.go` / `model_test.go`** — a **semantic `ScheduleModel`** generated
  *first* and rendered into protobuf forms (structured calendar, calendar string, cron,
  mixed). The model exposes `MatchesNominal(time.Time)` as an **independent oracle** that
  never calls the copied calendar search.
- **`properties_test.go` / `validation_properties_test.go` / `calculator_contract_test.go`**
  — the properties, checked with `rapid`.
- **`redteam_*` / `operational_*` / `experiment_test.go` / `parity_test.go`** — the two
  red-team campaigns (computational + operational) and parity vs. the untouched production
  computation.
- **`plans/01..04.md`** — staged execution plans (validity hardening → computational red
  team → operational red team → results processing), gated so each produces inputs the
  next needs.
- **`FINDINGS.md` + `results/` + `testdata/`** — reviewed campaign evidence, JSON result
  bundles with schemas, and minimized `rapid` counterexample failfiles committed as
  regressions.

#### The method, in five moves

1. **Copy, don't refactor.** Copy the minimum complete computation, preserve behavior,
   add parity tests against the original *before* changing anything. Copying buys the
   freedom to change signatures and collect diagnostics; it explicitly does **not** make
   the copy a correctness oracle — semantics are judged by a *separate* model.
2. **Generate valid-by-construction from a semantic model.** Build a `ScheduleModel`, then
   render it to protobuf. No "generate arbitrary protobuf and discard 99%." Invalid cases
   are a valid base plus **exactly one labeled mutation**, so the expected error is
   unambiguous and shrinking is clean.
3. **Three independent oracles.** (a) the semantic model's `MatchesNominal`; (b) a
   brute-force reference over reduced finite horizons; (c) metamorphic relations and
   parity against the production computation. The copy is never its own oracle.
4. **Deterministic work budget.** A versioned (`v1`) "iteration" is a budget tick per unit
   of repeated search work — never wall-clock, goroutine scheduling, or map order.
   `WorkBreakdown.Total` must equal the sum of budgeted categories. This makes cost itself
   a property (exact `N` succeeds, `N-1` fails with `ErrIterationLimit`).
5. **Red-team, then record.** Adversarial fixed-seed searches (Pareto-elite candidates
   over generations) drive cost to its worst case; operational campaigns add cancellation,
   deadline, concurrency, caching, memory, race, and fuzz dimensions. Everything lands in
   `FINDINGS.md` / `DECISIONS.md` with a mutation kill matrix.

#### What it actually found

- A **normalized-midnight arithmetic bug** in timezone-calendar validation (`Pacific/Apia`
  boundary) and a **`Lord Howe` repeated-half-hour divergence** where the production
  calendar search omits the first occurrence of an ambiguous local time — preserved as a
  regression and a production follow-up, *not* fixed (out of scope).
- A **nil-calendar-element-before-protobuf-cloning** defect (cloning normalizes nil into a
  valid-default empty message).
- Hard **cost evidence**: the worst reviewed valid case needs ~9.5M work units; a 10,000
  cumulative-work cap rejects 4 of 6 reviewed *valid* cases — i.e. the "obvious" guard is
  wrong. No production limit was selected; guards were deferred or rejected with evidence.
- An **11-of-11 mutation kill matrix** — each planted defect is killed by a named property
  (`PROP-ORDERING-START-EXCLUSIVE`, `PROP-EXCLUSION-SUBTRACTION`, …), demonstrating
  sensitivity to the *planned* fault set (explicitly not completeness).

### The core contrast: deep-narrow property analysis vs. broad-live acceptance testing

This is the one framing to internalize. **Alex's harness is a microscope** — one
computation, lifted out of the server, hammered by generated inputs with independent
oracles and exact cost accounting, offline and hermetic. **Umpire is a wide-angle
observer** — the whole live server, watched passively across every functional test, judged
by invariants over interacting entities.

They are not competitors; they sit at opposite ends of a scope/fidelity trade:

| | Alex's PBT harness | Umpire |
|---|---|---|
| **Unit of test** | one pure-ish computation (`ListMatchingTimes`) | the whole running Temporal server |
| **SUT relationship** | *copied out* into a `_test.go` package, instrumented | *observed in place*, never modified |
| **Where it runs** | offline, in-process, hermetic; no cluster | ride-along on a live functional-test cluster |
| **Input source** | **generated** (`rapid`), valid-by-construction from a semantic model | **whatever the existing tests drive** (Monitor); a Driver that generates actions is specced-but-unbuilt |
| **Oracle** | independent semantic model + brute force + parity + metamorphic | per-entity executable FSMs (`Classify`) + cross-entity relational rules |
| **Judgement style** | properties (universally-quantified predicates over inputs) | safety rules (every observation) + liveness rules (eventually) |
| **Shrinking / minimization** | first-class via `rapid`; minimized failfiles committed | none — a violation is whatever real traffic produced |
| **Cost as a target** | central: deterministic work budget, exact-boundary properties | not modeled |
| **Fault injection** | operational red team (cancellation/deadline/cache/race) around the copied fn | `FaultInjector` hook exists, dormant; is the Driver's job (unbuilt) |
| **Cross-entity reach** | none — single computation, no entities | the differentiator: speculative-task ↔ update ↔ workflow-close |
| **Portability tiers** | n/a (hermetic) | black/grey/white-box, canary-portable subset |
| **Primary output** | evidence: `FINDINGS`/`DECISIONS`/result bundles/corpus | violations that fail real tests, per-PR |
| **Maturity** | 3 of 4 plans complete, reviewed, evidence banked | Monitor built + enforced; Driver + coverage specced |
| **Production intent** | explicitly deferred (analysis-only) | enforced today across the suite |

#### Where they genuinely agree

Both were designed by someone who internalized the same principles — several of them are
almost verbatim across the two:

- **Model-first, and the copy/observation is not the oracle.** Alex: "copying the
  computation does not make the copy an independent correctness oracle … semantic
  properties must use a separate model, brute-force oracle, or metamorphic relationship."
  Umpire: entities are executable models, and a total `Classify` means no vacuous pass. Both
  refuse to let the thing-under-test grade its own homework.
- **Determinism & replay, run separated from eval.** Alex commits minimized `rapid`
  failfiles and versioned result bundles; the budget is deterministic by construction.
  Umpire's SPEC lists "deterministic & replayable" and "separate run from eval" as
  constraints, and the Driver doc repeats it.
- **Valid-by-construction generation over filter-and-discard.** Alex's generator rules ban
  `Filter`/`SkipNow` for validity and build values directly; the "one labeled mutation"
  rule for invalid cases is exactly Umpire's Driver "Invalid… variant" idea for
  negative-space coverage.
- **Versioned definitions so results aren't silently compared across changes.** Alex
  versions the iteration definition (`v1`), validator (`v1`), and context-check
  (`context-check-v1`) and refuses to aggregate across them. Umpire tiers facts by
  provenance and refuses to let a grey-box rule "pass" where its facts can't arrive.
- **A structured result/error vocabulary instead of pass/fail.** Alex's `valid-success` /
  `valid-empty-query-result` / `invalid-*` / `indeterminate-validation-budget` /
  `*-budget-exhausted` vocabulary mirrors Umpire's insistence that "observed nothing" must
  be an explicit skip, never a silent pass.

#### What Umpire can take from it

Ranked by how directly it hits an open Umpire gap (`UMPIRE_PLAN.md`).

1. **A mutation kill matrix answers "are our rules even exercised?" — mechanically.**
   Umpire's *whole-game* worry is dead/vacuous rules (`ContinueAsNew` almost certainly
   never fires; the scenario-coverage subsystem is the planned fix). Alex's kill matrix is
   the same idea from the other side: plant a defect switch per rule, prove the rule's
   outcome changes. Umpire could add a **mutation-test harness** — test-only mutation
   switches on the model, each asserted killed by a named rule — as a *cheaper, immediate*
   down payment on the dead-rule report, before the full generative Driver+Coverage lands.
   It directly answers the "unvalidated enforcement" acute risk.
2. **Cost/work as a first-class modeled property.** Umpire models lifecycle correctness but
   says nothing about *how much work* an operation took. Alex's deterministic work budget
   (a tick per unit of search, `Total == sum of categories`, exact-boundary properties) is
   a clean pattern for turning performance envelopes into invariants — a natural future
   Umpire rule class ("this update settled within N observed steps"), and it dovetails with
   the event-time fidelity gap.
3. **`rapid` as the generation engine when the Driver arrives.** The Driver doc reaches for
   coverage-guided fuzzing but defers the strategist. Alex's harness shows `rapid` doing
   exactly the deterministic, shrinking, replayable generation the Driver wants — including
   `rapid.MakeFuzz` to reuse a property under Go's coverage-guided fuzzer, and state-machine
   support for metamorphic *sequences*. That state-machine mode is a candidate substrate for
   Driver **flows** (guarded action sequences), and it's already a vetted dependency.
4. **Evidence discipline as a deliverable.** Alex banks reviewed campaigns, decision
   records with false-positive tables, and a schema-validated corpus. Umpire's enforcement
   currently produces violations but no comparable durable "here's what we checked, here's
   what we deferred and why" artifact. Adopting the `FINDINGS`/`DECISIONS` habit would make
   Umpire's tier-coverage reports and triage decisions auditable.

#### Where Umpire is ahead (and why the approaches are complementary, not redundant)

- **Cross-entity, whole-system reach.** Alex's harness has no notion of interacting
  entities — it is one computation. Umpire's differentiator is precisely the invariants a
  single-computation (or single-archetype, cf. SAA) model *cannot state*: speculative task
  ↔ update, update ↔ its workflow's close. A property test of `ListMatchingTimes` will never
  catch a scheduler that computes the right times but then *acts* on them wrongly against
  the rest of the system.
- **Portability across deployments.** Alex's harness is hermetic by design; Umpire's tier
  model aims the same rules at canary/Cloud where you have no internal insight. Different
  problem, and Umpire owns it.
- **Zero-authoring, ride-along enforcement.** Alex's campaigns are hand-designed analyses;
  Umpire judges *every* functional test with no per-test assertion writing. That breadth is
  the thing Alex's depth can't provide.

#### The synthesis

The two are the two axes of the same discipline. Alex proves you can take **one deep
computation** and, with generated inputs + independent oracles + exact cost accounting,
extract genuine bugs and hard evidence hermetically. Umpire proves you can take the **whole
live system** and judge cross-entity behavior for free on every test. The gap each leaves
is the other's strength:

- Point Alex's method at Umpire's model → a **mutation kill matrix** that kills the
  dead-rule risk now, and **cost properties** as a new rule class.
- Point Umpire's method at Alex's findings → the `Lord Howe` / `Apia` divergences are exactly
  the kind of thing a **black-box scheduler rule** (observe scheduled vs. actual fire times
  over a running schedule) would catch *in situ*, closing the loop from "analysis found it"
  to "enforcement guards against regression."

The ideal end state is one shared model with two consumers on *two* axes: Umpire's
Driver/Monitor across the live system, and a `rapid`-driven property layer drilling into the
deep computations (scheduler matching, calendar search, and their kin) that the wide-angle
observer can only see the surface of.
