# What Umpire can learn from STAMP

Notes from reading `main...stephanos/stamp`
([compare](https://github.com/temporalio/temporal/compare/main...stephanos/stamp)).
STAMP is a single prototype commit, 1755 commits behind `main` — treat it as a design
reference, not code to cherry-pick.

## What STAMP is

**S**cenario **T**ests by sending **A**ctions to **M**odels and checking their **P**roperties
(`common/testing/stamp/stamp.go`). It's the fuller version of the same idea Umpire pursues —
but STAMP covers the whole loop, where Umpire deliberately kept only the observe/model/judge
half:

| STAMP | Umpire analogue |
|---|---|
| `Model[T]` + typed `Prop[T]`/`Marker` + `Verify()` | `Entity` FSM + `Flag` + `Rule` |
| `mdl_router.go` routes RPCs to model instances | `Registry.RouteFacts` |
| **Actions / Scenarios / Patterns / Generators** (`act.go`, `test_scenario.go`, `tests/acceptance/patterns/`, `Gen[T]`) | *(none — Umpire's deferred "active" side)* |
| **Persistence interception** (`common/persistence/intercept/`) | *(none — Umpire observes only gRPC + OTEL)* |

Umpire is the more focused, further-along observe/judge engine (generation dirty-tracking,
namespace-scoped `Check`/`Purge`, per-PR default-on enforcement, explicit safety/liveness).
STAMP is broader but largely a prototype — its `Verify()` methods and most properties are
`TODO`. So the value is in **ideas**, ranked below by how directly they hit Umpire's current
gaps (see `UMPIRE_PLAN.md`).

## Worth importing (high value)

### 1. Derive the update lifecycle from the wire, not from server spans
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

### 2. Persistence interception as an observation (and fault) channel
`common/persistence/intercept/` wraps every persistence store (gowrap-generated) behind a
`PersistenceInterceptor func(methodName, fn, params...) error`. That gives a second, high-
fidelity observation channel Umpire lacks — you see actual writes (speculative task queue
usage, workflow close, registry mutations) directly, rather than inferring them from spans.
It's also the natural fault-injection seam (the dormant `FaultInjector` Umpire stubs).
**Action:** consider a persistence interceptor as the fidelity fix for the close-signal and
"observation-time" gaps — e.g. observe the real close from an execution-store write instead
of only the `CompleteWorkflowExecution` command. Weigh against its breadth (it's a lot of
generated surface).

### 3. Observe request **and** response (and the error) per RPC
STAMP models react to `IncomingAction[Req]` returning `func(OutgoingAction[Resp])`, so every
handler sees the request, the response, **and `ResponseErr`**. Umpire's decoder is largely
request-only (`ImportRequest`), with one response case (poll). A lot of truth lives in
responses and errors (aborts, resource-exhausted, deadline-exceeded). **Action:** make the
response/error path first-class in the decoder, not a special case.

### 4. Self-documenting markers (`doc:` tags)
STAMP markers carry documentation: ``Accepted stamp.Marker `doc:"accepted by the update
validator on the worker"` ``. That feeds both the "tests as living docs" goal and generated
names. Cheap to adopt for Umpire's `Flag`s. **Action:** add `doc:` tags to entity flags and
surface them in violation output / a generated property list.

## Worth considering (medium value)

### 5. Typed scope hierarchy
`Model[T]` + `Scope[*WorkflowExecution]` gives compile-time parent/child (Update ⊂ Execution ⊂
Workflow). Umpire's `EntityPath.Ancestors` is stringly-typed by comparison. A thin typed
wrapper over `EntityPath` would catch mis-parenting at compile time. Low urgency.

### 6. The active side, when Umpire is ready for it
STAMP's actions/scenarios/patterns/generators (`Gen[T]`, `GenName`, `util_names.go`,
`tests/acceptance/patterns/*`) are a concrete reference design for Umpire's explicitly-deferred
"drive scenarios / fuzz" half. When Umpire's judging is trusted (enforcement green across the
suite), this is the blueprint for the generative next step — including `poll_update_until_admitted`
-style reusable patterns and deterministic name generation.

## Deliberately *not* importing

- **The heavy generics + reflection model core** (`mdl.go`, `mdl_set.go`, `mdl_router.go`).
  Umpire's plainer `Entity`/`Registry`/`Rule` is easier to read and already does routing and
  dirty-tracking well. STAMP's model machinery is more abstract than its proven payload
  (`Verify()` is mostly TODO) — don't trade Umpire's clarity for unproven abstraction.
- **`Verify()`-on-the-model as the only judging path.** STAMP attaches verification to each
  model; Umpire's separate safety/liveness rulebook with generation dirty-tracking and
  namespace-scoped teardown is more capable and already enforced. Keep the rulebook.
- **Marker-only state (no FSM).** STAMP represents state as a bag of bool markers set
  reactively. Umpire's explicit `looplab/fsm` gives transition-legality and monotonicity for
  free — keep the FSMs (and see the "shared FSM" note in `UMPIRE_PLAN.md` discussion).

## Bottom line

The two ideas that would most improve Umpire *today* are **wire-derived update state**
(item 1) and **persistence interception** (item 2) — together they attack the biggest
structural weakness (dependence on bespoke production spans) and the model-fidelity gap. The
active/generative half (item 6) is the long-term direction but should wait until Umpire's
enforcement is proven false-positive-free.
