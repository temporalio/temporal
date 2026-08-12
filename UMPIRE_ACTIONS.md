# UMPIRE — The Actions Model

> **Status: implemented (Phases 1–5).** This document is both the design and, now, a description
> of working code — see the **Implementation** section below and `PLAN.md` for the phased build
> and what remains. The hand-coded `EnvFunc`s the "Why" describes have largely been retired in
> favour of a generic runtime + declared actions + a planner that computes the drives.

## Why

Umpire today has two of the three layers it needs to auto-drive coverage:

- **Entity models** — per-entity lifecycle FSMs (`umpire.Lifecycle`): states, transitions,
  and traits (`MustProgress`, `Success`/`Failure` dispositions, `Needs(Capability)`,
  `RequiresHosting`). These are the *state space*.
- **Observations** — the Monitor decodes `chasm.transition` telemetry (and gRPC/HTTP
  traffic) into facts, routes them to entity instances by stable identity (`WorkflowID:RequestID`),
  and records which edges the real implementation actually traversed (coverage) plus any
  illegal transitions (conformance).

What's missing is the **causal layer**: *which driver operation causes which entity
transitions*. Today that lives in hand-coded `EnvFunc`s — one per scenario (drive a workflow
Nexus op, drive a standalone op, async-start-then-complete, force a timeout, hold an HTTP
call, terminate, …). Each is a bespoke encoding of "do X, wait for state S, then do Y."

The **Actions model** makes that causal structure declarative. The goal:

> entity model + actions model + faults model + one happy-path run
> ⇒ umpire *infers* how to drive, *auto-generates* the drivers, *plans* sequences that cover
> every edge, and *explores* interesting combinations in parallel — removing the manual
> encodings.

This is, in shape, a **planning domain**: PDDL/STRIPS-style operators (actions with
preconditions and effects) layered over the entity FSMs, with a planner synthesizing
edge-covering action sequences.

## Background: what "PDDL/STRIPS-style" means

STRIPS (Stanford Research Institute Problem Solver, 1971) is the classic AI-planning
formalism: describe actions by their **preconditions** and **effects**, then let a search
algorithm assemble them into a sequence that reaches a goal. PDDL (Planning Domain Definition
Language, 1998) is the standardized *language* that generalized STRIPS and is still the lingua
franca of automated planning. The pieces:

- **State = a set of true facts** (predicates / fluents). Closed-world: anything not listed is
  false. E.g. `{ at(robot, A), clear(B) }`.
- **Action (operator) = precondition + effect.** The effect is an **add list** (facts it makes
  true) and a **delete list** (facts it makes false):

  ```
  action move(?from, ?to):
    precondition:  at(robot, ?from)
    effect:        add at(robot, ?to);  delete at(robot, ?from)
  ```

- **Goal = a set of facts to achieve.** **Plan = a sequence of actions** from the initial state
  to a state satisfying the goal.
- **Planner = the search** for that sequence: forward from the start, backward from the goal
  (*goal-regression* — "to make G true, pick an action whose effect adds G; its preconditions
  become new sub-goals"), or heuristic search.

PDDL's contribution was splitting a reusable **domain** (typed predicates + *parameterized*
action schemas — "lifted" operators with variables like `?from`) from an instance-specific
**problem** (objects, initial state, goal), and adding typing, negative/quantified
preconditions, conditional effects, numeric fluents, and durative (timed) actions.

### How umpire maps onto it

| PDDL/STRIPS | Umpire |
|---|---|
| fact / predicate | an entity's FSM state — `state(op1, scheduled)`, `state(wf1, started)` |
| world state | the tuple of all entity states the Monitor tracks |
| action schema (lifted) | an `Action` — the entity selectors are its `?parameters` |
| precondition | `requires [ entity @ state ]` |
| effect (delete old + add new) | `effects [ entity : transition ]` — a transition *is* "delete old-state fact, add new-state fact"; a `new` entity adds a fresh entity in its initial state |
| goal | coverage: every edge traversed |
| plan | the action sequence the driver runtime executes |

The Nexus-start actions, written STRIPS-style:

```
action start-standalone(?op):                 ; StartNexusOperationExecution
  precondition:  not exists(?op)
  effect:        add exists(?op), state(?op, scheduled), hosting(?op, standalone)

action cmd-schedule(?op, ?wf):                 ; ScheduleNexusOperation command
  precondition:  state(?wf, started)
  effect:        add exists(?op), state(?op, scheduled), hosting(?op, embedded), child(?op, ?wf)

action handler-async-ack(?op):                 ; the mock handler returns async
  precondition:  state(?op, scheduled)
  effect:        delete state(?op, scheduled); add state(?op, started)
```

The planner chains them by goal-regression: goal `state(op, started)` regresses to
`handler-async-ack`, whose precondition `state(op, scheduled)` regresses to `start-standalone`
or `cmd-schedule` — and out pops a driver sequence, no hand-coded `EnvFunc`.

### Where umpire departs from vanilla STRIPS

Two departures, and both are already addressed by the design below:

1. **Non-determinism.** Classic STRIPS actions are deterministic. `ExecuteOperation` on its own
   has several possible outcomes (started / succeeded / failed / backing_off), which is **FOND**
   (Fully-Observable Non-Deterministic) planning — that yields *contingent policies* ("if it
   went to backing_off, do X"), not linear plans, and is much harder. This is exactly why the
   [environment-as-actions](#the-key-move-model-the-environment-as-actions-too) move matters:
   making `handler:AsyncAck`, `callback:Complete`, `timer:ForceTimeout` first-class
   *controllable* actions collapses the non-determinism back into deterministic STRIPS, where
   planning is cheap and plans are linear.
2. **Coverage goal, not a reach-goal.** Classic planning reaches *one* goal state; umpire wants
   to *visit every edge at least once* — a set-cover / test-path-generation problem. In practice
   that is repeated goal-reaching: pick an uncovered edge, treat "be in its `from` state and fire
   it" as the goal, plan to it, drive, mark covered, repeat — precisely the
   [auto-drive loop](#the-auto-drive-loop). Faults ride on top as extra non-goal-advancing
   operators, explored combinatorially.

## The core inversion

Today each entity **edge** carries a passive "how" trait (`Needs`, `RequiresHosting`). The
actions model **inverts the index**:

- today: `edge → how it's realized`
- actions: `action → { edges it causes }`

That inversion is what unlocks the two structural facts hand-coding hides:

1. **Fan-out** — one action can cause transitions on *several* entities.
   `RespondWorkflowTaskCompleted` carries a *command set*; one call can schedule an activity,
   start a timer, and schedule a Nexus operation — three entities from one action.
2. **Hosting establishment** — the *action* determines whether the resulting entity is
   `Standalone` or `Embedded` (`StartNexusOperationExecution` vs the `ScheduleNexusOperation`
   workflow command), rather than it being an intrinsic property of the entity.

## The Action schema

```
Action {
  name        // "StartNexusOperationExecution" | "cmd:ScheduleNexusOperation"
              // | "handler:AsyncAck" | "callback:Complete(ok|fail|cancel)" | "timer:ForceTimeout"
  realization // Mechanism: client-RPC | worker-command | handler-response
              //           | completion-callback | timer   (the Footprint — see UMPIRE_TRACING.md)
  hosting     // establishes / requires: Standalone | Embedded | Any
  requires    // preconditions:  [ entity-selector @ state ]
  effects     // [ entity-selector : transition ]   ← possibly MANY entities
  params      // request/response field paths        → mutation targets
  faultable   // points in the realization where faults attach
}
```

`effects` and `requires` reference entity **selectors** — `new`, "the op created by action X",
or "any Workflow @ started" — so the actions form a relationship graph over the entity models.

### Worked example — NexusOperation start (both hostings)

- `StartNexusOperationExecution` — realization = client-RPC; **hosting = Standalone**;
  requires = ∅; effects = `[ NexusOp(new) : unspecified→scheduled ]`.
- `cmd:ScheduleNexusOperation` — realization = worker-command; **hosting = Embedded**;
  requires = `[ Workflow @ started ]`; effects = `[ NexusOp(new, child of that Workflow) : unspecified→scheduled ]`.

Both produce the *same* entity edge (`unspecified→scheduled`) on the *same* lifecycle — the
difference is entirely in the action (which RPC/command, which hosting). This is the concrete
answer to "how do we model standalone vs embedded": **one lifecycle, two actions.**

## The key move: model the environment as actions too

Effects look non-deterministic only because the *handler* and the *completion callback* are
treated as an uncontrolled environment: `ExecuteOperation` → then "succeed/fail/start depends."
But umpire **controls** them — the mock handler and the completion HTTP client are ours. So
promote them to actions:

- `handler:SyncOk`, `handler:AsyncAck`, `handler:OpFailed`, `handler:RetryableError`,
  `handler:Canceled`
- `callback:Complete(ok | fail | cancel)`
- `timer:ForceTimeout(from = scheduled | backing_off)` (the `NexusOperationForceTimeout` hook)

Now every effect is **deterministic given the action**, and the whole system is one
controllable operator set. The hand-coded async-completion dance — "return async, wait for
STARTED, deliver completion" — becomes three planned actions where the callback action has a
`requires [ op @ started ]` precondition the runtime waits on.

## Layering (worker behavior on top of the RPC contract)

```
  Worker / kitchensink   actions with realization = worker-command
                         (cmd:ScheduleNexusOperation → a kitchensink workflow calling ExecuteOperation)
  ─────────────────────
  Actions                operators whose `realization` references the RPC footprint;
                         faults attach here as decorators
  ─────────────────────
  RPC contract           raw methods + request/response shapes
```

The **driver becomes generated.** A small, fixed set of realization primitives —
issue-RPC, configure-kitchensink, set-handler-response, deliver-callback, inject-fault,
wait-for-state — and the plan picks which primitive per action. Every bespoke `EnvFunc`
(`nexusExec`, `nexusStandaloneExec`, `nexusExecComplete`, `nexusExecForceTimeout*`, the hold
drive, the terminate drive) collapses into *declared actions + those primitives*.

## Faults fold in as action decorators

A fault is a perturbation of an action's `faultable` points — drop / hold / fail an RPC in the
action's footprint. These are exactly the primitives already built (`armDrop`, `armHold`, the
HTTP fault RoundTripper, the force-timeout hook), attached declaratively instead of by hand.
Because a fault references the *same* footprint the action declares, "fault the schedule call"
is well-typed rather than a guessed method name. The search space becomes
**plans × fault-injection-points**.

## The auto-drive loop

1. **Happy-path run → ground the model.** Observe the actual `(action → transitions)` and
   `(action → RPC footprint)`; reconcile declared vs observed (the same drift/conformance
   pattern already used for illegal transitions). The model is *checked against reality*, not
   trusted. `FaultEachObservedCall` already captures the per-transition footprint — this is
   the seed.
2. **Goal = cover the edge set** (optionally × hosting × fault).
3. **Plan synthesis** (goal-regression): for each uncovered edge, find an action whose effect
   includes it; plan a route (the existing entity planner) to its preconditions; emit an
   action sequence.
4. **Auto-drive**: execute each sequence in its own testEnv, **in parallel**, the runtime
   resolving `requires @state` with wait-for-state sync (as `awaitNexusOpState` does today).
5. **Explore combinations**: coverage-guided, like a fuzzer over the action space — prioritize
   novelty (new entity-state tuples, new edge, new fault×edge pair) under a budget.

## Honest hard parts

- **Residual non-determinism** — timing races, server-internal ordering, the `backing_off`
  window. Env-as-actions removes most; the rest needs wait-for-state + retry, and some edges
  stay "best-effort" (already marked as such).
- **Combinatorial blow-up** — needs coverage-guided prioritization and a budget, not an
  exhaustive cross-product.
- **Identity / selectors** — "the op created by action X" → the observed entity relies on the
  `RequestID` routing already built; the actions model leans on that identity work.
- **Grounding gaps** — observed reconciliation validates *exercised* actions; a rare declared
  action stays unvalidated until first driven.
- **Effect multiplicity vs. state** — an action's effect set may depend on prior state
  (a command set differs by workflow logic). Model these as distinct actions or as
  parameterized effects; don't over-generalize a single action.

## What it removes / makes declarative

- The per-scenario `EnvFunc`s → declared actions + generated drivers.
- The hand-written "reach state S, then do Y" sequences → planned from `requires`/`effects`.
- The hand-picked fault targets → derived from action footprints.
- The hand-tuned coverage assertions → "cover the edge set" as a planning goal.

The endgame: **the model is the test.** Drivers, fault placements, and exploration plans are
synthesized, not written.

## Implementation

Built as `PLAN.md` describes, split along umpire's existing framework/registration seam.

**`common/testing/umpire/action.go`** — the domain-agnostic schema and runtime:
- `Action`, `Ref`, `Pre`, `Effect`, `Kind` (`ClientRPC` / `WorkerCommand` / `HandlerResponse` /
  `CompletionCallback` / `Timer` / `Fault`).
- Interfaces `Realizer`, `RealizeContext`, `StateOracle`, `EffectResolver`, `VisitedOracle` —
  implemented Temporal-side, so `common` stays free of `testcore`/RPC deps.
- `Drive` — installs standing/reactive actions, fires proactive actions in order (each waiting
  on its preconditions via the oracle), then confirms the final action's effects. **Only
  proactive actions wait on preconditions**: a reactive action passes through its precondition
  state as a transient the client can't observe, so waiting on it would race.
- `Reconcile` — returns declared effects the run did not produce (drift), same intent as the FSM
  conformance check, one layer up.

**`tests/umpirev1/action/`** — the Temporal concretions:
- `action.go` — `Ctx` (the `RealizeContext`, with fault cleanups), `Oracle` (`StateOracle` +
  `VisitedOracle` over the Monitor's `ModelState`), `Resolver` (`EffectResolver` over the
  lifecycles), `ResponsePolicy` (a programmable mock Nexus handler), the realizers, and the
  declared `NexusOperation` actions for both hostings.
- `plan.go` — `actionFor(from, event, hosting)` (the registry), `PlanEdge` (routes to `from`
  via the entity planner, maps each event to its action, validates the target edge's hosting),
  and `AutoCoverPlans` (one plan per settling edge — the coverage list, computed from the model).
- `fault.go` — `Drop` / `Hold` fault actions arming the `RPCFaultGenerator` (gRPC + Nexus HTTP),
  and `FaultVariants`.
- `plan_test.go` — unit tests for `PlanEdge` (no cluster).

**`tests/umpire_probe_test.go`** — one generated driver, `nexusGenExecPlan(plan)`, runs any
plan through `umpire.Drive` + `umpire.Reconcile`; the exploration computes its drive list from
`AutoCoverPlans()`. Seven bespoke `EnvFunc`s were retired into it; `Resilience` runs the
learned-footprint fault exploration over a generated plan; `TestPlanEdge` /
`TestProbeNexusGeneratedCompletion` / `TestProbeNexusFaultAction` are the round-trip proofs.

**Two edges stay bespoke, by nature** (no atomic action): the `backing_off→scheduled` retry
reschedule and `started→timed_out` (a real schedule-to-close timer — the force-timeout hook
fires on the attempt, not once started).

## What remains

- ~~**Coverage-guided fault exploration**~~ **(done)** — `ScheduleFaults(plans, budget)` ties
  `AutoCoverPlans` / learned footprints × per-execution testEnvs into a novelty-prioritized,
  budget-bounded drive list: each distinct fault target is scheduled once (breadth) before any
  repeat, and the overflow is reported as `dropped` — never a silent truncation. It is the
  deterministic upgrade of the uniform-random `TestProbeNexusRandomized`. See
  `tests/umpirev1/action/schedule.go` and `TestProbeNexusCoverageGuidedFaults`. (Smarter novelty —
  weighting a fault by the *new edges* it would exercise — remains a future upgrade, and feeds the
  "coverage-guided sampling" open question in [`UMPIRE_MATRIX.md`](./UMPIRE_MATRIX.md).)
- ~~**`Faultable` from the learned footprint**~~ **(done)** — the static field was renamed
  `Action.Entry` (the client-entry RPCs a Drop just fails on) and fault targeting now derives from
  the *observed* footprint: `LearnFootprint` drives a plan under observation, `FaultTargets` reduces
  the result (observed − entry − ambient) to the internal calls, and `FaultVariants(plan, learned)`
  builds one Drop-variant per target. See `tests/umpirev1/action/fault.go` and
  `TestProbeNexusLearnedFootprint`.
- **Beyond `NexusOperation`** — a second entity is now driven by the same generic runtime. The
  Nexus-specific `actionFor`/`PlanEdge`/`settlingEdges` were split into an entity-agnostic core
  (`planEdge` / `settlingEdgesFor`, parameterised by a lifecycle + an `actionForFunc`), the `Oracle`
  was generalised over any `Lifecycled` entity, and `workflow.go` adds the Workflow-family registry
  and realizers. Two levels are modelled:
  - **`Workflow`** (by id) — the logical handle / aggregate, with `StartWorkflow`/`CompleteWorkflow`
    actions proving the planner generalises to a richer lifecycle (`TestWorkflowPlanEdge` /
    `TestWorkflowAutoCoverPlans`).
  - **`WorkflowRun`** (by RunID, a child of `Workflow`) — the run-precise execution entity, so
    multiple runs of one WorkflowID (continue-as-new / retry / reset) are distinct. Its lifecycle is
    `created→completed` (the completion span carries the RunID), which matches observation exactly,
    so `TestProbeWorkflowGenerated` drives it to `completed` with a **clean `Reconcile`**. The
    completion span now yields two facts (`WorkflowExecutionCompleted` → `Workflow`,
    `WorkflowRunCompleted` → `WorkflowRun`) via a decoder that allows several facts per event.

  Known gaps / follow-ups on this entity:
  - **Observe `WorkflowStart`.** The `WorkflowStarted` fact decodes the *history*
    `StartWorkflowExecution`, which the frontend interceptor does not see, so `Workflow.started` (and
    a run-level `started`) is unobserved. Fix: decode the frontend request and resolve its namespace
    *name*→id via the seeded map (as `RecordRejection` does). Deferred by choice; the run drive is
    clean without it.
  - **`WorkflowRun` beyond completed** — `failed`/`canceled`/`terminated`/`timed_out`/
    `continued_as_new` transitions, and a `started` once observation lands.
  - **Run identity across CAN / reset / retry** — the single-run `WorkflowRun` correlates by the
    RunID from the start response, which does not scale to server-minted successor runs. The
    lineage-graph design (run nodes + CAN/reset/retry edges from observed telemetry, correlated by
    relationship, not by a pre-known id) and the namespace pre-seed vs. run-lineage distinction are
    written up in [`UMPIRE_IDENTITY.md`](./UMPIRE_IDENTITY.md); its foundation is emitting
    run-lifecycle telemetry with lineage attributes.
  - More entities (Activity, WorkflowUpdate).
- ~~**Footprint reconciliation**~~ **(done)** — an action now declares an expected footprint
  (`Action.Footprint`), and `ReconcileFootprint(plan, observed)` grounds it against the learned
  footprint: an expected internal call that never fired, or an observed non-ambient call outside the
  plan's `Entry ∪ Footprint`, is wire-level drift (the analog of `Reconcile`'s effect drift). Opt-in
  per action. See `tests/umpirev1/action/footprint.go` and `TestProbeNexusLearnedFootprint`.

## Relationship to the other umpire pieces

- **Entity models** (`common/testing/umpire/lifecycle.go`, `tests/umpirev1/model/*`) — the state
  space the actions move through.
- **Footprint / Mechanism** (see UMPIRE_TRACING.md) — the RPC-level realization an action
  references; the actions model is the semantic operator, the footprint is its wire-level
  detail.
- **Planner** (`tests/umpirev1/planner`) — routes over entity edges with capability and hosting
  constraints; `PlanEdge` (in `tests/umpirev1/action`) reuses it, mapping each route event to an
  action.
- **Probe / drivers** (`tests/probe`, `tests/umpire_probe_test.go`) — the probe's judge /
  coverage / verdict machinery is unchanged; only the drive is now generated
  (`nexusGenExecPlan`), and the exploration's drive list is computed by `AutoCoverPlans`.
- **Faults** (`common/rpc/faultinjection`, the probe's `armDrop`/`armHold`, the
  `NexusOperationForceTimeout` hook) — the realization primitives; `Drop`/`Hold`/`timer:ForceTimeout`
  actions arm them, and the probe's `FaultEachObservedCall` supplies the learned footprint.
