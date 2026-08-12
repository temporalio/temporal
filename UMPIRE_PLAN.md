# Umpire — Status & Plan

> **Status: authoritative roadmap.** This opening section supersedes older snapshots and proposals
> retained later in this document for design history.

Current state, a critical read against the goals, gap analysis, and rule inventory. For the
*why* read [`UMPIRE_SPEC.md`](./UMPIRE_SPEC.md); for *how it fits together* read
[`UMPIRE_MONITOR.md`](./UMPIRE_MONITOR.md).

## Current snapshot

- **Framework:** `common/testing/umpire` provides entity routing, lifecycle conformance,
  safety/liveness rules, facts, actions/planning, rejection capture, and the sparse regression
  compiler/executor under `common/testing/umpire/regress`.
- **V1:** `tests/umpirev1` remains an explicit compatibility and reference implementation. Its
  active monitor registers two safety and two liveness rules and is selectable through
  `WithUmpireMonitorFactory(umpirev1.NewMonitor)`.
- **V2:** `tests/umpire2` is the suite-wide `testcore` default. Its protocol compiles the fact,
  entity, action, action-gap, relation, and sparse-regression catalogs before monitor
  construction. Its active monitor registers four safety and two liveness rules and adds
  WorkflowRun, Activity, richer Nexus observations, and relation-backed link consistency.
- **Parity:** focused contracts cover active v1 plans, representative routing and completion
  payloads, rule registration, and the default-factory cutover. V2 is intentionally a strict
  superset; v1 stays available for explicit compatibility runs.
- **Sparse regressions:** seven functional proofs cover ordinary completion,
  completion-before-start, cancellation failure followed by terminal cancellation, shared
  handlers, timeout, standalone Activity links, and callback-after-caller-completion. Deeper
  imperative assertions still need callback-reference/idempotency and payload/link plus
  terminal-storage predicates before those tests can be retired.
- **Reusable slices:** typed runtime relations, semantic coverage, deterministic pairwise
  generation, bounded normalized tracing/refinement, and enum/integer/payload error domains are
  implemented. Broader Temporal adapters and external validator-backed domains remain follow-ups.

## Ordered next steps

1. Run broader functional-suite triage under the v2 default and classify any model-fidelity gaps;
   use explicit v1 only for a documented compatibility exception.
2. Model callback-to-operation and callback-to-handler references plus duplicate-response
   idempotency, then retire the corresponding imperative mechanics.
3. Add hashed payload/link predicates and an explicit terminal-storage observation for ordinary
   completion without persisting raw payload data.
4. Wire action coverage and action/verdict trace events at the executor boundary, and add a small
   Temporal catalog adapter over the generic pairwise generator.
5. Extend trace association toward checked-in causal footprints and add validator-backed error
   domains only when the server exposes a reusable validator registry.

## Useful parity gate

V2 must cover every entity, fact, registered rule, and active planner action used by the v1
default monitor; representative shared observations must yield equivalent state and verdicts.
V2 may remain a strict superset. Retired `_old.go` experiments and source-layout equality are not
part of parity. V1 stays in the repository and remains selectable through
`WithUmpireMonitorFactory`.

## Historical snapshot and design record

The material below captures earlier implementation snapshots and rationale. Where it conflicts
with the current snapshot above, the current snapshot is authoritative.

The pipeline is built and, as of the latest changes, **enforced suite-wide**:

- **Framework** (`common/testing/umpire/`): registry + generation dirty-tracking,
  safety/liveness `RuleRegistry`, `FactLog`, gRPC interceptor, OTEL span processor. Unit-tested.
- **Domain** (`tests/umpirev1/`): 4 entities, 14 facts, 11 registered rules (3 safety,
  8 liveness), each with a positive + negative test. The generic
  `EntityTransitionLegality` is built + unit-tested but **not registered**: a functional
  suite run surfaced a false positive (a `Workflow` sees `complete` while still in
  `created` because its `start` was unobserved). `Classify` now treats such forward
  jumps over unobserved states as legal — which fixed the false positive **and** made
  the rule **vacuous**: the current lifecycles are all converging DAGs with zero
  possible illegal transitions (measured: 0 illegal cells across all three entities),
  so registering it would be a never-firing rule (false confidence). It regains teeth
  only with event-time ordering, or a future lifecycle with isolated branches.
- **All 14 facts decode from live traffic.** Facts now carry the namespace, so entities are
  rooted at a `Namespace` (`EntityPath.Ancestors`, root-first).
- **Namespace-scoped, per-test enforcement is wired.** `CheckNamespace` + `PurgeNamespace`
  let one shared umpire serve many concurrent tests. `CheckAndPurgeUmpire(t, nsID)` runs at
  teardown — via `FunctionalTestBase.TearDownTest` for classic suites and via a `t.Cleanup`
  registered in `NewEnv` for `TestEnv` tests — and **fails the test on any violation**, then
  purges that namespace so nothing leaks between tests.

This flips the project's status: it is no longer "wired but never asserted." It now judges
**every** functional test. That is a real milestone — and it moves the central risk from
"unproven" to "unvalidated enforcement," which is more acute (below).

## Are we meeting the goals? (critical)

| Goal | Verdict | Why |
|---|---|---|
| Separate actions from assertions | ✅ mechanism realized | Every functional test is now judged by the umpire without writing assertions; the observe/judge split is real and running. Cross-context reuse (nightly/canary) still needs the server-side emits deployed there. |
| Terse tests | ⚠️ now possible, not yet done | The auto-check mechanism exists, but no test has actually *deleted* its hand-written assertions to rely on it. Ceiling remains: rules cover *invariants*, not the specific expected-value assertions that dominate functional tests. |
| Tests as living docs | ⚠️ partial | The rulebook is a readable property catalog; rule overlap still muddies it. |
| Find bugs earlier, cheap, fuzzing base | ⚠️ now active, value gated on false positives | Enforced per-PR across the suite, with per-test purge (the unbounded-growth worry is gone). Whether it *helps* now depends entirely on the false-positive rate — which is unmeasured. |

### The acute risk right now: enforcement is on but unvalidated

Enforcement (`Errorf`) is live before the rule set has been run against the whole suite. Any
false positive now **fails a real, unrelated test**. The known false-positive vectors are
concrete, not hypothetical:

- **Workflow close is observed for only one path.** `WorkflowExecutionCompleted` fires only
  from the `CompleteWorkflowExecution` command handler. A test whose workflow fails, times
  out, is cancelled/terminated, or continues-as-new leaves its updates non-terminal in the
  model → `LossPrevention`/`Completion`/`ContinueAsNew` fire at `CheckNamespace` → the test
  fails for a non-bug.
- **Observation-time timestamps.** Entity `…At` fields are `time.Now()` at fact processing,
  not event time; timestamp-comparison rules (`Closure`) rely on span arrival order.
- **Model fidelity generally.** Any behaviour the model reconstructs imperfectly can trip a
  rule on correct behaviour.

Until the suite has been run under enforcement and every violation triaged, this change is
not safe to merge on. **That triage is now the whole game.**

## Environments & capabilities (strategic)

Rules, facts, and actions differ in *how much of the system they need* — to observe and to
drive. This must be a first-class, explicit distinction: it decides what the umpire can run in
each deployment (CHASM-direct, in-process RPC, CICD, **canary / Temporal Cloud**).

The old scalar tier (`black ⊂ grey ⊂ white`) was too coarse: it conflated observing with
driving and assumed a total order the real environments don't obey — canary can't drive; CICD
drives and sees traces but has no internals; CHASM-direct reads and drives internals with no
wire at all. Model capabilities as **independent flags on three axes**; an **environment** is a
named profile = the subset it grants.

| Axis | Capabilities |
|---|---|
| **Observe** (fact sources) | `rpc` (frontend req/resp/errors: Start/Signal/Update, `PollWorkflowExecutionUpdate`, `DescribeWorkflowExecution`, `GetWorkflowHistory`) · `traces` (OTEL spans + internal-service RPC interceptors) · `internals` (direct CHASM / persistence / mutable-state read) |
| **Drive** (action realizers) | `rpcDrive` (public API / SDK calls) · `faults` (inject at internal RPC / persistence / timing seams) · `directDrive` (call CHASM transitions directly, no wire) |
| **Transport** | `inproc-noserver` · `inproc-server` · `remote` |

The four environments as profiles:

| Environment | observe | drive | transport |
|---|---|---|---|
| `local-chasm` | `internals` | `directDrive` (+`faults`) | in-process, no server |
| `local-rpc` | `rpc`+`traces`+`internals` | `rpcDrive`+`faults` | in-process server |
| `cicd` | `rpc`+`traces` | `rpcDrive` | remote / onebox |
| `canary` | `rpc` | observe-only (opt. `rpcDrive`) | remote |

`black`/`grey`/`white` survive only as shorthand for common *observe* bundles
(`rpc` / `+traces` / `+internals`); the mechanism is the flag set, not a scalar.

### Why it must be explicit
1. **False confidence in canary.** Run a rule needing `traces` where those facts never arrive
   and the entity simply never advances — the rule *passes*. "Observed nothing → no violation"
   reads as "healthy," the worst possible canary outcome. A missing capability must be an
   **explicit skip**, never a silent pass. Same on the drive side: a `faults` action a profile
   can't inject must fail loudly, not silently no-op.
2. **Accidental unportability.** The umpire today leans on `traces`/`internals` without anyone
   deciding it, so almost nothing is canary-ready even where it could be.

### Mechanism (proposed)
- **Facts carry the observe-capability they need**, set at the importer/decoder (a property of
  the channel, not the invariant): `registerRequestFact(rpc, …)`, `registerSpanFact(traces, …)`,
  future `registerComponentFact(internals, …)`.
- **Actions carry the drive-capability they need** — `rpcDrive` / `faults` / `directDrive` —
  and the Driver realizer for the environment executes them (a CHASM call, a frontend RPC, a
  remote client call).
- **A run enables only channels its profile grants** — a canary literally cannot produce
  `internals` facts or `faults` actions, so they are never registered (no silent no-ops).
- **Each rule declares its required observe-capabilities** = max of the entity states it reads;
  **each planned route declares its required drive-capabilities**. `InitRules(profile)` and route
  selection run only what the profile supports and emit a **coverage report** (“observe `rpc`: N
  active; `traces`-only: M skipped; `internals`-only: K skipped; routes needing `faults`: J
  skipped”), so we always know what is and isn’t exercised in a given environment.
- **Entities, rules, and routes stay identical across environments** — only the *fact sources*
  and *action realizers* (the edges) change. The profile is a capability filter over one unified
  model, not a fork.

### The reclassification insight (payoff)
Mapping the current rules onto observe-capabilities shows the umpire is needlessly dependent on
`traces`:
- **Update-lifecycle + workflow-completion rules** (progress, dedup, closure, history-ordering,
  continue-as-new) read states that are all reconstructable from **`rpc` alone**
  (`GetWorkflowHistory` + `PollWorkflowExecutionUpdate` + response errors) — yet are currently
  sourced from **server OTEL spans (`traces`)**. Re-sourcing them `rpc` makes the *flagship*
  invariants **runnable in `cicd` and `canary`** *and* removes the production-span coupling that
  is the #1 structural risk.
- **Speculative + task rules** (creation, conversion, rollback, starvation) read
  `IsSpeculative`, task `stored`/`discarded`, matching internals — **intrinsically
  `traces`/`internals`**; they correctly stay in the `local-*` environments.

Target end-state: an **`rpc`-only rule set** (update + workflow lifecycle) that runs everywhere
including `canary`, and a **`traces`/`internals` superset** (speculative, task-internal,
persistence) for the local environments — same model, capability-gated.

### How it reframes fidelity
The fidelity gaps split by capability, which also resolves the settle-vs-detect tension:
- **Close observation** has an `rpc` form (history terminal event / Describe status / update
  "already completed" error) and an `internals` form (persistence write / mutable-state status).
  Prefer the `rpc` form so closure & liveness run in `canary`; use `internals` only to sharpen or
  for internal-only invariants.
- **Persistence / CHASM interception** *is* the `internals` capability — powerful, but tagged so
  no rule depending on it is ever expected to run in `canary`. In `local-chasm` it is also the
  *drive* path (`directDrive`).
- `rpc`-sourced close removed the need for the old teardown-settle hack (`settleWorkflows`,
  since deleted) — real close is now observed from close/terminate facts.

Discipline: **push every rule and action to the widest set of environments its capabilities
allow**, so the maximum number run in the maximum number of environments.

## Gaps

**Resolved by the latest changes**
- ~~Nothing asserts against live traffic~~ → every functional test now checks its namespace.
- ~~Can't `Check` on shared/pooled clusters / unbounded growth~~ → `CheckNamespace` +
  `PurgeNamespace` scope and evict per test.

**Still open**
1. **Close signals cover only `CompleteWorkflowExecution`** — fail/cancel/timeout/terminate/
   continue-as-new emit nothing. Now a correctness *and* false-positive issue (above), and it
   leaves `ContinueAsNew` unable to fire on its real trigger.
   - This also subsumes the apparent "test ended vs. genuinely stuck" worry: the umpire does
     **not** expect all entities to reach terminal — progress is opt-in via `MustProgress`
     (only `WorkflowUpdate = {admitted, accepted}` today; workflows/tasks have none). An
     in-flight update only *looks* stuck at teardown because we don't observe the workflow
     close that aborts it server-side. Fix = observe close→abort, and judge update-progress
     **relative to observed workflow close, not to test-teardown timing** — so an update on a
     still-running workflow is never flagged. No "test intent" mechanism is needed.
2. **Observation-time, not event-time, timestamps** (`time.Now()` ×15).
3. **Dead instrumentation in `cache.go`** — emits via the global tracer (never wired to the
   umpire) and no fact decodes it.
4. **`TestEnv` vs classic coverage** — verify the enforcement fires for the intended tests
   only once (both `TearDownTest` and the `NewEnv` cleanup exist; confirm no double-check or
   missed path).

## Complexity / cleanup

Done in the latest cleanup pass:
- **Dead `Flag` removed** — per-entity `Flag` markers were set on every transition but read by
  no rule (state comes from `Lifecycle.Reached` / `EnteredAt`); `flag.go` and all fields deleted.
- **Dead `Monitor.Check` / `Reset` removed** — superseded by `CheckNamespace` / `PurgeNamespace`.
- **`settleWorkflows` gone** — the teardown-settle hack was deleted; closes now settle via
  observed close/terminate facts (`WorkflowExecutionCompleted` / `WorkflowTerminated` routed to
  entities), not teardown timing.
- **Naming aligned to the docs** — framework `Registry`→`ModelState`, `Rulebook`→`RuleRegistry`;
  active package `driver`→`planner` with the realizer interface `Driver`; the
  speculative rule files renamed to match their types.

Still open:
- **Overlapping rules** (one defect → several violations): `HistoryOrdering` ⊂ `Closure`; the
  admitted-stuck family (`EntityProgress` / `SpeculativeConversion` / `ContinueAsNew` /
  `WorkerSkipped`); `SpeculativeTaskRollback` ⊂ `Closure`. Consolidate to reduce noise.
- **`TaskQueue`** — modelled and fact-decoded but read by no rule; kept only as `WorkflowTask`'s
  structural parent. Either give it a rule (it carries `LastEmptyPollTime`) or re-parent tasks
  and drop it.
- **Dormant `FaultInjector`** — reserved for the Driver's fault actions; intentional, not dead.

## Design note: a shared FSM abstraction?

Would one reusable FSM across entities cut per-rule effort? Not a single shared FSM — the
lifecycles genuinely differ — but a **declarative layer on each entity's FSM plus a few
generic rules** would. Annotate each FSM with metadata (which states are terminal, which
transitions are legal, an optional per-state "must progress within"), then provide generic
rules that consume it:

- generic **legal-transition / monotonic** safety → subsumes `StageMonotone` and catches
  illegal transitions for free (**done**: `EntityTransitionLegality` over `Classify`);
- generic **reach-a-terminal-state** liveness → subsumes `LossPrevention`, `Completion`, and
  the single-entity "stuck" rules;
- generic **state ↔ marker/timestamp consistency** safety → subsumes `StateConsistency`.

That collapses ~4 of the 14 rules (the single-entity, overlap-heavy ones) into generic,
FSM-parameterised rules. The rest are genuinely **cross-entity correlations** (speculative
task ↔ update, update ↔ its workflow's close, worker poll ↔ update) and stay bespoke — that's
where the interesting bugs live. Trade-off: generic rules give less specific messages (carry
the state/metadata in tags to compensate). (STAMP took the opposite route — no FSM, state as
reactive bool markers + per-model `Verify()`; Umpire's explicit FSMs already give
transition-legality and monotonicity, so lean on them. See `UMPIRE_PRIOR_ART.md` (STAMP).)

**Status: built and adopted.** `common/testing/umpire/lifecycle.go` provides `Lifecycle`
(a drop-in superset of the looplab FSM that records per-state entry times, derives terminal
states, tracks a per-entity `MustProgress` set, and captures illegal transitions the old
guarded `if Can(){Event()}` pattern dropped silently), plus `Lifecycled` + `ChangedLifecycles`
for type-erased iteration. **All three entities** (`WorkflowUpdate`, `WorkflowTask`,
`Workflow`) are migrated onto it — looplab now appears only inside `lifecycle.go`.

The FSM is now an **executable transition function** (an oracle), not just a legal-edge
guard. `Lifecycle.Classify(event)` is a pure, three-valued function — `Advance` (a legal
forward edge), `NoOp` (a benign duplicate / late / out-of-order / post-terminal
re-observation), or `Illegal` (impossible given the observed history) — and `Fire` is defined
in terms of it. Modelling the benign re-observations as `NoOp` (rather than lumping every
non-edge into "illegal") is what removed the false-positive vector. `States`/`Events`/
`Reachable`/`Validate` expose the graph for Tier-1 static validation (`tests/umpirev1/model`
checks every default lifecycle is sound and `Classify` is total, server-free in ms — the
analog of the SAA model's `validate` package). This realizes item #1 of `UMPIRE_PRIOR_ART.md` (SAA).

- `EntityProgress` (liveness, generic) is **registered and replaces** `WorkflowUpdateLossPrevention`
  + `WorkflowUpdateCompletion`, which are deleted. Parity is exact: the update declares
  `MustProgress = {admitted, accepted}`, so it fires on exactly the states those two rules
  did and stays silent on `unspecified`. Other entities declare no must-progress states, so
  the rule is a safe no-op for them.
- `EntityTransitionLegality` (safety, generic) **subsumes** the deleted `WorkflowUpdateStageMonotone`
  (a stage regression is simply not a legal edge) and checks every `Lifecycled` type at once.
  `Classify` returning `NoOp` for benign races (duplicate/stale/post-terminal spans) removed the
  original blocker; treating **forward jumps over unobserved states** as legal removed a second
  false-positive class (a `Workflow` seeing `complete` from `created` when `start` was unobserved).
  But that second fix has a consequence: with forward jumps legal, the current converging-DAG
  lifecycles have **zero** possible illegal transitions (0 illegal cells, measured), so the rule
  is **vacuous** and stays **unregistered** — a never-firing rule is false confidence. The
  mechanism is real and unit-tested (a branching lifecycle in `lifecycle_test.go` still produces
  and flags an illegal transition); it needs **event-time ordering** to have teeth over these
  lifecycles (event-time distinguishes a missed observation from a genuine illegal skip). The
  forward-jump handling itself is kept — it is a fidelity win: entities now reach their true
  observed state (e.g. `Workflow` → `completed`) instead of stalling on an unobserved intermediate.

  Note the two consumers diverge here on purpose: the **Monitor** treats a forward jump as a legal
  Advance (observe-only can't tell a missed observation from an illegal skip), while the **Driver**
  planner routes only over **direct edges** (`Lifecycle.Edges()`), never jumps — a plan must drive
  every real step.

`WorkflowUpdateStateConsistency` is **deleted**: the update's `*At` accessors are now derived
from the lifecycle's entry times (`EnteredAt`), so "state reached ⇔ timestamp set" holds by
construction — there is nothing left to check. Remaining: close the close-signal / event-time
fidelity gaps to sharpen the residual cross-branch-reorder case for `EntityTransitionLegality`.

## How rules map onto the model (north star: complete models)

The direction of travel (`UMPIRE_PRIOR_ART.md` (SAA) #1) is a **complete, executable model per entity**: a
total transition function (`Classify`: every state × event → `Advance`/`NoOp`/`Illegal`) plus
the state-derived predictions (terminal, must-progress, timestamps, and eventually the expected
API result per edge). "Complete" is a *means*, not the goal — it buys the SPEC goals (no vacuous
passes, model-derived coverage, cheap bug-finding). The crucial caveat: **a complete model is
per-entity; most of our rules are cross-entity.** So the rulebook does not collapse into one
model — it splits into three buckets.

| Bucket | What it is | Rules | Fate |
|---|---|---|---|
| **Is the model** | single-entity conformance | `EntityTransitionLegality` | *becomes* the generic model-conformance check (built). Not a hand-written rule anymore. |
| **Property of a model** | liveness/structure derived from one FSM's annotations | `EntityProgress`, `WorkflowTaskStarvation`, `WorkflowUpdateDeduplication` (+ already-absorbed `StageMonotone`, `StateConsistency`) | collapse into model annotations (terminal / must-progress / state-derived), checked generically. |
| **Relation between models** | invariant over two-or-more entities | `SpeculativeTaskCreation`, `WorkflowUpdateHistoryOrdering`, `WorkflowUpdateClosure`, `WorkflowUpdateContinueAsNew`, `WorkflowUpdateWorkerSkipped`, `WorkflowUpdateContextClear`, `SpeculativeTaskRollback`, `SpeculativeConversion` | **not subsumed.** Stay bespoke — this is where the interesting bugs live. |

A per-entity complete model absorbs ~4–5 of the 12 registered rules; the other ~7 remain rules.
That is not a shortfall: even the SAA model is single-archetype and has **no** cross-entity
story (`UMPIRE_PRIOR_ART.md` (SAA), "where Umpire's approach is genuinely different"). Cross-entity correlation
is Umpire's differentiator, not a gap in it.

### The boundary is movable
A cross-entity rule can be pulled **into** a single entity's complete model by adding the other
entity's transitions as **events in that entity's alphabet** — exactly how SAA folds
timeouts/backoff into the activity model. Feed the update model an `owning_workflow_closed`
event and `Closure` / `ContinueAsNew` / `HistoryOrdering` stop being relational rules and become
ordinary transition / must-progress properties of the (now richer) update model. This is the
same move as broadening close signals in the fidelity work — it does double duty. What stays
**irreducibly** relational is invariants over a *set* of peer entities: `SpeculativeTaskCreation`
("at most one pending normal task alongside a speculative one for a workflow") is about the
collection, not any one entity's history, and no single-entity model can state it.

### Why the goal is "models *plus* relational invariants," not one monolith
Replacing rules with a single global model would forfeit Umpire's three edges: cross-entity
reach, portability (capabilities & environments, canary — see that section), and ride-along
enforcement over the whole suite. A complete *global* product automaton (TLA+-style) is a much
larger lift and out of scope. The practical target:
1. **Per-entity complete models** — total `Classify` + state predictions (+ expected API result)
   → generic conformance + generic liveness. Vacuous passes gone for single-entity behavior.
2. **Enrich event alphabets** with cross-entity facts (workflow-close first) to fold as many
   correlations as possible into single models.
3. **Keep the residual set-level relational invariants** as explicit rules — sharper now, since
   they read validated model state and their preconditions are model-derived (so a dead/vacuous
   rule becomes detectable rather than silently passing).

## Gate run (priority 0) — first result

**Update:** the settle-vs-detect design discussed below has since been resolved — `settleWorkflows`
was removed and closes now settle via observed close/terminate facts (`workflow.go`). The finding
below is kept as history; re-run the gate to confirm current detection.

Ran the umpire's own functional suite (`tests/lost_task_test.go`, `FunctionalTestBase` +
enforcement) under a live in-process cluster. **All three tests fail — the umpire detects
0 violations where it expects one.** Root cause (not a regression from the consolidation;
these commits don't touch that test, and the rule's unit tests pass):

- **`settleWorkflows` masks `WorkflowTaskStarvation`.** `Check(final)` runs `settleWorkflows`
  *before* the rules; it broadcasts `WorkflowTerminated` to every task, so a stuck task
  transitions `stored`/`added → terminated`, and `WorkflowTaskStarvation`'s `default` branch
  then `Resolve`s it. No rule reads the `terminated`/`discarded` task state or
  `WorkflowTerminated`, so this settle serves no rule — it only suppresses detection.
- The **tension**: settle appears intended to suppress false positives for tasks legitimately
  in-flight at teardown, but it over-suppresses and defeats true detection. Since the only
  producer of a task `terminated` state is settle itself, `current == terminated && never
  polled` is in fact an exact "was stuck at teardown" signal — a candidate fix is to flag
  that in `WorkflowTaskStarvation` (using `Lifecycle.Reached("polled")`). But enabling it
  will fire for *any* never-polled task across the enforced suite, so it needs the fidelity
  work (real close/terminal signals) to distinguish "test ended in-flight" from "stuck".
- `TestStuckWorkflowDetectionWithSDK` expects detection of a workflow stuck in `Await` (its
  tasks *were* polled) — that's **workflow-completion liveness**, not task starvation, and
  needs `Workflow.MustProgress = {started}`, which is deliberately off pending close signals.

This is the priority-0 triage in action: the blocker is the settle-vs-detect design and the
missing close-signal fidelity, not the rules themselves.

## Plan (priority order)

0. **Resolve settle-vs-detect, then re-run the gate.** Decide how teardown settling should
   interact with task/workflow liveness (the finding above), fix `WorkflowTaskStarvation`
   accordingly, and confirm the umpire's own suite goes green — before trusting enforcement
   across the broad functional suite.
1. **Close the false-positive vectors** surfaced by (0), most likely:
   - broaden close signals to all close types (ideally one emit at the mutable-state
     status→closed transition) so updates on closed-but-not-"completed" workflows settle;
   - carry event-time in facts for order-sound timestamp rules;
   - make `settleWorkflows` (or its replacement) settle workflows/updates, not just tasks.
2. **Demonstrate terseness (goal 2).** Take one real update test, delete its hand-written
   invariant assertions, and rely on the umpire — the first concrete proof of the payoff.
3. **Consolidate overlapping rules** so a single defect yields one clear violation.
4. **Housekeeping.** Remove dead `cache.go` instrumentation and unused symbols; confirm the
   teardown wiring covers each test exactly once.

## Coverpoints & coverage (planned)

A **coverpoint** is a named, documented *situation of interest* plus a predicate that
recognizes it in the observed model. It is a third subsystem alongside facts→entities→rules,
with inverted semantics from a rule:

| | fires on | absence means | lifetime |
|---|---|---|---|
| Rule | model in a *bad* state | (presence = bug) | per-namespace, purged |
| One-off assert | a specific expected value | test fails locally | per-test |
| **Coverpoint** | model reaching an *interesting* state | **coverage gap** (missing test or dead behaviour), not a bug | **process-global, survives purge** |

A rule says "if this situation happens it must resolve"; a coverpoint says "this situation
should happen to *someone* during the run." This directly serves two goals the rules can't:
*tests as living docs* (a named catalog of what the system can do) and a *foundation for
fuzzing* (detection is the reward signal a future generator optimizes toward).

**Highest-value use — prove the rules aren't dead.** A rule's *precondition* is a coverpoint
worth covering; a rule whose precondition is never reached passes vacuously and gives false
confidence. Seeding the catalog from each registered rule's trigger turns "are our rules even
exercised?" into a mechanical report — which is exactly the validation this doc keeps flagging
as the whole game (`ContinueAsNew` almost certainly never fires today, since its CAN close
signal is missing).

### Model

```
Coverpoint     = { Name, Doc, MinHits, Detect(*CoverpointContext) }
CoverpointRegistry — registers coverpoints (mirrors RuleRegistry; name-validated)
Coverage     — process-global sink: name → set{occurrenceKey}; hits = |set|.
               thread-safe, NOT cleared by PurgeNamespace, mergeable across shards.
```

Marked seen two ways, both feeding `Coverage`:
- **Detected** — `Detect` runs over the scoped model (via `ChangedEntities`/`QueryAll`,
  mostly `Lifecycle.Reached(state)` / `EnteredAt`) and calls `c.Reached(key)`. Objective.
- **Declared** — a test calls `env.Umpire().Reached(name)`; cross-checked against the
  predicate when one exists (declared-but-never-detected ⇒ fail: the test's model is wrong).

Occurrence key (usually the entity registry key) dedups so one long-lived entity counts once.

### Exhaustiveness — which kind

This buys **catalog coverage** (hand-curated equivalence classes, QuickCheck-`cover` style),
*not* true state-space exhaustiveness (that needs the active/generator side). Catalog coverage
is the pragmatic target now and the down payment on generation later.

### Phases

0. **Framework core** (`common/testing/umpire`): `coverpoint.go` (`Coverpoint`, `CoverpointRegistry`,
   `CoverpointContext` reusing the dirty-query plumbing + scope) and `coverage.go` (`Coverage`:
   `Reached`, `Hits`, `Unmet`, dedup, mutex). Unit-tested in isolation.
1. **Wire into the Umpire.** `Umpire` owns a `CoverpointRegistry` + shared `Coverage`;
   `CheckNamespace`/`Check` run detection over the scoped model *before* purge into the
   *unpurged* `Coverage`. `PurgeNamespace` leaves coverage intact (assert it). Add
   `Umpire.Reached` + `Coverage()`.
2. **Seed the catalog from rule preconditions** (the payoff): one coverpoint per registered
   rule trigger (`update.admitted/accepted/completed/rejected`, `update.aborted_on_close`,
   `update.accept_complete_same_wft`, `speculative.rolled_back`/`converted`,
   `workflow.completed_with_pending_update`, `task.sync_match`/`async_match`). Most reduce to
   `EnteredAt`/`Reached` at teardown — no transition hooks needed. Output: which rule
   preconditions were never reached ⇒ dead-rule report.
3. **Completeness gate + reporting.** `Coverage.WriteReport(path)` (JSON) + merge helper; a
   `TestMain` gate behind an env flag (e.g. `UMPIRE_COVERAGE=1`) that fails on any
   coverpoint `< MinHits` **only in a full run** (never under `-run`); per-shard reports merged
   in a final CI step (exhaustiveness is whole-suite + cross-shard).
4. **Declaration verification + living docs.** Cross-check declared vs detected; generate the
   catalog → markdown (name, doc, hits, covering tests).
5. **(future, out of scope) Generation seam.** Expose `Coverage.Unmet()` as targets for the
   active side; don't build it.

### Decisions to lock before Phase 0

1. Exhaustiveness = catalog coverage (recommended), not state-space.
2. Gate granularity: start "≥1 hit = alive" (dead-rule detection); defer statistical
   `MinHits > 1` until generation exists.
3. CI aggregation: confirm a per-shard-report + merge step is acceptable (required for a real
   suite-wide gate; without it the gate only works in a single-process full run).
4. Naming: `CoverpointRegistry`/`Coverage` (literal, consistent with `RuleRegistry`/`FactLog`) vs a
   metaphor. Leaning literal.
5. Require every rule to carry a paired precondition-coverpoint, enforced at registration
   (a lint against vacuous rules)? Leaning yes.

### Guardrails

- Counters live at a different layer than entities/facts — must survive `PurgeNamespace`.
- Gate only in full CI runs; never break filtered/local runs.
- Predicate fidelity depends on emit-site completeness, same as rules — an unemitted signal
  makes a coverpoint silently uncoverable (the close-signal gap bites here too).
- Curate meaningful combinations; don't auto-cross-product labels.

**First cut:** Phases 0–2 alone deliver the dead-rule report and are shippable independently
of the CI gate (Phase 3).

## Rule inventory (11 registered + 1 built-unregistered)

Naming: struct drops the `Rule` suffix; `Name()` returns struct name + `"Rule"` (enforced at
registration).

### Safety — asserted at every observation

| Rule | Invariant | Notes |
|---|---|---|
| `SpeculativeTaskCreation` | a speculative task must not coexist with a pending normal task for the same workflow | groups by `workflowID:runID` |
| `WorkflowUpdateHistoryOrdering` | update not in `accepted` after workflow completed | subset of `Closure` |
| `WorkflowUpdateClosure` | no update accepted/completed after workflow `CompletedAt` | needs live workflow completion |

### Liveness — must eventually hold; unresolved at teardown ⇒ violation

| Rule | Invariant | Notes |
|---|---|---|
| `EntityProgress` (generic) | a lifecycle entity must leave its `MustProgress` states | replaces LossPrevention (admitted) + Completion (accepted) for `WorkflowUpdate` |
| `WorkflowTaskStarvation` | a non-speculative task added/stored is eventually polled | task-loss / worker starvation |
| `SpeculativeTaskRollback` | an update accepted on a polled speculative task eventually completes | specialization of the progress invariant |
| `SpeculativeConversion` | update not left `admitted` after its speculative task converts to normal | admitted-stuck family |
| `WorkflowUpdateDeduplication` | a deduplicated update (`RequestCount>1`) doesn't stall non-terminal | |
| `WorkflowUpdateContinueAsNew` | update not left `admitted` on a completed workflow (should retry on new run) | needs CAN close signal (missing) |
| `WorkflowUpdateWorkerSkipped` | update not left `admitted` after a task was polled post-admit | admitted-stuck family |
| `WorkflowUpdateContextClear` | non-terminal update not stranded with no pending task (workflow not completed) | defers completed case to `Closure` |

### Built but not registered

| Rule | Invariant | Why gated |
|---|---|---|
| `EntityTransitionLegality` (generic safety) | no `Lifecycled` entity observes an illegal transition | over `Lifecycle.Classify`; subsumes `WorkflowUpdateStageMonotone`. Unregistered: a suite run flagged a forward jump over an unobserved state (`Workflow` `complete` from `created`) as illegal. Register once `Classify` treats forward-reachable jumps as legal, or event-time lands. |

Removed by consolidation: `WorkflowUpdateLossPrevention`, `WorkflowUpdateCompletion` (→ `EntityProgress`),
`WorkflowUpdateStageMonotone` (→ `EntityTransitionLegality`), `WorkflowUpdateStateConsistency`
(now structural via derived `*At` accessors).

## Done recently

`Lifecycle` upgraded to an **executable transition function**: pure three-valued
`Classify` (`Advance`/`NoOp`/`Illegal`) with `Fire` defined over it, plus
`States`/`Events`/`Reachable`/`Validate` and server-free Tier-1 static validation of every
default lifecycle (`UMPIRE_PRIOR_ART.md` (SAA) #1). Generic `EntityTransitionLegality` built
(deleting `WorkflowUpdateStageMonotone`, which it subsumes) — benign duplicate/late/post-terminal
spans now classify as `NoOp`. It was briefly registered, then **unregistered** after a functional
suite run flagged a forward jump over an unobserved state as illegal (see the built-but-unregistered
note above); it stays available pending a `Classify` forward-jump fix.
Reusable `Lifecycle` FSM primitive (entry timestamps, derived terminals, `MustProgress`,
illegal-transition capture) adopted by all entities; generic `EntityProgress` liveness rule
registered, replacing `LossPrevention` + `Completion`; `WorkflowUpdateStateConsistency`
deleted (now structural — `*At` accessors derived from lifecycle entry times).
Namespace-scoped `CheckNamespace`/`PurgeNamespace` with per-test teardown enforcement across
the functional suite (facts now namespace-rooted via `EntityPath.Ancestors`); live update-
lifecycle decoding; live workflow completion (`WorkflowExecutionCompleted`); duplicate dedup
rule collapsed; dead scaffolding removed; umpire default-on for all functional clusters.
