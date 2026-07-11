# Umpire — Status & Plan

Current state, a critical read against the goals, gap analysis, and rule inventory. For the
*why* read [`UMPIRE_SPEC.md`](./UMPIRE_SPEC.md); for *how it fits together* read
[`UMPIRE_ABOUT.md`](./UMPIRE_ABOUT.md).

## Snapshot

The pipeline is built and, as of the latest changes, **enforced suite-wide**:

- **Framework** (`common/testing/umpire/`): registry + generation dirty-tracking,
  safety/liveness rulebook, fact log, gRPC interceptor, OTEL span processor. Unit-tested.
- **Domain** (`tests/umpire/`): 4 entities, 14 facts, 12 registered rules (4 safety,
  8 liveness) + 1 generic rule built-but-unregistered, each with a positive + negative test.
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
  fails for a non-bug. `settleWorkflows` only settles *tasks*, not workflows/updates.
- **Observation-time timestamps.** Entity `…At` fields are `time.Now()` at fact processing,
  not event time; timestamp-comparison rules (`Closure`) rely on span arrival order.
- **Model fidelity generally.** Any behaviour the model reconstructs imperfectly can trip a
  rule on correct behaviour.

Until the suite has been run under enforcement and every violation triaged, this change is
not safe to merge on. **That triage is now the whole game.**

## Gaps

**Resolved by the latest changes**
- ~~Nothing asserts against live traffic~~ → every functional test now checks its namespace.
- ~~Can't `Check` on shared/pooled clusters / unbounded growth~~ → `CheckNamespace` +
  `PurgeNamespace` scope and evict per test.

**Still open**
1. **Close signals cover only `CompleteWorkflowExecution`** — fail/cancel/timeout/terminate/
   continue-as-new emit nothing. Now a correctness *and* false-positive issue (above), and it
   leaves `ContinueAsNew` unable to fire on its real trigger.
2. **Observation-time, not event-time, timestamps** (`time.Now()` ×15).
3. **Dead instrumentation in `cache.go`** — emits via the global tracer (never wired to the
   umpire) and no fact decodes it.
4. **`TestEnv` vs classic coverage** — verify the enforcement fires for the intended tests
   only once (both `TearDownTest` and the `NewEnv` cleanup exist; confirm no double-check or
   missed path).

## Complexity / cleanup

- **Overlapping rules** now mean one real defect fails a test with several violations (noisy,
  not wrong): `HistoryOrdering` ⊂ `Closure`; `Completion` ≈ `LossPrevention`; the admitted-
  stuck family (`LossPrevention`/`SpeculativeConversion`/`ContinueAsNew`/`WorkerSkipped`);
  `SpeculativeTaskRollback` ⊂ `Completion`. Consolidate to reduce noise.
- `WorkflowUpdateStateConsistency` (92 LoC, 5 branches) — could be table-driven.
- `settleWorkflows` teardown hack — still papers over missing real terminal signals, and now
  its incompleteness (tasks only) is a false-positive source.
- Dead/thin surface: `TaskQueue` (no FSM), dormant `FaultInjector`, leftover ID helpers.

## Design note: a shared FSM abstraction?

Would one reusable FSM across entities cut per-rule effort? Not a single shared FSM — the
lifecycles genuinely differ — but a **declarative layer on each entity's FSM plus a few
generic rules** would. Annotate each FSM with metadata (which states are terminal, which
transitions are legal, an optional per-state "must progress within"), then provide generic
rules that consume it:

- generic **legal-transition / monotonic** safety → subsumes `StageMonotone` and would catch
  illegal transitions for free;
- generic **reach-a-terminal-state** liveness → subsumes `LossPrevention`, `Completion`, and
  the single-entity "stuck" rules;
- generic **state ↔ marker/timestamp consistency** safety → subsumes `StateConsistency`.

That collapses ~4 of the 14 rules (the single-entity, overlap-heavy ones) into generic,
FSM-parameterised rules. The rest are genuinely **cross-entity correlations** (speculative
task ↔ update, update ↔ its workflow's close, worker poll ↔ update) and stay bespoke — that's
where the interesting bugs live. Trade-off: generic rules give less specific messages (carry
the state/metadata in tags to compensate). (STAMP took the opposite route — no FSM, state as
reactive bool markers + per-model `Verify()`; Umpire's explicit FSMs already give
transition-legality and monotonicity, so lean on them. See `STAMP_IMPORT.md`.)

**Status: built and adopted.** `common/testing/umpire/lifecycle.go` provides `Lifecycle`
(a drop-in superset of the looplab FSM that records per-state entry times, derives terminal
states, tracks a per-entity `MustProgress` set, and captures illegal transitions the old
guarded `if Can(){Event()}` pattern dropped silently), plus `Lifecycled` + `ChangedLifecycles`
for type-erased iteration. **All three entities** (`WorkflowUpdate`, `WorkflowTask`,
`Workflow`) are migrated onto it — looplab now appears only inside `lifecycle.go`.

- `EntityProgress` (liveness, generic) is **registered and replaces** `WorkflowUpdateLossPrevention`
  + `WorkflowUpdateCompletion`, which are deleted. Parity is exact: the update declares
  `MustProgress = {admitted, accepted}`, so it fires on exactly the states those two rules
  did and stays silent on `unspecified`. Other entities declare no must-progress states, so
  the rule is a safe no-op for them.
- `EntityTransitionLegality` (safety, generic) is **built and unit-tested but not registered**:
  "illegal transition" over-captures benign races (e.g. a duplicate accepted span, or a
  best-effort settle), so it would false-positive under enforcement. `WorkflowUpdateStageMonotone`
  stays registered until event-time ordering makes illegal transitions unambiguous.

`WorkflowUpdateStateConsistency` is **deleted**: the update's `*At` accessors are now derived
from the lifecycle's entry times (`EnteredAt`), so "state reached ⇔ timestamp set" holds by
construction — there is nothing left to check. Remaining: close the close-signal / event-time
fidelity gaps, then register `EntityTransitionLegality` and retire `StageMonotone`.

## Plan (priority order)

0. **Run the full functional suite under enforcement and triage every violation.** This is
   the gate. Each violation is either a real bug (great) or a false positive (fix the model,
   not the rule). Nothing else should merge on top of enforcement until this is green or
   every remaining violation is confirmed real.
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

## Rule inventory (12 registered + 1 built-unregistered)

Naming: struct drops the `Rule` suffix; `Name()` returns struct name + `"Rule"` (enforced at
registration).

### Safety — asserted at every observation

| Rule | Invariant | Notes |
|---|---|---|
| `SpeculativeTaskCreation` | a speculative task must not coexist with a pending normal task for the same workflow | groups by `workflowID:runID` |
| `WorkflowUpdateHistoryOrdering` | update not in `accepted` after workflow completed | subset of `Closure` |
| `WorkflowUpdateClosure` | no update accepted/completed after workflow `CompletedAt` | needs live workflow completion |
| `WorkflowUpdateStageMonotone` | update stage never regresses | stateful (`highWater`); superseded by `EntityTransitionLegality` once ordering is sound |

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
| `EntityTransitionLegality` (generic safety) | no lifecycle entity observes an illegal transition | over-captures benign races (duplicate spans) until event-time ordering lands |

Removed by consolidation: `WorkflowUpdateLossPrevention`, `WorkflowUpdateCompletion` (→ `EntityProgress`),
`WorkflowUpdateStateConsistency` (now structural via derived `*At` accessors).

## Done recently

Reusable `Lifecycle` FSM primitive (entry timestamps, derived terminals, `MustProgress`,
illegal-transition capture) adopted by all entities; generic `EntityProgress` liveness rule
registered, replacing `LossPrevention` + `Completion`; `WorkflowUpdateStateConsistency`
deleted (now structural — `*At` accessors derived from lifecycle entry times); generic
`EntityTransitionLegality` built (unregistered).
Namespace-scoped `CheckNamespace`/`PurgeNamespace` with per-test teardown enforcement across
the functional suite (facts now namespace-rooted via `EntityPath.Ancestors`); live update-
lifecycle decoding; live workflow completion (`WorkflowExecutionCompleted`); duplicate dedup
rule collapsed; dead scaffolding removed; umpire default-on for all functional clusters.
