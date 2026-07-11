# Umpire — Status & Plan

Current state, a critical read against the goals, gap analysis, and rule inventory. For the
*why* read [`UMPIRE_SPEC.md`](./UMPIRE_SPEC.md); for *how it fits together* read
[`UMPIRE_ABOUT.md`](./UMPIRE_ABOUT.md).

## Snapshot

The observe → model → judge pipeline is built and green at the unit level, and every
observable is wired from a live server emit site through to a rule:

- **Framework** (`common/testing/umpire/`, ~1.7k LoC): registry + generation dirty-tracking,
  safety/liveness rulebook, fact log, gRPC interceptor, OTEL span processor. Unit-tested.
- **Domain** (`tests/umpire/`): 4 entities, 14 facts, 14 rules (5 safety, 9 liveness), each
  fully implemented with a positive + negative unit test. All pass.
- **All 14 facts decode from live traffic** — none test-only. 4 from gRPC requests, 1
  request+response, 10 from OTEL span events emitted by the server.
- **Umpire is enabled by default** on every functional-test cluster (observes everything).

The gap is no longer plumbing. It's whether the thing actually delivers its goals — and
so far it hasn't demonstrated any of them.

## Are we meeting the goals? (critical)

Measured against `UMPIRE_SPEC.md`, the **architecture** faithfully implements the spec's
design decisions, but on the **goals** the project has built the capability and proven none
of it. Honest scorecard:

| Goal | Verdict | Why |
|---|---|---|
| Separate actions from assertions | ✅ structure, ❌ payoff | Clean observe-only split, but the point of it — reuse across functional / nightly / canary — is blocked: the umpire is one global, non-namespace-scoped registry and can't even run across the current suite. |
| Terse tests | ❌ and capped | No test is terser; the update suite kept every assertion. Rules express *invariants*; most functional assertions are *specific expected values* that can never become reusable rules — so the terseness ceiling is a minority of assertions. |
| Tests as living docs | ⚠️ partial | The rulebook is a readable catalog of ~14 properties, but rule overlap muddies it and reading a *test* is unchanged. |
| Find bugs earlier, cheap, fuzzing base | ❌ unproven, risky | No bug found (nothing asserts in the suite). "Cheap" holds except the registry never evicts (unbounded on shared clusters). Turning it on broadly today risks being net-negative (below). |

### Structural risks that threaten the goals (not just "not done yet")

1. **False positives could make it cost time, not save it.** The model is a lossy
   reconstruction: observation-time timestamps (`time.Now()`, not event time), an inferred
   task FSM, workflow completion observed for only one close path. The `settleWorkflows`
   teardown hack — synthesizing terminal facts to stop liveness rules firing — is a tell:
   the tool can't faithfully observe termination, so it fakes it. That same suppression
   also hides real bugs. False-positive *and* blind-spot risk attacks the bug-finding goal
   from both sides.
2. **Hidden production-instrumentation coupling contradicts the spec's spirit.** The spec
   sells "observe the wire/spans, no instrumentation burden," but the wire was insufficient
   (the update lifecycle isn't on any interceptable gRPC call), so emit sites were hand-
   placed inside production hot paths (history update state machine, WFT-completed handler,
   matching). Every new internal invariant likely needs another production emit. That is an
   invasive, unbounded maintenance coupling the spec does not acknowledge.
3. **Model fidelity is the ceiling on everything.** Every rule verdict is only as
   trustworthy as the entity reconstruction, which today is partial (one close path,
   approximate ordering, TaskQueue barely modeled). Sophisticated rules over an unsound
   model produce untrustworthy verdicts.

### The honest next milestone

Not "more rules." **One end-to-end proof:** take a real update test, delete its hand-written
invariant assertions, rely on the umpire, and show it (a) stays green on correct behaviour
and (b) catches a regression you deliberately inject — with no false positives. If that
can't be made to work, the approach needs rethinking before scaling. If it can, most risks
above become tractable. Everything in the plan below is subordinate to this.

## Gaps — missing / not working

1. **Nothing asserts against live traffic except one test.** `lost_task_test.go` is the
   only functional test that calls `Check`. The `update_workflow` suite is observed but
   never checked — 14 fully-wired rules with ~zero live validation.
2. **The umpire can't safely `Check` on shared/pooled clusters.** One global registry, never
   evicts entities, teardown-scoped liveness shared across concurrent tests. Safe validation
   needs **namespace-scoping** (the entity hierarchy already has a `Namespace` root to key on).
3. **Workflow completion observed for only one close path** (`CompleteWorkflowExecution`).
   Fail/cancel/timeout/terminate/continue-as-new emit nothing, so three rules gated on
   `completed`+`CompletedAt` (`Closure`, `HistoryOrdering`, `ContinueAsNew`) silently no-op
   for those closes — including `ContinueAsNew`, whose CAN signal is never emitted.
4. **Timestamps are observation-time, not event-time** (`time.Now()` ×15). Timestamp-
   comparison rules rely on span arrival order, only approximately the real order.
5. **Dead instrumentation in `cache.go`** — emits `workflow.cache.lock.*` via the global
   tracer (never wired to the umpire) and no fact decodes it. Doubly-dead.

## Complexity / cleanup

- **Overlapping rules:** `HistoryOrdering` ⊂ `Closure` (merge); `Completion` (accepted-stuck)
  ≈ `LossPrevention` (admitted-stuck) — parameterize into one stuck-state rule; the
  admitted-stuck family (`LossPrevention`, `SpeculativeConversion`, `ContinueAsNew`,
  `WorkerSkipped`) over-triggers because `LossPrevention` is unconditional; `SpeculativeTaskRollback`
  ⊂ `Completion`.
- `WorkflowUpdateStateConsistency` (92 LoC, 5 branches) — could be table-driven.
- `settleWorkflows` teardown hack (see risk #1) — masks missing real terminal signals.
- Cosmetic, already-drifted `subscribesTo` lists on `RegisterEntity` (routing ignores them).
- Dead/thin surface: `TaskQueue` has no FSM (only empty-poll tracking); `Namespace`/
  `NamespaceType`/`ExecutionID`/`WorkflowID.Execution|Task|Update` unused; dormant `FaultInjector`.

## Plan (priority order)

0. **Prove one test end-to-end** (the milestone above): delete a real update test's invariant
   assertions, rely on the umpire, inject a regression, confirm green→red with no false
   positives. Gate further investment on this.
1. **Harden model fidelity before adding rules.** Event-time timestamps carried in facts;
   remove the `settleWorkflows` hack in favour of real terminal signals; decide TaskQueue's
   fate. Untrustworthy model ⇒ untrustworthy rules.
2. **Namespace-scope the umpire.** Partition entities by namespace; scope `Check` +
   settlement per test namespace. The real unlock for validating the whole suite safely on
   shared clusters — and the prerequisite for the spec's "reuse across contexts" goal.
3. **Broaden close signals.** Emit a "workflow closed" event for all close types (ideally one
   emit at the mutable-state status→closed transition). Unblocks the three completed-gated
   rules and `ContinueAsNew`.
4. **Confront the instrumentation cost in the spec.** Either own the production-emit burden
   explicitly (a documented, reviewed set of umpire span sites) or accept that pure wire
   observation can't see enough — don't leave it implicit.
5. **Consolidate overlapping rules** (Closure ∪ HistoryOrdering; Completion ∪ LossPrevention;
   clarify the admitted-stuck family).
6. **Housekeeping.** Remove dead `cache.go` instrumentation and unused
   `NamespaceType`/`ExecutionID`/ID-helpers; resolve the cosmetic `subscribesTo` lists.

## Rule inventory (14)

Naming: struct drops the `Rule` suffix; `Name()` returns struct name + `"Rule"` (enforced at
registration).

### Safety — asserted at every observation

| Rule | Invariant | Notes |
|---|---|---|
| `SpeculativeTaskCreation` | a speculative task must not coexist with a pending normal task for the same workflow | groups by `workflowID:runID` |
| `WorkflowUpdateStateConsistency` | FSM state agrees with which of Accepted/Completed/Rejected timestamps are set | largest rule (5 checks) |
| `WorkflowUpdateHistoryOrdering` | update not in `accepted` after workflow completed | subset of `Closure` |
| `WorkflowUpdateClosure` | no update accepted/completed after workflow `CompletedAt` | needs live workflow completion |
| `WorkflowUpdateStageMonotone` | update stage never regresses | only stateful rule (`highWater`) |

### Liveness — must eventually hold; unresolved at teardown ⇒ violation

| Rule | Invariant | Notes |
|---|---|---|
| `WorkflowTaskStarvation` | a non-speculative task added/stored is eventually polled | task-loss / worker starvation |
| `SpeculativeTaskRollback` | an update accepted on a polled speculative task eventually completes | specialization of `Completion` |
| `SpeculativeConversion` | update not left `admitted` after its speculative task converts to normal | admitted-stuck family |
| `WorkflowUpdateLossPrevention` | update in `admitted` is eventually accepted | fires unconditionally on `admitted` |
| `WorkflowUpdateCompletion` | update in `accepted` eventually completes | mirror of LossPrevention |
| `WorkflowUpdateDeduplication` | a deduplicated update (`RequestCount>1`) doesn't stall non-terminal | |
| `WorkflowUpdateContinueAsNew` | update not left `admitted` on a completed workflow (should retry on new run) | needs CAN close signal (missing) |
| `WorkflowUpdateWorkerSkipped` | update not left `admitted` after a task was polled post-admit | admitted-stuck family |
| `WorkflowUpdateContextClear` | non-terminal update not stranded with no pending task (workflow not completed) | defers completed case to `Closure` |

## Done recently

Live update-lifecycle decoding (admitted/accepted/completed/rejected emitted from the history
update state machine → `WorkflowUpdate` FSM); live workflow completion
(`WorkflowExecutionCompleted` → `Workflow` FSM); duplicate dedup rule collapsed; `ActivityTask`
and other dead scaffolding removed; umpire made default-on for all functional clusters.
