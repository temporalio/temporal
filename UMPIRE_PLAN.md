# Umpire

Property-based test monitoring for Temporal. Umpire observes a running server via
gRPC interceptors and OTEL spans, routes what it sees to entity state machines, and
runs invariant rules that flag violations — without the rules knowing anything about
change tracking or wire formats.

## Layout

Two packages. The **framework** is generic; the **domain** is Temporal-specific.

```
common/testing/umpire/          # framework (generic, reusable)
├── entity.go                   # Fact / BroadcastFact / Entity / Identity / EntityID
├── registry.go                 # entity store, fact routing, generation-based dirty tracking
├── rulebook.go                 # SafetyRule / LivenessRule, contexts, Pending/Resolve, Check
├── fact_log.go                 # queryable log of all observed facts
├── interceptor.go              # gRPC unary interceptor (FactRecorder + FaultInjector)
├── instrument.go               # OTEL span helpers (Instrument / RecordFact / RecordError)
└── flag.go                     # named boolean observable on an entity

tests/umpire/                   # Temporal domain
├── umpire.go                   # orchestrator: SpanProcessor + gRPC recorder, registers all rules
├── entity_key.go               # entity-key path builder (Workflow(id).Update(id) / .Task(...))
├── entity/                     # Workflow, WorkflowTask, WorkflowUpdate, TaskQueue FSMs + decoder
├── fact/                       # fact types (request-, response-, span-, history-event-derived)
└── rulebook/                   # 15 rules (5 safety, 10 liveness) + unit tests
```

## How it works

1. `functional_test_base.go` builds the `Umpire`, installs its gRPC interceptor, and
   registers it as an OTEL `SpanProcessor` on the test cluster.
2. gRPC requests/responses and OTEL span events are decoded into **facts** (`FactDecoder`).
3. `Registry.RouteFacts` delivers each fact to the target **entity**, which advances its
   FSM. Each delivery bumps a global **generation** counter on the entity.
4. **Rules** query `ChangedEntities[T]` — only entities touched since the rule's last
   check (dirty tracking). Safety rules assert immediately (`Eval`/`Pass`); liveness
   rules mark conditions `Pending`/`Resolve`, and unresolved ones become violations at
   teardown (`Check(ctx, final=true)`).

## Status

### Gaps
- **Decoder is the critical gap.** The four update-lifecycle facts
  (`WorkflowUpdateAdmitted/Accepted/Completed/Rejected`) have no importer — no
  `ImportRequest`, no `ImportSpanEvent`. They are only constructed by hand in tests.
  In a live run the `WorkflowUpdate` FSM can't advance past `admitted`, so the 10 update
  rules effectively can't fire against real traffic. This is the main thing keeping the
  tool test-only.
- `WorkflowTaskCompleted` never sets an `Identity` and `Workflow` isn't registered for it,
  so the workflow `→completed` transition never fires live (tests only).
- Only one production instrumentation site exists (workflow cache lock).

### Cleanup / redundancy
- `WorkflowUpdateDeduplicationRule` and `WorkflowUpdateDedupProgressRule` are near-duplicates
  (same iteration, `RequestCount > 1` gate, terminal-state resolution) — differ only in
  message. Collapse into one.
- The "stuck in `admitted`" family overlaps: `WorkflowUpdateLossPreventionRule` fires on any
  `admitted` update unconditionally, double-reporting cases already caught by
  `ContinueAsNew`, `WorkerSkipped`, `SpeculativeConversion`, and `ContextClear`. Decide
  catch-all vs. mutually exclusive.
- `WorkflowUpdateClosureRule` (timestamp) and `WorkflowUpdateHistoryOrderingRule` (FSM state)
  both flag "update accepted after workflow close" — both fire on the same real case.
- Naming drift: `workflow_task_stale_token.go` defines `WorkflowUpdateContextClearRule`.
- Leftover scaffolding: `WorkflowExecutionType` constant, `ExecutionID` type — unused.

## Plan (priority order)

1. **Wire the update-lifecycle decoder.** Implement `ImportSpanEvent` (history events via
   the existing `SpanFact` path is cleanest) for `WorkflowUpdateAdmitted/Accepted/
   Completed/Rejected`. Turns 10 rules from test-only into live. Highest leverage.
2. **Fix `WorkflowTaskCompleted`** identity + registration so the Workflow FSM completes live.
3. **Consolidate rules:** collapse the two dedup rules; resolve the Loss-vs-specific
   overlap; reconcile Closure vs. HistoryOrdering.
4. **Housekeeping:** rename `workflow_task_stale_token.go` to match its type; drop unused
   `WorkflowExecutionType` / `ExecutionID`.

## Rule inventory

Safety (immediate assertions):
- `SpeculativeTaskCreationRule` — speculative task exists while a normal pending task exists for same (workflowID, runID).
- `WorkflowUpdateStateConsistencyRule` — FSM state agrees with which timestamp fields are set.
- `WorkflowUpdateHistoryOrderingRule` — update in `accepted` when workflow already completed.
- `WorkflowUpdateClosureRule` — update accept/complete timestamp after workflow close.
- `WorkflowUpdateStageMonotoneRule` — update FSM stage never regresses (only stateful rule).

Liveness (must eventually hold; unresolved → violation at teardown):
- `WorkflowTaskStarvationRule` — non-speculative task added/stored but never polled.
- `SpeculativeTaskRollbackRule` — update accepted on a polled speculative task, never completed.
- `SpeculativeConversionRule` — update stuck `admitted` after speculative→normal conversion.
- `WorkflowUpdateLossPreventionRule` — update stuck in `admitted` (unconditional).
- `WorkflowUpdateCompletionRule` — update stuck in `accepted`, never completed.
- `WorkflowUpdateDeduplicationRule` — duplicate requests, update not resolved. *(dup of below)*
- `WorkflowUpdateDedupProgressRule` — deduplicated update stuck non-terminal. *(dup of above)*
- `WorkflowUpdateContinueAsNewRule` — update still `admitted` on a completed workflow.
- `WorkflowUpdateWorkerSkippedRule` — update still `admitted` after a task was polled.
- `WorkflowUpdateContextClearRule` — update `admitted`/`accepted`, no pending task, workflow not completed.
</content>
</invoke>
