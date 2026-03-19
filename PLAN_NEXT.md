# Next Steps

## Terminology

| Term | Purpose | Location |
|------|---------|----------|
| **Umpire** | Orchestrates validation | `tools/umpire/umpire.go` |
| **Pitcher** | Fault injection | `tools/umpire/pitcher/` |
| **Scorebook** | Event recording | `tools/umpire/scorebook/` |
| **Lineup** | Entity registry | `tools/umpire/lineup/` |
| **Rulebook** | Property models | `tools/umpire/rulebook/` |

---

## Status

### Done

| Item | File(s) |
|------|---------|
| Move `lineup` from `tools/bats/` to `tools/umpire/lineup/` | `tools/umpire/lineup/` |
| Pitcher matching (`matchesCriteria` + `extractEntityIDs`) | `tools/umpire/pitcher/pitcher.go` |
| Entity registration wired in `umpire.go` | `tools/umpire/umpire.go` |
| `Workflow` registered as parent entity | `tools/umpire/lineup/entities/register.go` |
| `WorkflowUpdate.WorkflowID` seeded from identity | `tools/umpire/lineup/entities/workflow_update.go` |
| `UpdateCompletion` rule | `rulebook/rules/workflow_update/update_completion.go` |
| `UpdateDeduplication` rule | `rulebook/rules/workflow_update/update_deduplication.go` |
| `UpdateLossPrevention` rule | `rulebook/rules/workflow_update/update_loss_prevention.go` |
| `UpdateStateConsistency` rule | `rulebook/rules/workflow_update/update_state_consistency.go` |
| Unit tests for all 4 update rules | `rulebook/rules/workflow_update/rules_test.go` |
| End-to-end umpire tests (full pipeline) | `tools/umpire/umpire_test.go` |

### Blocked

| Item | Blocker |
|------|---------|
| `SpeculativeTaskRollback` rule | No observable signal for speculative task type (see below) |

---

## Priority 1: SpeculativeTaskRollback Instrumentation

`WORKFLOW_TASK_TYPE_SPECULATIVE` is internal to the history service state machine. It is **not** present in any interceptable gRPC call:

- `matching.AddWorkflowTask` — no task type field
- `workflowservice.PollWorkflowTaskQueue` — response carries no task type
- `historyservice.RespondWorkflowTaskCompleted` — carries capability flags, not task type

To implement the rule, one of these is needed:

### Option A: New Move via OTEL Span (preferred)

Add an OTEL span attribute `workflow.task.type = "speculative"` when the history service schedules a speculative task. The umpire's `AddTraces()` path already processes OTEL spans. A new `WorkflowTaskScheduled` span importer would extract this into a new move, which the `WorkflowTask` entity can consume to set `IsSpeculative = true`.

### Option B: Scorebook Hook in History Service

Add a direct call to `umpire.RecordMove(ctx, &moves.WorkflowTaskScheduled{IsSpeculative: true, ...})` in `workflow_task_state_machine.go` where the task type is known. This is lower infrastructure cost than OTEL but is more invasive to production code.

### Option C: Infer from Update Lifecycle (approximate)

A speculative task's updates are rolled back — they revert from `accepted` to `admitted`. If a `WorkflowUpdate` entity in `accepted` state receives an `UpdateAdmitted` event for the same update ID, that implies rollback. This is imprecise but requires no new infrastructure.

**Recommended: Option A** for clean observability. Only pursue Option C if instrumentation is out of scope.

---

## Priority 2: Integration Test with Live Temporal

**File**: `tests/umpire/fault_injection_test.go` (new)

The existing `umpire_test.go` tests the pipeline with direct event injection. A live integration test would:

1. Start a local Temporal server (`testsuite.TestWorkflowEnvironment` or real server)
2. Configure the pitcher to inject a fault on `AddWorkflowTask` for workflow `"test-123"`
3. Start the workflow
4. Verify the fault was applied (check scorebook)
5. Verify the workflow recovered
6. Call `umpire.Check()` — no violations

This requires the gRPC interceptor to be wired to the test server, which is the remaining gap between the unit-tested pipeline and real production traffic.

---

## Architecture Summary

```
Test
  ├── Configure faults (Pitcher)
  ├── Run workflow
  ├── Record events via gRPC interceptor (RecordMove → Scorebook + Lineup)
  └── Check rules (Umpire + Rulebook)
       └── Report violations
```

The Lineup entity hierarchy (`WorkflowType → WorkflowUpdateType`) enables:
- Parent-child entity relationships
- Scoped queries (all updates for a workflow)
- Clear entity addressing via `Identity{EntityID, ParentID}`
