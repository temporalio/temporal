# Umpire — Nexus Coverage Plan

How to extend the umpire from workflow-update coverage to **Nexus operations**. For the *why*
read [`UMPIRE_SPEC.md`](./UMPIRE_SPEC.md); for *how it fits together* read
[`UMPIRE_MONITOR.md`](./UMPIRE_MONITOR.md); for current status read [`UMPIRE_PLAN.md`](./UMPIRE_PLAN.md).

## Snapshot

The umpire ingests two signals through `tests/testcore`: **gRPC request/response facts** (via
the unary interceptor, `NewUnaryServerInterceptor`) and **OTEL span-event facts** (via the
`SpanProcessor`, `Umpire.OnEnd`). Facts route to `Lifecycled` entities (FSMs); safety/liveness
rules judge them. Workflow update proved the pattern: its interesting lifecycle lives *inside*
history and is surfaced by explicit span-event instrumentation
(`emitUpdateLifecycleEvent`, `service/history/workflow/update/util.go`), decoded by `SpanFact`s.

**Nexus is the same situation, and the same shape of solution applies.**

## The gap

The Nexus operation lifecycle is an internal HSM (`components/nexusoperations/statemachine.go`)
and is **invisible over gRPC**. Confirmed: there is *no* telemetry instrumentation in
`components/nexusoperations/` today. The gRPC-observable surface (`PollNexusTaskQueue`,
`RespondNexusTaskCompleted/Failed`, operator `*NexusEndpoint`) only sees the *edges*, not the
operation state transitions that carry the invariants.

So covering Nexus needs the same three layers workflow update has:

1. **Span instrumentation** on the HSM transitions (the load-bearing, server-side part).
2. **`SpanFact`s** that decode those span events.
3. A **`Lifecycled` entity** mirroring the HSM + **rules**.

## Source of truth: the operation HSM

From `components/nexusoperations/statemachine.go` (`Transition*` vars):

```
UNSPECIFIED → SCHEDULED ⇄ BACKING_OFF
SCHEDULED / BACKING_OFF → STARTED
{SCHEDULED, BACKING_OFF, STARTED} → SUCCEEDED | FAILED | CANCELED | TIMED_OUT   (terminal)
```

- `TransitionScheduled`: UNSPECIFIED → SCHEDULED (fires in `AddChild`, immediately after init).
- `TransitionAttemptFailed`: SCHEDULED → BACKING_OFF (retryable attempt failure).
- `TransitionRescheduled`: BACKING_OFF → SCHEDULED (retry).
- `TransitionStarted`: SCHEDULED/BACKING_OFF → STARTED (async handler ack).
- `TransitionSucceeded` / `Failed` / `Canceled` / `TimedOut`: from any of
  SCHEDULED/BACKING_OFF/STARTED → the respective terminal state. Note **sync completion skips
  STARTED** (SCHEDULED → SUCCEEDED directly), so "started precedes succeeded" is *not* an
  invariant.

There is also a child **Cancelation** sub-machine (`CancelationMachineKey`,
`TransitionCancelation*`) — out of scope for v1 (see follow-ups).

**Identity.** An operation is the HSM node whose ID is the scheduled-event ID, under a caller
workflow run. Fields available at the node (`persistencespb.NexusOperationInfo`): `Endpoint`,
`Service`, `Operation`, `RequestId`. Proposed entity path:
`Namespace → Workflow(callerWorkflowID) → NexusOperation(workflowID:scheduledEventID)`.

## Proposed code

### 1. Telemetry constants — `common/telemetry/tags.go`

```go
AttrNexusEndpoint         attribute.Key = "nexus.endpoint"
AttrNexusService          attribute.Key = "nexus.service"
AttrNexusOperation        attribute.Key = "nexus.operation"
AttrNexusScheduledEventID attribute.Key = "nexus.scheduled_event_id"
AttrNexusOutcome          attribute.Key = "nexus.outcome"

EventNexusOperationScheduled     = "NexusOperationScheduled"
EventNexusOperationStarted       = "NexusOperationStarted"
EventNexusOperationSucceeded     = "NexusOperationSucceeded"
EventNexusOperationFailed        = "NexusOperationFailed"
EventNexusOperationCanceled      = "NexusOperationCanceled"
EventNexusOperationTimedOut      = "NexusOperationTimedOut"
EventNexusOperationAttemptFailed = "NexusOperationAttemptFailed" // → backing_off
```

### 2. Server instrumentation — `components/nexusoperations`

An emitter mirroring `emitUpdateLifecycleEvent`, called from the transition sites in
`events.go` / `executors.go` where the `hsm.Node` (and thus the caller workflow key) is in
scope:

```go
func emitNexusLifecycleEvent(ctx context.Context, tracer trace.Tracer, event string, ref hsm.Ref, op Operation, extra ...attribute.KeyValue) {
    _, span := tracer.Start(ctx, "nexus.operation.lifecycle")
    defer span.End()
    wf := ref.WorkflowKey // namespaceID, workflowID, runID
    attrs := []attribute.KeyValue{
        telemetry.AttrNamespaceID.String(wf.NamespaceID),
        telemetry.AttrWorkflowID.String(wf.WorkflowID),
        telemetry.AttrRunID.String(wf.RunID),
        telemetry.AttrNexusScheduledEventID.String(ref.StateMachineRef.MachineKey.Id),
        telemetry.AttrNexusEndpoint.String(op.Endpoint),
        telemetry.AttrNexusService.String(op.Service),
        telemetry.AttrNexusOperation.String(op.Operation),
    }
    span.AddEvent(event, trace.WithAttributes(append(attrs, extra...)...))
}
```

**Open plumbing question (the only non-mechanical part):** the transitions are package-level
`var`s (`TransitionScheduled`, …), so the tracer + node ref must be threaded in from the
executor call sites, not from inside the transition closures. Emit where node metadata (the
workflow key) is accessible — the executor / event-handler layer in `executors.go` and
`events.go`. This is the part with design risk; everything else is additive test-only code.

### 3. Facts — `tests/umpirev1/fact/`

Add `NexusOperationType umpire.EntityType = "NexusOperation"` to `constants.go`. A shared
decoder `nexus_span.go` (mirroring `update_span.go`), plus one `SpanFact` per event:

```go
// nexus_span.go
func importNexusSpanEvent(attrs attribute.Set) (schedEventID, workflowID string, path *umpire.EntityPath) {
    schedEventID = strAttr(attrs, telemetry.AttrNexusScheduledEventID)
    workflowID   = strAttr(attrs, telemetry.AttrWorkflowID)
    nsID        := strAttr(attrs, telemetry.AttrNamespaceID)
    if schedEventID == "" {
        return "", "", nil
    }
    self := umpire.NewEntityID(NexusOperationType, workflowID+":"+schedEventID)
    var parents []umpire.EntityID
    if workflowID != "" {
        parents = append(parents, umpire.NewEntityID(WorkflowType, workflowID))
    }
    return schedEventID, workflowID, nsPath(nsID, self, parents...)
}

// nexus_operation_started.go (representative; one file per event)
type NexusOperationStarted struct {
    ScheduledEventID, WorkflowID string
    EntityPath                   *umpire.EntityPath
}

func (e *NexusOperationStarted) Name() string                     { return telemetry.EventNexusOperationStarted }
func (e *NexusOperationStarted) TargetEntity() *umpire.EntityPath { return e.EntityPath }
func (e *NexusOperationStarted) ImportSpanEvent(attrs attribute.Set) bool {
    e.ScheduledEventID, e.WorkflowID, e.EntityPath = importNexusSpanEvent(attrs)
    return e.ScheduledEventID != ""
}
```

Terminal facts (`NexusOperationSucceeded/Failed/Canceled/TimedOut`) additionally read
`AttrNexusOutcome` to record the outcome on the entity.

### 4. Entity — `tests/umpirev1/model/nexus_operation.go`

A `Lifecycled` entity mirroring the HSM (same shape as `WorkflowUpdate`):

```go
func NewNexusOperation() *NexusOperation {
    op := &NexusOperation{ /* Flags: Scheduled/Started/Succeeded/... */ }
    op.FSM = umpire.NewLifecycle(umpire.LifecycleSpec{
        Initial: "unspecified",
        Transitions: []umpire.Transition{
            {Event: "schedule",       From: []string{"unspecified", "backing_off"},   To: "scheduled"},
            {Event: "attempt_failed", From: []string{"scheduled"},                    To: "backing_off"},
            {Event: "start",          From: []string{"scheduled", "backing_off"},     To: "started"},
            {Event: "succeed", From: []string{"scheduled", "backing_off", "started"}, To: "succeeded"},
            {Event: "fail",    From: []string{"scheduled", "backing_off", "started"}, To: "failed"},
            {Event: "cancel",  From: []string{"scheduled", "backing_off", "started"}, To: "canceled"},
            {Event: "timeout", From: []string{"scheduled", "backing_off", "started"}, To: "timed_out"},
        },
        // Terminal states derive automatically. A scheduled/backing_off/started
        // operation must eventually settle.
        MustProgress: []string{"scheduled", "backing_off", "started"},
    })
    return op
}
```

`OnFact` fires the matching event per fact type — the exact shape of `WorkflowUpdate.OnFact`,
deriving `*At` accessors from `FSM.EnteredAt(...)` so state ⇔ timestamp holds by construction.

### 5. Registration — `tests/umpirev1/model/register.go`

Register the 7 span facts and `NewNexusOperation` (subscribing to those facts).

### 6. Key helper — `tests/umpirev1/entity_key.go`

Add `WorkflowPath.NexusOperation(scheduledEventID) string` returning the registry key
`namespace:…@Workflow:wf@NexusOperation:wf:<schedEventID>`, so tests can assert
`RequireRulePassed`.

### 7. Rules — `tests/umpirev1/rule/`

Two new rules; the generic `EntityProgress` (already registered) covers *stuck* operations for
free via `MustProgress`:

| Rule | Kind | Property |
|---|---|---|
| `NexusOperationClosure` | safety | No operation transition after the caller workflow reaches `completed`. Direct analog of `WorkflowUpdateClosure`. |
| `NexusOperationLifecycle` | safety | `FSM.Illegal()` is empty for Nexus operations (no double-terminal, no start-after-terminal). A Nexus-*scoped* version of the generic `EntityTransitionLegality`, which is kept unregistered globally because it over-captures benign update races. |
| `EntityProgress` (existing) | liveness | An operation left in `scheduled`/`backing_off`/`started` at teardown is flagged. No new code. |

Each new rule ships with a positive + negative unit test, per project convention
(`rule/*_test.go`).

## Scope & follow-ups

**v1 covers the workflow-driven operation lifecycle** — the core of `tests/nexus_workflow_test.go`
(cancel, sync/async completion, failure, schedule-to-close / schedule-to-start /
start-to-close timeouts, retries via `backing_off`).

Deferred, lower priority:

- **Endpoint CRUD** (`tests/nexus_endpoint_test.go`) — a `NexusEndpoint` entity built purely
  from `operatorservice` request facts (Create/Update/Delete). No instrumentation needed, but
  lower invariant density.
- **Standalone / CHASM operations** (`tests/nexus_standalone_test.go`) — different identity
  (its own operation ID, not a workflow child); reuses the same FSM under a separate root path.
- **Cancelation sub-machine** — the child `Cancelation` HSM
  (scheduled/backing_off/succeeded/failed), for asserting cancel delivery.

## Risks

- **Instrumentation is the only server-side change** and carries the design risk (transition
  emit plumbing, above). Everything else is additive test-only code.
- **Same false-positive vectors as workflow update** (see `UMPIRE_PLAN.md`): workflow close is
  observed for only one path, and entity `*At` timestamps are observation-time, not event-time.
  `NexusOperationClosure` inherits both — triage against the real suite before enforcing.
- **Cheap to validate:** run `TestNexusWorkflowTestSuiteHSM` (and `…CHASM`) under enforcement
  and triage every violation, exactly as the update rules were validated.
