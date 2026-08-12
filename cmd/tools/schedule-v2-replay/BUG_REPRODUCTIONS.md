# Schedule V1 versus CHASM bug reproductions

## Summary

The production replay sweep found one high-confidence CHASM implementation bug and two genuine
behavioral compatibility differences. The latter are important migration decisions, but the
current evidence does not classify them as CHASM correctness bugs.

| Finding | Classification | Observable effect | Production evidence |
| --- | --- | --- | --- |
| Immediate trigger ignores `ScheduledTime` | V2 bug | Workflow identity and deduplication can change across a second boundary. | One history produced 57 CHASM starts versus 56 V1 starts. |
| `SKIP` resolves overlap at the exact deadline | Migration incompatibility | CHASM may wait for the next occurrence where V1 starts the just-passed occurrence. | Eight histories differed; several produced roughly half as many CHASM starts. |
| Non-failed terminal statuses become `ContinuedFailure` | Migration incompatibility | The next workflow receives a failure in CHASM and nil in V1. | Five histories independently showed timeout or termination differences. |

Replay itself never migrates or modifies a V1 schedule. It applies history inputs to a fresh
in-memory CHASM state machine with time skipping.

## Reproduction tests

Run the focused characterization tests:

```sh
go test -tags test_dep ./chasm/lib/scheduler ./service/worker/scheduler \
  -run '^TestDivergenceRepro_' -count=1
```

These tests intentionally assert the behavior observed in the current implementations. They are
regression evidence for the investigation, not assertions that every difference is desirable.

### Immediate-trigger identity

`TestDivergenceRepro_ImmediateTriggerIgnoresScheduledTime` supplies a trigger timestamp one second
before CHASM processing time. It demonstrates that the buffered start uses processing time and
does not use the request's `ScheduledTime`.

V1 reads non-zero `TriggerImmediately.ScheduledTime`. CHASM's `NewImmediateBackfiller` instead
stores `ctx.Now` in `LastProcessedTime`, and `processTrigger` uses that value to construct the
workflow ID. Receipt just after a second boundary can therefore select a different workflow ID
than V1. This is a high-confidence V2 bug because the request field defines target-workflow
identity.

### `SKIP` at workflow completion

The paired tests
`TestDivergenceRepro_SkipDropsOccurrenceAtDeadlineWhileWorkflowRunning` and
`TestDivergenceRepro_SkipStartsOccurrenceAfterWorkflowCompleted` isolate the ordering rule. A due
occurrence is permanently dropped when CHASM observes a running workflow at its exact deadline;
the same occurrence starts when completion is already recorded.

V1 can generate an elapsed occurrence and refresh workflow completion later in the same workflow
task. It may consequently start a nominal occurrence that CHASM already skipped at the physical
deadline. CHASM matches the documented `SKIP` contract, so this is a potentially high-impact
migration incompatibility rather than a demonstrated V2 correctness bug.

### Terminal failure propagation

`TestDivergenceRepro_V1OmitsNonFailedTerminalFailure` demonstrates that the V1 watcher returns nil
failure for canceled, terminated, and timed-out executions.
`TestDivergenceRepro_CHASMPropagatesTerminalFailureToNextStart` demonstrates that CHASM passes typed
failures for those statuses as `ContinuedFailure` on the next `StartWorkflowExecution` request.

The richer CHASM value is internally consistent, but workflows inspecting `ContinuedFailure` can
take a different branch after migration. Exact V1 compatibility would require an explicit choice
to suppress these failure types.

## Recommended disposition

1. Fix immediate-trigger identity by using non-zero `TriggerImmediately.ScheduledTime`, with
   framework time only as a compatibility fallback when the field is absent.
2. Decide whether migration promises exact V1 `SKIP` boundary behavior or the documented overlap
   semantics. Do not reproduce V1 worker latency accidentally.
3. Decide whether richer terminal failures are an intentional V2 contract change or whether V2
   should omit canceled, terminated, and timed-out `ContinuedFailure` values for compatibility.

The full timelines, replay classifications, and production case matrix are in
[DIVERGENCES.md](DIVERGENCES.md) and
[S_AW031_SWEEP_2026-08-09.md](S_AW031_SWEEP_2026-08-09.md).
