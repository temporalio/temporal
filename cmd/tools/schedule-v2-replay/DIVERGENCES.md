# Schedule V1 versus CHASM divergence report

## Executive summary

In this report, an **action** is an operation selected by a Temporal Schedule. Every fixture in
this corpus uses the `StartWorkflow` action, so an action difference means that one scheduler
called `StartWorkflowExecution` for a workflow ID that the other scheduler did not.

The corrected current-V1 corpus contains nine scenarios. After fixing immediate-trigger identity
and normalizing approved subsecond search-attribute precision, eight differ only in observation
time. The `skip-running` scenario retains a named compatibility difference because it selects a
different nominal occurrence:

| Finding | V1 | CHASM | Compatibility impact | Likelihood of a V2 correctness bug |
| --- | --- | --- | --- | --- |
| Immediate-action scheduled time | Uses `TriggerImmediately.ScheduledTime`, then truncates for the request | Uses non-zero `ScheduledTime`, with framework time as a fallback | Subsecond search-attribute precision is normalized by shadow comparison | Fixed |
| `SKIP` completion near a deadline | Starts nominal `07:38:50` | Skips `07:38:50`, starts `07:38:51` | Medium | Low |
| Pause near a deadline (original corpus) | Suppressed nominal `07:18:04` | Started nominal `07:18:04` | Low | Low |

The pause finding was caused by the fixture's capture-time pause and is no longer present after
removing that teardown operation. It still demonstrates a real ordering difference, but it is not
evidence of a defect in the tested pause/unpause happy path. The `SKIP` difference remains after
the fixture correction and is a genuine compatibility difference.

No live schedule is migrated or modified by replay. The checked histories are applied to fresh,
in-memory CHASM state machines.

## How to read the classifications

- `match`: the compared decisions and state match.
- `timing_only`: both schedulers start the same workflow IDs, but the V1 local-activity result and
  CHASM start are observed at different times. Local-activity marker time is not the exact V1
  decision time, so this is evidence about timing rather than a behavioral failure.
- `known_compatibility`: a named, reviewed V1/V2 behavior difference. These findings remain in
  reports and cohort counts but do not fail the default `significant` gate.
- `significant`: an extra or missing workflow ID, a different action count, or a reliably
  comparable final configuration differs.
- `inconclusive`: the history ends before an external input can be applied with confidence.
- `unsupported`: the history contains an input the harness cannot safely model.

The replay also compares normalized `StartWorkflowExecution` requests. It ignores generated RPC
identity, request ID, callback plumbing, namespace injection, empty last-completion state, and
search-attribute type metadata. Differences in workflow type, task queue, inputs, timeouts, retry
policy, memo, headers, user metadata, and search-attribute values remain significant.

The final state comparison intentionally excludes `LimitedActions` and `RemainingActions`. Workflow
history exposes starts but does not always reveal whether a start was manual; triggers and
backfills do not consume the scheduled-action limit. Workflow IDs and action counts remain in the
oracle, and the JSON report includes CHASM's final state.

## Resolved bug: immediate-action scheduled time

Seven fixtures use `TriggerImmediately` during schedule creation. For the initial action, V1 and
CHASM start the same workflow ID, but the `TemporalScheduledStartTime` search attribute differs.
For example:

- Workflow ID nominal suffix: `2026-08-09T07:38:24Z`.
- V1 search attribute: `2026-08-09T07:38:24Z`.
- CHASM search attribute: `2026-08-09T07:38:24.750628Z`.

The frontend stamps `TriggerImmediately.ScheduledTime` before delivering the patch. V1 consumes
that field in `service/worker/scheduler/workflow.go`, then truncates the buffered nominal time to a
whole second when constructing the workflow ID and scheduled-time search attribute. Before this
fix, CHASM's `NewImmediateBackfiller` initialized `LastProcessedTime` from `ctx.Now` and
`processTrigger` used that framework time without reading `ScheduledTime`.

Most requests are received within the same second, so the workflow IDs match and only the search
attribute differs. The production sweep contains a request stamped at `23:33:03.998` and delivered
at `23:33:04.003`; V1 targets the `23:33:03` workflow ID while CHASM targets `23:33:04`. With
same-second repeated triggers and workflow-ID rejection, this changes which attempt succeeds and
increases the CHASM action count by one. Chronological success/failure replay confirms this is not
an oracle ordering artifact.

First-pass triage:

- **Compatibility impact: Low normally, Medium at a second boundary.** The common case changes
  only timestamp precision; a request crossing the boundary can change workflow identity and
  deduplication outcome.
- **V2 correctness-bug determination: Confirmed and fixed.** `ScheduledTime` is explicitly the
  timestamp used for target-workflow identity.
- **Confidence: High.** Controlled and production evidence agree, and the responsible V1/CHASM
  code paths are direct.

CHASM now uses a non-zero request `ScheduledTime`, falling back to framework time only for older
requests where the field is absent. Shadow comparison truncates only
`TemporalScheduledStartTime` to whole-second precision. It does not normalize workflow IDs,
action counts, or other request fields, so a future identity regression remains significant.

## Confirmed difference: `SKIP` near workflow completion

### Scenario

- One-second interval.
- Default-style `SKIP` overlap policy.
- The action workflow runs for three seconds.
- An immediate action starts at nominal `07:38:47`.
- One additional scheduled action is permitted.

### Observed result

V1 starts:

1. `schedule-v1-conformance-skip-running-action-2026-08-09T07:38:47Z`
2. `schedule-v1-conformance-skip-running-action-2026-08-09T07:38:50Z`

CHASM starts:

1. `schedule-v1-conformance-skip-running-action-2026-08-09T07:38:47Z`
2. `schedule-v1-conformance-skip-running-action-2026-08-09T07:38:51Z`

Both implementations start two workflows, but they select different nominal occurrences. This is
not merely timestamp noise because the nominal time is part of the workflow ID.

### Timeline and cause

1. The first workflow is still running at the exact `07:38:50` schedule deadline.
2. CHASM's generator executes at that deadline. Its invoker sees a running workflow and applies
   `SKIP`, permanently dropping the `07:38:50` occurrence.
3. V1's timer fires at `07:38:50.419`; its workflow task starts at `07:38:50.423`.
4. V1 first generates the nominal `07:38:50` buffered start for the elapsed time range. Later in
   the same workflow iteration, it refreshes the prior workflow's status and sees it completed.
5. V1 therefore processes the already-buffered `07:38:50` start with no workflow now running.
6. The recorded completion reaches CHASM at approximately `07:38:50.434`. The `07:38:50`
   occurrence has already been skipped, so CHASM selects the next occurrence at `07:38:51`.

This follows directly from the execution models. V1 generates elapsed occurrences in
`service/worker/scheduler/workflow.go` before `processBuffer` refreshes running executions. CHASM
generates at exact physical task deadlines and resolves overlap in
`chasm/lib/scheduler/invoker_tasks.go` using state at that deadline.

### First-pass triage

- **Compatibility impact: Medium.** `SKIP` is the default overlap policy. After migration, a
  schedule whose workflow completes shortly after a nominal boundary may wait for the following
  occurrence where V1 would immediately start the just-passed occurrence. This changes workflow
  IDs and can shift cadence by one interval.
- **V2 correctness-bug likelihood: Low.** The public enum contract says that while a workflow is
  running `SKIP` starts nothing and, after completion, considers the next scheduled event after
  that time. CHASM's `07:38:51` decision matches that wording; V1's delayed workflow-task
  coalescing admits the earlier `07:38:50` occurrence.
- **Confidence: High.** The result survives fixture correction, has exact workflow-ID evidence,
  and is covered by corpus and invoker boundary tests.

This should be treated as a migration-compatibility decision, not fixed as a V2 defect without
first deciding whether exact V1 behavior or the documented `SKIP` contract takes precedence.

## Confirmed difference: terminal failure propagation

V1's workflow watcher returns a failure payload only for `FAILED`. For `CANCELED`, `TERMINATED`,
and `TIMED_OUT`, it returns the terminal status with no failure. Consequently, V1 leaves
`ContinuedFailure` unchanged. Workflow completion callbacks used by CHASM construct typed canceled,
terminated, and timeout failures, and CHASM stores that failure for the next
`StartWorkflowExecution` request.

The difference is covered directly by V1 response-builder and CHASM invoker tests. It appears
independently in five production histories before their first unmatched occurrence: three timeout
chains and two terminated workflows. Later repeated mismatches are downstream, not independent
failures.

Triage:

- **Compatibility impact: Medium.** The next workflow can observe a non-nil `ContinuedFailure` in
  V2 where V1 supplied nil and may branch differently.
- **V2 correctness-bug likelihood: Low.** The CHASM callback contains more complete terminal-state
  information, and its behavior is internally consistent. This is nevertheless an observable
  migration difference that needs an explicit compatibility decision.
- **Confidence: High.** Both implementations' unit tests and production request diffs show the
  same status matrix.

The 36 production `LastCompletionResult` mismatches and activity-failure mismatches begin only
after a nominal occurrence has already diverged. They are causal fallout from comparing different
workflow chains, not evidence of a separate result-conversion bug.

## Closed finding: pause at a deadline

The original `pause-unpause` fixture produced two V1 starts and three CHASM starts. The extra
CHASM workflow had nominal time `07:18:04`.

The fixture waited for two actions and then sent a final pause solely to stop history capture. The
pause arrived at `07:18:04.015`, after CHASM had executed the exact `07:18:04` deadline. V1 did not
process its corresponding timer until a later workflow task; it buffered the elapsed occurrence,
processed the pause signal, and then dropped that buffered start while paused.

The regenerated fixture stops through `RemainingActions` and does not issue the capture-time
pause. V1 and CHASM now start the same two workflow IDs in the pause/unpause scenario. Triage:

- **Compatibility impact: Low.** A pause issued within scheduler-processing latency of a deadline
  can suppress a just-due V1 occurrence while V2 has already started it.
- **V2 correctness-bug likelihood: Low.** A pause received after a deadline is not normally
  expected to retroactively undo an action already started at that deadline.
- **Status: Closed as a conformance-fixture false positive.** The boundary behavior is real but
  was not caused by the intended pause/unpause interaction.

## Recommended follow-up

1. Decide explicitly whether migration promises exact V1 `SKIP` boundary behavior or adherence to
   the documented overlap policy.
2. Sample production V1 histories with `SKIP` and measure how often completion falls between a
   nominal deadline and V1 workflow-task processing.
3. Monitor the named `skip_deadline_boundary` and `terminal_failure_propagation` findings in
   production shadow reports even when the default gate succeeds.
4. Expand the controlled corpus across interval lengths, workflow durations just before/after a
   deadline, jitter, and worker latency. This will quantify impact rather than change semantics.
5. If strict compatibility is required, design an explicit grace/coalescing rule. Do not infer it
   from V1 worker latency: doing so would make CHASM timing load-dependent and may contradict the
   API contract.

## Reproduction

Generate fixtures against a disposable server configured for V1 schedule creation:

```sh
go run ./cmd/tools/schedule-v2-replay \
  -generate-scenarios \
  -namespace default \
  -history-dir ./chasm/lib/scheduler/testdata/v1-replay \
  -timeout 2m
```

Produce a JSON report without failing on known differences:

```sh
SCHEDULE_V1_HISTORY_DIR="$PWD/chasm/lib/scheduler/testdata/v1-replay/current-v1" \
SCHEDULE_V2_REPLAY_REPORT=/tmp/current-v1-replay-report.json \
SCHEDULE_V2_REPLAY_FAIL_ON=none \
go test -tags test_dep ./chasm/lib/scheduler \
  -run '^TestDownloadedV1HistoriesAgainstCHASM$' -count=1
```

Use `SCHEDULE_V2_REPLAY_FAIL_ON=significant` to make the confirmed `SKIP` and immediate-action
request differences fail the comparison command.
