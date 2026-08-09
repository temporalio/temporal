# Schedule V1 versus CHASM divergence report

## Executive summary

In this report, an **action** is an operation selected by a Temporal Schedule. Every fixture in
this corpus uses the `StartWorkflow` action, so an action difference means that one scheduler
called `StartWorkflowExecution` for a workflow ID that the other scheduler did not.

The corrected current-V1 corpus contains nine scenarios. Eight produce the same workflow-start
decisions and differ only in when the result is observed. One scenario, `skip-running`, selects a
different nominal occurrence:

| Finding | V1 | CHASM | Compatibility impact | Likelihood of a V2 correctness bug |
| --- | --- | --- | --- | --- |
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
- `significant`: an extra or missing workflow ID, a different action count, or a reliably
  comparable final configuration differs.
- `inconclusive`: the history ends before an external input can be applied with confidence.
- `unsupported`: the history contains an input the harness cannot safely model.

The final comparison intentionally excludes `LimitedActions` and `RemainingActions`. Workflow
history exposes starts but does not always reveal whether a start was manual; triggers and
backfills do not consume the scheduled-action limit. Workflow IDs and action counts remain in the
oracle, and the JSON report includes CHASM's final state.

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
3. Expand the controlled corpus across interval lengths, workflow durations just before/after a
   deadline, jitter, and worker latency. This will quantify impact rather than change semantics.
4. If strict compatibility is required, design an explicit grace/coalescing rule. Do not infer it
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

Use `SCHEDULE_V2_REPLAY_FAIL_ON=significant` to make the confirmed `SKIP` difference fail the
comparison command.
