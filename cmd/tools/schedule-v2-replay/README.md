# Schedule V1 to V2 replay harness

This read-only harness downloads legacy scheduler workflow histories through
`GetWorkflowExecutionHistory`, verifies that they still replay against Schedule V1, and then
drives fresh in-memory Schedule V2 instances with the CHASM test engine. Production history files
contain raw payloads and must be treated as sensitive data.

The CHASM replay starts from the `StartScheduleArgs` in event 1 and advances a virtual clock to
each CHASM task deadline and V1 external input in chronological order. External inputs at a shared
timestamp are applied before timer work. The harness applies update and patch signals, returns
observed workflow-start results through mocked clients, and applies observed workflow completions
as CHASM callbacks. It compares workflow IDs, normalized `StartWorkflowExecution` requests, action
counts, and final schedule configuration, and reports selected CHASM scheduler counters.
In the current corpus every schedule action is `StartWorkflow`, so an action mismatch means a
workflow execution was started by only one implementation. See [DIVERGENCES.md](DIVERGENCES.md)
for the investigated controlled-corpus findings and first-pass severity triage. See
[S_AW031_SWEEP_2026-08-09.md](S_AW031_SWEEP_2026-08-09.md) for the redacted production-sweep
results and replay-fidelity limitations found on `s-aw031`.

V1 may record `WatchWorkflow` as either a normal activity or a local activity. For local
activities, the marker contains the result but not the watched workflow ID. The harness therefore
performs a deterministic V1 SDK replay with an outbound workflow interceptor, pairs each captured
watch request with its marker, and applies the result at the marker's original timestamp. This
preserves overlap-policy behavior without guessing which running workflow completed.
Final observed start failures are applied to CHASM. A successful local activity marker records its
attempt count but not intermediate failure details, so histories with transient local-activity
retries are classified as `inconclusive` instead of inventing an error sequence.

The harness does not replace CHASM's scheduling calculations with V1 decisions. A completion that
arrives before CHASM's corresponding start is held until that start occurs. Equal action decisions
at different observed times are classified as `timing_only`. Approved subsecond-only differences
in `TemporalScheduledStartTime` are normalized before request comparison. The exact-deadline
`SKIP` behavior and terminal `ContinuedFailure` propagation are reported as
`known_compatibility`; they remain counted and visible but do not fail the default `significant`
gate. Other extra or missing workflow IDs, action counts, request inputs, or final state remain
`significant`. An extra CHASM action receives a synthetic successful start response, so one
mismatch does not create an artificial retry cascade.

Continue-as-new histories can begin with running workflows. The harness seeds their workflow/run
identity from the migrated buffered-start state before applying later completion events. A
completion without a run ID is applied only when exactly one execution with that workflow ID is
known; an ambiguous completion is `inconclusive` instead of being guessed.

No API that updates, signals, patches, pauses, or migrates a live schedule is called. The tool has
no migration option.

Run the read-only replay:

```sh
go run ./cmd/tools/schedule-v2-replay \
  -address localhost:7233 \
  -namespace default \
  -schedule-id my-schedule \
  -history-out /tmp/my-schedule-history.json.gz
```

Replay a sample from every namespace and save each history:

```sh
./cmd/tools/schedule-v2-replay/replay-sample.sh \
  --acknowledge-sensitive-data \
  --address localhost:7233 \
  --sample-size 10 \
  --history-dir /tmp/schedule-v1-replay-histories \
  --report /tmp/schedule-v2-replay-report.json
```

The history directory must be an approved encrypted local volume that is not synchronized or
checked into source control. The collector creates directories with mode `0700`, writes histories
and manifests atomically with mode `0600`, uses opaque hashed filenames, and records checksums. It
rejects an existing history directory that is accessible by group or other users.
Reports redact production identifiers by default; pass `--unredacted-report` only for restricted
local triage. Report version 4 includes named known-compatibility findings and payload-safe request
diagnostics: presence, element counts, encodings, reserved scheduled-time values, and failure-info
types. The unredacted report also
contains deterministic payload digests; neither report contains decoded user payload values.

Restrict the sample to one namespace:

```sh
./cmd/tools/schedule-v2-replay/replay-sample.sh \
  --acknowledge-sensitive-data \
  --namespace payments \
  --sample-size 25 \
  --history-dir /tmp/payments-schedule-histories
```

Batch mode lists the full namespace population and orders candidates by a deterministic hash of
the seed, namespace, and schedule ID. It collects a uniform base sample, then continues scanning to
top up behavioral cohorts for spec type, overlap policy, pause/update/backfill interactions,
workflow completion, and start failures. V2-only schedules are skipped because they have no SDK
workflow history. The defaults collect at most three continue-as-new runs from the last 30 days,
inspect at most 20 times the base sample, run two namespaces concurrently, and issue at most two
production API requests per second globally. Event and byte limits prevent unbounded histories.

Each namespace has a resumable collection manifest containing the sampling parameters, population
and scan counts, server and collector versions, run horizon, per-case errors, checksums, cohorts,
and truncation status. Changing a fidelity-affecting option requires a new history directory or
`--no-resume`.

Collection and replay can run independently:

```sh
./cmd/tools/schedule-v2-replay/replay-sample.sh \
  --collect-only --acknowledge-sensitive-data \
  --namespace payments --sample-size 100 \
  --history-dir /approved-encrypted-volume/payments

./cmd/tools/schedule-v2-replay/replay-sample.sh \
  --replay-only --history-dir /approved-encrypted-volume/payments \
  --report /tmp/payments-redacted-report.json --fail-on none
```

Replay work is bounded independently of wall-clock test timeout. Defaults are 10,000 virtual
deadlines, 100,000 CHASM tasks, and 10,000 workflow starts per history. Override them with
`--max-replay-deadlines`, `--max-replay-tasks`, and `--max-replay-starts`. Exceeding a limit emits
`replay_budget_exceeded` with counters and classifies the case as `inconclusive`; it does not emit
partial missing-action comparisons.

In combined mode, replay still runs over successfully collected histories when individual
collection cases fail, and the command exits nonzero after both phases. Use `--fail-on all` to
include timing-only and inconclusive results, or `--fail-on none` to collect evidence without
failing the replay phase. Targeted cohort counts are for finding bug classes and must not be used
as fleet-wide prevalence estimates.

The production identity needs only `ListNamespaces`, `ListSchedules`,
`DescribeWorkflowExecution`, `GetWorkflowExecutionHistory`, and `GetSystemInfo`. The collector
never calls a schedule mutation API. Remove raw histories according to the approved retention
policy after triage.

Generate the current-V1 conformance corpus against a disposable local server configured to create
V1 schedules:

```sh
go run ./cmd/tools/schedule-v2-replay \
  -generate-scenarios \
  -namespace default \
  -history-dir ./chasm/lib/scheduler/testdata/v1-replay \
  -timeout 2m
```

The generator creates and deletes nine schedules covering interval, calendar, cron, jitter,
update, pause/unpause, backfill with `ALLOW_ALL`, and long-running actions with `BUFFER_ALL` and
`SKIP`. It refuses to continue if the server creates a CHASM schedule instead of a V1 scheduler
workflow. Each generated history is first replayed through the V1 SDK before being saved. Run the
saved corpus through the offline comparison with `replay-sample.sh` or by setting
`SCHEDULE_V1_HISTORY_DIR`, `SCHEDULE_V2_REPLAY_REPORT`, and `SCHEDULE_V2_REPLAY_FAIL_ON` for
`TestDownloadedV1HistoriesAgainstCHASM`.

Histories under `service/worker/scheduler/testdata` are useful as a historical compatibility
corpus, but should be run separately with `SCHEDULE_V2_REPLAY_FAIL_ON=none`: old V1 releases can
have intentional or already-known next-time and backfill-boundary behavior. Newly generated
current-V1 histories are the significant-divergence gate.

Set `TEMPORAL_API_KEY` for API-key authentication and pass `--tls` when connecting over TLS.

## Investigation reports

- [BUG_REPRODUCTIONS.md](BUG_REPRODUCTIONS.md) summarizes the confirmed findings and their focused
  unit-test reproductions.
- [DIVERGENCES.md](DIVERGENCES.md) explains the comparison model, timelines, and first-pass triage.
- [S_AW031_SWEEP_2026-08-09.md](S_AW031_SWEEP_2026-08-09.md) records the redacted production-sweep
  results and case matrix.
