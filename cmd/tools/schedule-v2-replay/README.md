# Schedule V1 to V2 replay harness

This read-only harness downloads legacy scheduler workflow histories through
`GetWorkflowExecutionHistory`, verifies that they still replay against Schedule V1, and then
drives fresh in-memory Schedule V2 instances with the CHASM test engine.

The CHASM replay starts from the `StartScheduleArgs` in event 1 and advances a virtual clock to
each CHASM task deadline and V1 external input in chronological order. External inputs at a shared
timestamp are applied before timer work. The harness applies update and patch signals, returns
observed workflow-start results through mocked clients, and applies observed workflow completions
as CHASM callbacks. It compares workflow IDs, action counts, and final schedule configuration.
In the current corpus every schedule action is `StartWorkflow`, so an action mismatch means a
workflow execution was started by only one implementation. See [DIVERGENCES.md](DIVERGENCES.md)
for the investigated findings and first-pass severity triage.

V1 may record `WatchWorkflow` as either a normal activity or a local activity. For local
activities, the marker contains the result but not the watched workflow ID. The harness therefore
performs a deterministic V1 SDK replay with an outbound workflow interceptor, pairs each captured
watch request with its marker, and applies the result at the marker's original timestamp. This
preserves overlap-policy behavior without guessing which running workflow completed.

The harness does not replace CHASM's scheduling calculations with V1 decisions. A completion that
arrives before CHASM's corresponding start is held until that start occurs. Equal action decisions
at different observed times are classified as `timing_only`; extra or missing workflow IDs, action
counts, or final state are `significant`. An extra CHASM action receives a synthetic successful
start response, so one mismatch does not create an artificial retry cascade.

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
  --address localhost:7233 \
  --sample-size 10 \
  --history-dir /tmp/schedule-v1-replay-histories \
  --report /tmp/schedule-v2-replay-report.json
```

Restrict the sample to one namespace:

```sh
./cmd/tools/schedule-v2-replay/replay-sample.sh \
  --namespace payments \
  --sample-size 25 \
  --history-dir /tmp/payments-schedule-histories
```

Batch mode pages through schedules in server order until it finds the requested number of
workflow-backed V1 schedules per namespace. V2-only schedules are skipped because they have no SDK
workflow history. Histories are stored as gzip JSON under namespace-specific directories. After
download and V1 replay, the script invokes `TestDownloadedV1HistoriesAgainstCHASM`; any download,
V1 replay, unsupported event, CHASM error, or significant behavioral mismatch makes the script
exit nonzero. Use `--fail-on all` to include timing-only and inconclusive results, or
`--fail-on none` to collect evidence without failing the command.

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
