# Schedule V1 to V2 replay harness

This read-only harness downloads legacy scheduler workflow histories through
`GetWorkflowExecutionHistory`, verifies that they still replay against Schedule V1, and then
drives fresh in-memory Schedule V2 instances with the CHASM test engine.

The CHASM replay starts from the `StartScheduleArgs` in event 1, advances a virtual clock to the
timestamps of observed timer and external-input events, applies update and patch signals, returns
observed workflow-start results through mocked clients, and applies observed workflow completions
as CHASM callbacks. It compares workflow-start decisions, action counts, and final schedule
configuration. Unsupported inputs fail the test instead of being silently ignored.

V1 may record `WatchWorkflow` as either a normal activity or a local activity. For local
activities, the marker contains the result but not the watched workflow ID. The harness therefore
performs a deterministic V1 SDK replay with an outbound workflow interceptor, pairs each captured
watch request with its marker, and applies the result at the marker's original timestamp. This
preserves overlap-policy behavior without guessing which running workflow completed.

The harness does not replace CHASM's scheduling calculations with V1 decisions. If an older V1
version used different jitter or next-time semantics, the replay fails at the first completion for
which CHASM has not emitted the corresponding start and reports it as a scheduling/timing
divergence. This keeps completion-replay gaps separate from genuine generator behavior changes.

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
  --history-dir /tmp/schedule-v1-replay-histories
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
V1 replay, unsupported event, CHASM error, or behavioral mismatch makes the script exit nonzero.

Set `TEMPORAL_API_KEY` for API-key authentication and pass `--tls` when connecting over TLS.
