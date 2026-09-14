# activity/model — a behavioral model of the CHASM activity archetype

`model` is a test-only, server-free description of how a CHASM activity *should* behave:
`Transition(cfg, state, event) -> Outcome` plus the state-derived response predictions
(`ExpectedHeartbeatFlags`, `ExpectedDescribe`). It is not runtime code and must never be imported by
the server binary. It is archetype-level, not tied to one product surface: both the standalone-activity
(SAA) and workflow-activity (WFA) drivers check a real server against it, which is what makes the two
comparable for equivalence rather than each against its own expectations.

- `vocabulary.go` — the event alphabet (`Event`/`EventType`), start-time `Config`, and the
  observable-state projection (`AbstractState`, `Observed`, `Abstract`).
- `model.go` — the transition rules (`Transition`, `Initial`, the per-event functions) and the
  response predictors.
- `explore.go` — pure graph helpers shared by the explorers (`Fingerprint`, `Reachable`, `CellKey`,
  `NeedsToken`, `CarriesReqID`), which events can occur in a state (`Possible`) and the trace check
  built on it (`ValidateTrace`).
- `validate/` — static checks *validating the model* against the product state-machine code, no
  server.

The model is exercised at three tiers, cheapest first: tier 1 is static model validation; tiers 2 and
3 are conformance testing (a running implementation vs the model). All three use the *same* model.


## Tier 1 — no server (~1s)

Model unit tests plus the static checks that `Transition` is total over the RPC domain and that every
status change the model accepts is reachable via the code's declared transitions.

```bash
go test -v -count=1 ./chasm/lib/activity/model/validate
```

## Tier 2 — in-process (~1s)

BFS graph traversal and random walk against a real in-memory CHASM engine with a virtual clock,
covering the worker RPCs plus the StartToClose / Heartbeat / ScheduleToClose timeouts and backoff.

```bash
go test -v -count=1 -run TestConformance ./chasm/lib/activity/
```

Takes the same `TEMPORAL_SAASPEC_*` knobs as tier 3 (below); `model/tuning.go` is their one definition.
Depth is nearly free here: the traversal exhausts the reachable graph at `TEMPORAL_SAASPEC_MAX_DEPTH=8`.

Pins which physical queue an `ActivityDispatchTask` lands on.

```bash
go test -v -count=1 -run TestDispatchRouting ./chasm/lib/activity/
```

## Tier 3 — real server, real timers (~minutes)


```bash
go test -v -count=1 -tags test_dep -run 'TestActivityParityTestSuite/TestParity' ./tests/
go test -v -count=1 -tags test_dep -run 'TestActivityParityTestSuite/TestWFASAAMetricsParity' ./tests/
```

### RPC graph traversal + random walk

```bash
go test -v -count=1 -tags test_dep -run 'TestActivityParityTestSuite/TestConformance/RPCGraphTraversal' ./tests/
go test -v -count=1 -tags test_dep -run 'TestActivityParityTestSuite/TestConformance/RandomWalk$' ./tests/
go test -v -count=1 -tags test_dep -run 'TestActivityParityTestSuite/TestConformance/WFARandomWalk' ./tests/
```

Anchor the SAA walk with `RandomWalk$`, or the pattern also selects `WFARandomWalk`.

#### The WFA explorer

The same `model.Transition` specifies both surfaces, so running it against WFA turns the model into an
equivalence check: a divergence it reports is one a future WFA-on-CHASM port would inherit.

Two things differ from SAA, both because WFA is not a CHASM archetype.

**It checks less.** SAA reads the activity component directly and asserts the full `AbstractState`, the
per-attempt and schedule-to-close task stamps, and the public Describe projection. WFA can only read the
workflow's pending-activity entry, and its history once the activity closes — so it asserts the run state
and attempt while the activity is in progress, and the terminal status after. The reset intent and the
stamps have no WFA analogue and are not asserted. The seam is `activityConformanceTarget.conformsTo`: a
surface asserts what it can see, and the engine never demands an observation a surface cannot make.

**Walk only, no BFS traversal.** The traversal replays each path on a fresh activity, which here means a
workflow start per edge. Its coverage guarantee is worth adding later; it is not free the way it is on
SAA.

Two smaller consequences. The alphabet drops `Terminate` (no workflow-activity form) and `RequestCancel`
(`RequestCancelActivityExecutionRequest` carries no workflow id, so cancellation comes from the workflow
by signal — an occurrence with no accept/reject outcome to predict), which leaves `CancelRequested`
unreachable. And the config set drops the two start-delay ones, since a workflow activity has no
per-activity start delay.

The wrapper workflow is held open past its activity (`wfaActivityParams.HoldOpen`). Without that the two
close together, and an RPC driven from a terminal state would be answered about a workflow that no longer
exists rather than about a closed activity.



Tunable via env vars:
- `TEMPORAL_SAASPEC_MAX_DEPTH=N` — raise the BFS depth cap (default 4).
- `TEMPORAL_SAASPEC_NO_NEGATIVE_POLL=1` — skip the ~3s "a PAUSED activity must not dispatch" long
  poll (the dominant cost of deep walks); the per-edge state check still runs.
- `TEMPORAL_SAASPEC_COMPLETENESS=1` — also print reachable-but-unexercised cells (informational).
- `TEMPORAL_SAASPEC_WALK_STEPS=N` / `TEMPORAL_SAASPEC_WALK_SEED=N` / `TEMPORAL_SAASPEC_VERBOSE=1` —
  random-walk steps per config (default 200), RNG seed (default 1, logged), and per-step logging.

A deep, fast run: BFS to depth 6 without the negative poll, plus a long random walk on a fresh seed.

```bash
TEMPORAL_SAASPEC_MAX_DEPTH=6 \
TEMPORAL_SAASPEC_NO_NEGATIVE_POLL=1 \
TEMPORAL_SAASPEC_COMPLETENESS=1 \
TEMPORAL_SAASPEC_WALK_STEPS=2000 \
TEMPORAL_SAASPEC_WALK_SEED=42 \
TEMPORAL_SAASPEC_VERBOSE=1 \
go test -v -count=1 -timeout 60m -tags test_dep \
  -run 'TestActivityParityTestSuite/TestConformance' ./tests/
```

### Wall-clock directed traces — `activity_standalone_traces_test.go`

Scripted event sequences, SAA-only, that configure a timeout or start-delay/backoff window short and
wait it out, checking the model at every step.

```bash
go test -count=1 -tags test_dep -run 'TestActivityParityTestSuite/Test.*_Declarative' ./tests/
```

### Driver self-tests — `activity_driver_selftest_test.go`

Tests that the drivers report a scripted event whose effect never arrived, and blame themselves
rather than the product when their own timing is at fault.

```bash
go test -count=1 -tags test_dep -run 'TestActivityParityTestSuite/TestSAADriver' ./tests/
```

## Graph tools (no server)

```bash
go run ./chasm/lib/activity/model/cmd/graph                          # counts+nodes+edges, onebox configs
go run ./chasm/lib/activity/model/cmd/graph -show skeleton           # status-level transition relation
go run ./chasm/lib/activity/model/cmd/graph -explorer engine -show counts
go run ./chasm/lib/activity/model/cmd/graph -explorer wfa -show counts
go run ./chasm/lib/activity/model/cmd/graph -config 1 -show nodes,edges
```

Flags: `-explorer {engine,onebox,wfa}` (tier 2, the tier-3 SAA explorer, and the tier-3 WFA one),
`-config N` (index into that explorer's set, default all), `-show` (comma list of
counts,nodes,edges,skeleton).


## Run all tests

```bash
go test -count=1 ./chasm/lib/activity/model/... && \
  go test -count=1 -run TestConformance ./chasm/lib/activity/ && \
  go test -count=1 -tags test_dep -run 'TestActivityParityTestSuite' ./tests/
```

go test -count=1 -run TestDispatchRouting ./chasm/lib/activity/
