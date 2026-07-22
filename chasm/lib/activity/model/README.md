# activity/model — a behavioral model of the CHASM activity archetype

`model` is a test-only, server-free description of how a CHASM activity *should* behave:
`Transition(cfg, state, event) -> Outcome` plus the state-derived response predictions
(`ExpectedHeartbeatFlags`, `ExpectedDescribe`). It is not runtime code and must never be imported by
the server binary. It is archetype-level, not tied to one product surface: the standalone-activity
(SAA) frontend driver checks a real server against it today; a workflow-activity driver over the same
model will let the two be checked for equivalence.

- `vocabulary.go` — the event alphabet (`Event`/`EventKind`), start-time `Config`, and the
  observable-state projection (`AbstractState`, `Observed`, `Abstract`).
- `model.go` — the transition rules (`Transition`, `Initial`, the per-event functions) and the
  response predictors.
- `explore.go` — pure graph helpers shared by the explorers (`Fingerprint`, `Reachable`, `CellKey`,
  `NeedsToken`, `KindName`, `EventLabel`).
- `conformance/` — static checks of the model against the product state-machine code, no server.

The model is exercised at three tiers, cheapest first. All three check the real behavior against the
*same* model.

## Tier 1 — no server (~1s): model unit tests + static conformance

`model_test.go` smoke tests (including the dispatch-delay requirements for start_delay and retry
backoff), `conformance.TestModelDecisionCoverage` (`Transition` is total over the RPC domain — no
unexpected panics), and `conformance.TestModelEdgesReachableInCode` (every status change the model
accepts is reachable via the code's declared transitions).

```bash
go test ./chasm/lib/activity/model/...
```

## Tier 2 — in-process (~1s): model-conformance explorers over a real in-memory engine

`TestInProcessSpec` (in package `activity`, `spec_inprocess_test.go`) runs the BFS graph traversal and
random walk against a real in-memory CHASM engine (`chasm/chasmtest`) with a virtual clock
(`clock.EventTimeSource`). Each event is realized by the production component method its worker RPC
invokes (`HandleStarted`/`HandleFailed`/`HandleCompleted`/`HandleCanceled`/`RecordHeartbeat`) via
`chasm.UpdateComponent`; timeouts and backoff are realized by advancing the clock to the relevant
deadline — no onebox, no wall-clock waits. Every step is checked against `model.Transition` for
observed state, reject kind, dispatch readiness, public Describe, and task-invalidation stamps.

Covers the worker RPCs plus the StartToClose / Heartbeat / ScheduleToClose timeouts and backoff — the
wall-clock behavior that is prohibitively slow to explore at tier 3. Operator commands
(pause/cancel/terminate/unpause/reset/update-options) are explored at tier 3.

```bash
go test -run TestInProcessSpec -count=1 -v ./chasm/lib/activity/
```

## Tier 3 — onebox (real server, real timers)

In the commands below, `-count=1` skips the test cache and `-v` shows per-subtest logs. Prefix with
`TEMPORAL_TEST_LOG_LEVEL=ERROR TEMPORAL_TEST_LOG_STACKTRACE_LEVEL=off` to quiet logger noise. The SAA
driver and model-checking engine live in `tests/` (`activity_standalone_utils.go`,
`activity_standalone_spec_harness.go`, `activity_standalone_spec_test.go`,
`activity_standalone_test.go`).

### RPC graph traversal + random walk

`TestSpec` walks the model's reachable states against a real server over the full event alphabet,
including the operator commands. `RPCGraphTraversal` is a breadth-first walk of every decided edge
(deduped by fingerprint, depth-bounded); `RandomWalk` drives one activity forward through randomly
chosen events, reaching deep interaction sequences the bounded traversal never visits. Both check the
internal state, reject kind, heartbeat flags, Describe projection, and task-invalidation stamps
against the model at every step.

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestSpec' -count=1 -v ./tests/
```

Tunable via env vars:
- `TEMPORAL_SAASPEC_MAX_DEPTH=N` — raise the BFS depth cap (default 4).
- `TEMPORAL_SAASPEC_NO_NEGATIVE_POLL=1` — skip the ~3s "a PAUSED activity must not dispatch" long
  poll (the dominant cost of deep walks); the per-edge state check still runs.
- `TEMPORAL_SAASPEC_COMPLETENESS=1` — also print reachable-but-unexercised cells (informational).
- `TEMPORAL_SAASPEC_WALK_STEPS=N` / `TEMPORAL_SAASPEC_WALK_SEED=N` / `TEMPORAL_SAASPEC_VERBOSE=1` —
  random-walk steps per config (default 200), RNG seed (default 1, logged), and per-step logging.

### Wall-clock directed traces (real timers)

Where the traversal is exhaustive over RPCs, wall-clock behavior is also checked at tier 3 by directed
traces — a scripted event sequence run once on one activity, checked against the model at every step,
where a timeout or a start-delay/backoff window is configured short and waited out so each real wait
is paid once. These are the `*_Declarative` tests.

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/Test.*_Declarative' -count=1 -v ./tests/
```

### Graph tools
```bash
go run ./chasm/lib/activity/model/cmd/graph                          # counts+nodes+edges, tier-3 configs
go run ./chasm/lib/activity/model/cmd/graph -show skeleton           # status-level transition relation
go run ./chasm/lib/activity/model/cmd/graph -tier 2 -show counts
go run ./chasm/lib/activity/model/cmd/graph -config 1 -show nodes,edges
```
Flags: -tier {2,3}, -config N (index into the tier's set, default all), -show (comma list of counts,nodes,edges,skeleton).
