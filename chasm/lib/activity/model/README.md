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
- `conformance/` — static checks of the model against the product state-machine code, no server.

The SAA driver and model-checking engine that exercise this model against a real onebox server live
in `tests/` (`activity_standalone_utils.go`, `activity_standalone_spec_harness.go`,
`activity_standalone_spec_test.go`, and `activity_standalone_test.go`).

In the onebox commands below, `-count=1` skips the test cache and `-v` shows per-subtest logs. Prefix
with `TEMPORAL_TEST_LOG_LEVEL=ERROR TEMPORAL_TEST_LOG_STACKTRACE_LEVEL=off` to quiet logger noise.

## No server (~1s) — model unit tests + static conformance

Runs the `model_test.go` smoke tests (including the dispatch-delay requirements for start_delay and
retry backoff), `conformance.TestModelDecisionCoverage` (`Transition` is total over the RPC domain —
no unexpected panics), and `conformance.TestModelEdgesReachableInCode` (every status change the model
accepts is reachable via the code's declared transitions).

```bash
go test ./chasm/lib/activity/model/...
```

## RPC graph traversal + random walk (onebox)

`TestSpec` walks the model's reachable states against a real server. `RPCGraphTraversal` is a
breadth-first walk of every decided edge (deduped by fingerprint, depth-bounded); `RandomWalk` drives
one activity forward through randomly chosen events, reaching deep interaction sequences the bounded
traversal never visits. Both check the internal state, reject kind, heartbeat flags, Describe
projection, and task-invalidation stamps against the model at every step.

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

## Wall-clock traces (onebox, real timers)

Where the traversal is exhaustive over RPCs, wall-clock behavior is checked by directed **traces**
(`TestActivityTraces`): a scripted event sequence run once on one activity, checked against the model
at every step. A timeout firing or a start-delay/backoff window elapsing is realized by configuring it
short and waiting, so each real wait is paid once.

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestActivityTraces' -count=1 -v ./tests/
```

## Driver example (onebox)

`TestActivityDriveToState` demonstrates the model-free driver on its own: reach a state with a few DSL
events, then make ordinary assertions about details the model does not track.

```bash
go test -tags test_dep -run 'TestStandaloneActivityTestSuite/TestActivityDriveToState' -count=1 -v ./tests/
```
