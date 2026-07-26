# activity/model — a behavioral model of the CHASM activity archetype

`model` is a test-only, server-free description of how a CHASM activity *should* behave:
`Transition(cfg, state, event) -> Outcome` plus the state-derived response predictions
(`ExpectedHeartbeatFlags`, `ExpectedDescribe`). It is not runtime code and must never be imported by
the server binary. It is archetype-level, not tied to one product surface: the standalone-activity
(SAA) frontend driver checks a real server against it today; a workflow-activity driver over the same
model will let the two be checked for equivalence.

- `vocabulary.go` — the event alphabet (`Event`/`EventType`), start-time `Config`, and the
  observable-state projection (`AbstractState`, `Observed`, `Abstract`).
- `model.go` — the transition rules (`Transition`, `Initial`, the per-event functions) and the
  response predictors.
- `explore.go` — pure graph helpers shared by the explorers (`Fingerprint`, `Reachable`, `CellKey`,
  `NeedsToken`, `CarriesReqID`), which events can occur in a state (`Possible`: a clock event cannot
  occur unless its clock is running) and the trace check built on it (`ValidateTrace`), and the
  `String` methods for `EventType` and `Event`.
- `validate/` — static checks *validating the model* against the product state-machine code, no
  server. (Distinct from conformance testing below, which checks a running server against the model.)

The model is exercised at three tiers, cheapest first. Tiers 2 and 3 are conformance testing (a
running implementation vs the model); tier 1 is static model validation. All three use the *same* model.

This file is the point of truth for how to run any of it.

## Everything, in one go

Tiers 1 and 2 are ~1s each. The tier-3 leg needs `-tags test_dep` and takes a couple of minutes.

```bash
export TEMPORAL_TEST_LOG_LEVEL=ERROR TEMPORAL_TEST_LOG_STACKTRACE_LEVEL=off
go test -count=1 ./chasm/lib/activity/... &&                                   # tiers 1 and 2
go test -count=1 -tags test_dep -run TestActivityParityTestSuite ./tests/       # tier 3
```

A passing suite test prints nothing. Add `-v` and read the `--- PASS` / `--- FAIL` lines to see which
ran. `-count=1` skips the test cache.

## Tier 1 — no server (~1s): model unit tests + static model validation

`model_test.go` smoke tests (including the dispatch-delay requirements for start_delay and retry
backoff), `validate.TestModelDecisionCoverage` (`Transition` is total over the RPC domain — no
unexpected panics), and `validate.TestModelEdgesReachableInCode` (every status change the model
accepts is reachable via the code's declared transitions).

```bash
go test -count=1 ./chasm/lib/activity/model/...
```

## Tier 2 — in-process (~1s): model-conformance explorers over a real in-memory engine

`TestConformance` (package `activity`, `activity_conformance_test.go`) runs the BFS graph traversal and
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
go test -count=1 -run TestConformance ./chasm/lib/activity/
```

The same package holds `TestDispatchRouting`, which pins which physical queue an `ActivityDispatchTask`
lands on — a due dispatch must go to the transfer queue, since a timer task's fire time is floored at
`now + TimerProcessorMaxTimeShift` (~1s).

```bash
go test -count=1 -run TestDispatchRouting ./chasm/lib/activity/
```

## Tier 3 — onebox (real server, real timers)

Everything at tier 3 hangs off `TestActivityParityTestSuite` in package `tests`. The drivers are
`activity_standalone_driver.go` and `activity_workflow_driver.go`; the model-conformance engine is
`activity_standalone_conformance{,_test}.go`.

### SAA↔WFA parity — `activity_parity_test.go`

The urgent goal: prove the CHASM activity (SAA) behaves like the legacy workflow activity (WFA) at
their intersection. Because the model is ours, "SAA conforms to the model" does not prove "SAA matches
WFA"; equivalence is checked by *differential testing* — drive the same trace through a SAA and a WFA
activity with the parallel drivers and compare their user-visible activity info. There is no oracle:
each test's `expected` encodes how the product *should* behave, and both surfaces are asserted against
it, so a failure on either (or both) is useful information. Each `TestParity*` holds a
`WorkflowActivity` and a `StandaloneActivity` subtest asserting the same projection.

```bash
go test -count=1 -tags test_dep -run 'TestActivityParityTestSuite/TestParity' ./tests/
```

`activity_metrics_parity_test.go` does the same for metric emission and tag keys
(`TestWFASAAMetricsParity`).

### RPC graph traversal + random walk

`TestConformance` walks the model's reachable states against a real server over the full event alphabet,
including the operator commands. `RPCGraphTraversal` is a breadth-first walk of every decided edge
(deduped by fingerprint, depth-bounded); `RandomWalk` drives one activity forward through randomly
chosen events, reaching deep interaction sequences the bounded traversal never visits. Both check the
internal state, reject kind, heartbeat flags, Describe projection, and task-invalidation stamps
against the model at every step.

```bash
go test -count=1 -tags test_dep -run 'TestActivityParityTestSuite/TestConformance' ./tests/
```

Tunable via env vars:
- `TEMPORAL_SAASPEC_MAX_DEPTH=N` — raise the BFS depth cap (default 4).
- `TEMPORAL_SAASPEC_NO_NEGATIVE_POLL=1` — skip the ~3s "a PAUSED activity must not dispatch" long
  poll (the dominant cost of deep walks); the per-edge state check still runs.
- `TEMPORAL_SAASPEC_COMPLETENESS=1` — also print reachable-but-unexercised cells (informational).
- `TEMPORAL_SAASPEC_WALK_STEPS=N` / `TEMPORAL_SAASPEC_WALK_SEED=N` / `TEMPORAL_SAASPEC_VERBOSE=1` —
  random-walk steps per config (default 200), RNG seed (default 1, logged), and per-step logging.

### Wall-clock directed traces — `activity_standalone_traces_test.go`

Where the traversal is exhaustive over RPCs, wall-clock behavior is checked by directed traces — a
scripted event sequence run once on one activity, checked against the model at every step, where a
timeout or a start-delay/backoff window is configured short and waited out so each real wait is paid
once. These are the `*_Declarative` tests. They are standalone-activity only: the behavior they cover
has no workflow-activity counterpart, so there is nothing to compare against.

```bash
go test -count=1 -tags test_dep -run 'TestActivityParityTestSuite/Test.*_Declarative' ./tests/
```

### Driver self-tests — `activity_driver_selftest_test.go`

The drivers are test infrastructure, so they are themselves tested: that they report a scripted event
whose effect never arrived, and that they blame themselves rather than the product when their own
timing is at fault.

```bash
go test -count=1 -tags test_dep -run 'TestActivityParityTestSuite/TestSAADriver' ./tests/
```

## Graph tools (no server)

```bash
go run ./chasm/lib/activity/model/cmd/graph                          # counts+nodes+edges, onebox configs
go run ./chasm/lib/activity/model/cmd/graph -show skeleton           # status-level transition relation
go run ./chasm/lib/activity/model/cmd/graph -explorer engine -show counts
go run ./chasm/lib/activity/model/cmd/graph -config 1 -show nodes,edges
```

Flags: `-explorer {engine,onebox}` (the tier-2 and tier-3 explorers respectively), `-config N` (index
into that explorer's set, default all), `-show` (comma list of counts,nodes,edges,skeleton).
