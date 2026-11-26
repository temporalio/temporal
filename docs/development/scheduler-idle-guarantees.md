# Scheduler idle guarantee prototype

This branch adds tests and test-harness extensions on top of PR #13016. It does
not change production behavior or implement the stronger guarantees below.

## Properties

1. **Safe idle closure:** immediately before the real idle handler closes a
   non-sentinel schedule, every buffered start has a recorded RunId. A successful
   start or an explicit discard resolves an action; a running workflow need not
   have completed. Explicit schedule deletion is outside this property.
2. **A real close path:** after a committed transition, an advertised
   IdleCloseTime has a logical SchedulerIdleTask at that deadline, allowing for
   task timestamp precision. An unrelated visibility task or an obsolete
   physical timer does not satisfy this property.
3. **Eventual closure in a drained scenario:** after the test resolves the final
   action, advancing the clock through the idle deadline closes the schedule
   exactly once. This is a bounded scenario check, not a proof of liveness under
   arbitrary task delivery, external patches, or failures.
4. **Retention follows activity:** after a manual backfill executes now, its
   future calendar label must not extend retention beyond the recorded workflow
   start time plus IdleTime.

`observedIdleHandler` delegates validation and execution to the production idle
handler. It observes violations without preventing closure. The assertion is
made by the test goroutine, so a failed property cannot conceal the production
transition or strand a worker goroutine. A negative control checks that the
oracle rejects unresolved starts and accepts running workflows and sentinels.

The clean-state observer reads serialized snapshots without materializing
component fields. It checks both task presence and the advertised deadline,
including after validation removes tasks. The real invoker handler executes
against a mocked frontend, exercising result recording and regeneration.

## Passing scenarios

Run:

```sh
go test -tags test_dep ./chasm/lib/scheduler/...
```

The new `TestIdleGuarantees*` cases cover successful starts, permanent start
failure, a response reported lost followed by a retry with the same request ID,
an in-flight RPC while timers are processed within the grace period, historical
backfills, and explicit deletion. Success and failure scenarios also check
eventual closure. The successful workflow is left running to establish that
idle retention does not wait for workflow completion.

The gated RPC uses channels rather than sleeps. Timer processing happens only
after the invoker has finished reading state and entered the RPC, and result
recording happens only after the timer transition finishes. The framework clock
is controlled. The production invoker still records workflow start with wall
time; expectations use that recorded StartTime rather than an elapsed-time
threshold.

The frontend mock returns the same run on a retry. These tests verify request
identity and scheduler action accounting; they do not independently verify
frontend deduplication or persistence behavior.

## Executable specifications for remaining gaps

These tests intentionally fail against the current hotfix:

```sh
go test -tags test_dep,scheduler_contract ./chasm/lib/scheduler \
  -run '^TestIdleContract' -count=1
```

| Test | Expected diagnostic |
| --- | --- |
| `PendingStartsSurviveDeadline/invoker_delayed` | Idle closure abandons an unresolved start after the grace period. |
| `PendingStartsSurviveDeadline/generator_delayed` | A late generator queues eligible catchup work with an expired idle deadline. |
| `StartRPCOutlivesDeadline` | Idle closure occurs while the start RPC is still unresolved. |
| `UpgradePreservesIdleTimer` | New validation removes the sole old idle timer without a matching replacement. |
| `FutureBackfillDoesNotExtendRetention` | A future calendar label persists as LastEventTime and extends retention after the workflow starts now. |

The upgrade scenario generates the old timer with the switch disabled, commits
that state, and then enables the new calculation before delivering the timer.
It does not first run the fixed generator, so a replacement timer is genuinely
absent. This models the old/new calculation boundary on persisted state; it is
not a two-binary upgrade test or a reload into a fresh engine.

The separate build tag keeps the existing suite usable while preserving the
stronger contracts as failing assertions rather than asserting the bugs are
correct. Once a guarantee is implemented, move its test into the normal suite.

## Further extensions

The safety oracle covers unresolved actions still represented in BufferedStarts.
It does not detect an action silently omitted by generation or removed from the
buffer without a recorded reason. A separate action ledger, seeded from a finite
schedule's expected occurrences, would cover that broader conservation property.
Sequence generation could then vary timer delivery, retries, updates, backfills,
and reloads while checking each action ends in a start or an allowed discard.

Real-history-service integration tests are still needed for start RPC response
loss, closed-execution rejection, failover, and persistence/reload behavior.
The prototype does not claim those guarantees from an in-memory engine.
