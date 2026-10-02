# Standalone Activity Eager Start Plan

## Goal

Allow a caller of `StartActivityExecution` to request an eager first activity
attempt. When accepted, the server returns a worker-ready activity task directly
in the start response, avoiding the initial Chasm dispatch task and Matching
round trip.

The eager path must leave the standalone activity (SAA) in the same persisted
state that normal Matching dispatch would create after a worker accepts its
first task.

## Scope

This change is server-only. It covers the API contract and Temporal server
behavior needed to accept an eager-start request and return an eager activity
task. SDK support—including deciding when a local worker has capacity,
requesting eager start, handing the returned task to that worker, and samples—
is explicitly out of scope and will be handled as follow-up work by the SDK
team.

## Current lifecycle

```text
StartActivityExecution
  -> Chasm Activity created
  -> SCHEDULED + durable dispatch/timer tasks
  -> ActivityDispatchTask -> Matching.AddActivityTask
  -> worker poll -> RecordActivityTaskStarted
  -> STARTED -> worker completes, fails, or heartbeats
  -> retry: SCHEDULED; terminal: COMPLETED, FAILED, CANCELED, TERMINATED, or TIMED_OUT
```

The relevant implementation points are:

- `chasm/lib/activity/handler.go`: creates the execution and applies the initial
  `TransitionScheduled` transition.
- `chasm/lib/activity/statemachine.go`: owns state transitions and durable task
  creation.
- `chasm/lib/activity/tasks.go`: dispatches a valid scheduled attempt to Matching.
- `service/matching/matching_engine.go`: records the worker pickup and builds
  `PollActivityTaskQueueResponse`.
- `service/history/handler.go`: routes a standalone `RecordActivityTaskStarted`
  request to `Activity.HandleStarted` through Chasm.

## Proposed eager lifecycle

```text
StartActivityExecution(request_eager_execution=true)
  -> validate eligibility
  -> Chasm Activity created
  -> initialize attempt 1 and schedule durable lifetime timers
  -> atomically transition SCHEDULED -> STARTED
  -> return EagerActivityTask with a normal activity task token
  -> caller executes task directly

Subsequent activity attempts use normal SCHEDULED -> Matching dispatch. A
retry of the *start request* returns the existing execution without
re-delivering its eager task.
```

The worker-facing eager task must be equivalent to the response normally built
by Matching: same input, header, activity/run IDs, timeouts, attempt, component
reference, and attempt stamp in its task token.

### Idempotent eager-start retry lifecycle

The initial eager response is created only after `StartExecution` succeeds,
using the persisted execution in `result.ExecutionRef`. It is returned only
when `result.Created` is true. A same-request-ID retry returns the existing
execution without another eager task, even if the original first attempt is
still current. Unlike a workflow task, an activity can have external side
effects, so re-delivering an eager activity task could run user code twice.

```text
StartActivityExecution(request ID = R, eager = true)
  -> StartExecution succeeds with Created = true
  -> create and persist eager attempt 1 as STARTED
  -> return EagerActivityTask

Retry StartActivityExecution(request ID = R, eager = true)
  -> StartExecution returns the existing execution
  -> return the idempotent start response without EagerActivityTask

StartActivityExecution(request ID != R, conflict policy = USE_EXISTING)
  -> result.Created = false
  -> return the existing execution without EagerActivityTask
```

If the first eager response is unavailable, the started attempt is recovered by
the normal timeout and retry path; subsequent attempts are dispatched through
Matching. The initial response token represents the first logical attempt and
must pass the normal activity-token validation path.

## Implementation steps

1. Define and regenerate the API contract.

   - Add `request_eager_execution` to `StartActivityExecutionRequest`.
   - Add `eager_activity_task` to `StartActivityExecutionResponse`.
   - Update the pinned Temporal API dependency and regenerate code as required.
   - Confirm the final field names and numbers against the API repository before
     implementation.

2. Add eligibility and fallback policy.

   - Add a namespace-scoped dynamic configuration setting for standalone eager
     start.
   - Call `StartExecution` before constructing an eager task. Use
     `result.ExecutionRef`, rather than a precomputed reference, so its token
     identifies the execution actually persisted.
   - Return an eager task only for a newly created execution, when
     `result.Created == true`.
   - A distinct request that resolves through `USE_EXISTING` must return no
     eager task.
   - Do not eagerly start an activity with non-zero `start_delay`.
   - For ineligible requests, clear the effective eager request and use the
     existing scheduling path. Return a normal start response with no eager task.
   - Add accepted and denied metrics, including denial reasons.

3. Preserve the state-machine invariant atomically.

   - Generalize the initial scheduling event/body so eager initialization creates
     attempt 1, its stamp, dispatch time, and schedule-to-close timer without
     emitting an `ActivityDispatchTask` or schedule-to-start timer.
   - In the same Chasm creation transaction, apply the existing
     `SCHEDULED -> STARTED` behavior.
   - Persist start-to-close and heartbeat timers exactly as a Matching-started
     task does.
   - Do not rely on a later stale-task validation check as the primary mechanism;
     avoid creating the initial Matching dispatch task in the eager case.

4. Share start and task-response construction.

   - Extract the common state update and task-response data preparation currently
     split between `Activity.HandleStarted` and Matching's
     `createPollActivityTaskQueueResponse`.
   - Keep `HandleStarted` as the normal Matching entry point, but make eager
     start call the same transition and response-building helpers.
   - Inject the token serializer into the SAA history handler or another local
     dependency that owns the shared response builder.
   - Include the serialized Chasm component reference and current attempt stamp
     in the eager activity task token.
   - Make the response builder usable by the request-ID deduplication path: it
     must load the activity referenced by `result.ExecutionRef` and rebuild a
     response only for the still-current original eager attempt.

5. Keep post-start behavior unchanged.

   - Completion, failure, cancellation, heartbeat, pause, reset, and timeout
     paths continue to validate the existing activity task token.
   - Retryable worker failures and start-to-close/heartbeat timeouts transition
     back to `SCHEDULED` and dispatch through Matching.
   - Schedule-to-close remains an activity-lifetime timeout; it must still be
     installed for an eager attempt.

6. Implement idempotency and conflict behavior.

   - Let `StartExecution` perform request-ID deduplication and conflict/reuse
     resolution before any eager task is built.
   - When `result.Created == true`, construct the initial eager activity token
     from `result.ExecutionRef`.
   - When `result.Created == false`, including a request-ID retry or a
     `USE_EXISTING` conflict, return no eager task. Do not create an attempt,
     emit a dispatch task, or mutate activity state on this path.
   - Keep token validation unchanged. The initial response token must carry the
     persisted component reference and attempt stamp.

## Test plan

### State-machine tests

- Eager initialization enters `STARTED`, creates attempt 1, and records the
  expected started metadata.
- It creates start-to-close and heartbeat timers when configured.
- It does not create initial `ActivityDispatchTask` or schedule-to-start timer.
- A normal first attempt remains unchanged.

### Handler tests

- Accepted eager start returns an eager `PollActivityTaskQueueResponse`.
- The response token completes and heartbeats the SAA successfully.
- The token contains the activity's component reference and current attempt stamp.
- Disabled dynamic config and non-zero start delay fall back without an eager
  task.
- An immediate retry with the same request ID returns the existing execution
  without an eager task and without creating another attempt or invoking
  Matching.
- A distinct `USE_EXISTING` request returns the existing execution without an
  eager task.

### End-to-end tests

- Eager first attempt does not call `Matching.AddActivityTask`.
- Retrying an eager attempt sends the subsequent attempt through Matching.
- Completion, cancellation, start-to-close timeout, heartbeat timeout, and
  schedule-to-close timeout retain current behavior.
- A stale normal dispatch task cannot start or duplicate an eagerly started
  attempt.

## Failure modes and trade-offs

- **Lost start response:** a same-request-ID retry does not re-deliver the
  original eager task. Normal timeout and retry behavior recover delivery
  through Matching, without risking duplicate execution of the same activity
  attempt.
- **Concurrent dispatch:** atomically omitting the initial dispatch task avoids
  duplicate delivery and removes unnecessary queue work.
- **Worker crash after receipt:** normal start-to-close and heartbeat timers
  recover the attempt through existing retry behavior.
- **Load increase:** eager start moves first-attempt work from Matching to the
  synchronous start RPC. It reduces queue latency but increases start-response
  size and History/Chasm transaction work; configuration gating provides a safe
  rollout control.
- **Security:** reuse the normal task token format and validation path. Never
  introduce a special eager-only token that bypasses component-reference,
  namespace, attempt, or stamp checks.

## Verification

Run focused unit tests with `-tags test_dep`, then run the relevant integration
coverage if added.

After the server/API changes are available, perform a local latency comparison
with the standalone-activity sample at
`/Users/wenlonggu/Desktop/workspaces/samples-go`:

1. Build and run the local server from this checkout,
   `/Users/wenlonggu/Desktop/workspaces/temporal`, then configure the sample
   to use that local endpoint. The sample is an SDK client and does not import
   the server module directly.
2. Run the same SAA workload without eager start and record end-to-end
   start-to-running latency from the `StartActivityExecution` call until the
   activity begins running.
3. Enable eager start, run the identical workload against a local worker on the
   same task queue, and record the same latency measurement.
4. Compare the before/after results (at least p50 and p95) and verify that the
   eager first attempt has no Matching dispatch or poll. Keep worker capacity,
   task queue, payload, and repetition count equivalent between the two runs.

Finish with:

```text
make lint-code-fast
```

### Local direct-RPC result

On 2026-10-01, 200 paired local runs against the modified server measured the
time from `StartActivityExecution` until a worker-ready task was available.
Normal runs used a waiting `PollActivityTaskQueue`; eager runs read the task
from the start response. Each task was immediately completed after measurement.

| Path | p50 | p95 | p99 |
| --- | ---: | ---: | ---: |
| Normal | 2.372 ms | 58.654 ms | 88.855 ms |
| Eager | 1.588 ms | 5.028 ms | 7.362 ms |

This local result shows lower task-delivery latency and much tighter tail
latency for eager start. It is not a production end-to-end worker benchmark:
it excludes SDK activity-function execution and measures only server-side task
delivery.
