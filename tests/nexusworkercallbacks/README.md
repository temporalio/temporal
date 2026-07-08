# Nexus Worker Callbacks

Nexus worker callbacks let a developer attach a **completion callback** to a standalone Nexus
operation (a "SANO" — Standalone Nexus Operation, started via `StartNexusOperationExecution`
rather than from within a workflow). The caller can still await the SANO's result as usual, but
when a worker callback is attached, Temporal will — once the SANO reaches a terminal state —
start a *second* Nexus operation against a well-known service/operation on a task queue of the
caller's choosing, delivering the SANO's outcome to a waiting worker.

This is the counterpart to the existing HTTP "outbound" callback (which POSTs a completion to a
URL). Instead of an HTTP round-trip through the frontend, the worker callback is delivered
directly to a task queue via `MatchingService.DispatchNexusTask`, which is more efficient.

> ⚠️ This is an in-progress feature. See [Remaining / unimplemented work](#remaining--unimplemented-work).

## Well-known completion contract

The dispatched Nexus operation always targets a fixed service and operation name (defined
server-side in `chasm/lib/callback/invocable_nexusworker.go`):

| | Value | Constant |
|---|---|---|
| Service | `temporal.nexus.v1.CompletionService` | `callback.CompletionServiceName` |
| Operation | `OnComplete` | `callback.CompletionOperationName` |

The operation input is a `nexuspb.OnCompleteHandlerInput`
(`go.temporal.io/api/nexus/v1`, in `events.pb.go`) with:

- `Outcome` — a oneof of `Success` (`*commonpb.Payload`, the SANO's result) or `Failure`
  (`*failurepb.Failure`, the converted operation error).
- `SourceContext` — a `*commonpb.Payload` of user-supplied data provided at the SANO callsite.
- `SourceOperation` — metadata about the source operation (endpoint/service/operation/token/links).
  **Not yet populated** (see below).

## Where the code lives

| Path | Role |
|---|---|
| `chasm/lib/nexusoperation/` | Runs the SANO. Attaches completion callbacks (`Operation.addCompletionCallbacks`) and schedules them on terminal states (`Operation.scheduleCompletionCallbacks` → `callback.ScheduleStandbyCallbacks`). Exposes the SANO outcome via `Operation.GetNexusCompletion` (implements `callback.CompletionSource`). |
| `chasm/lib/callback/` | The callback CHASM component + state machine. `invocable_nexusworker.go` holds the `NexusWorker` variant logic: it builds the `OnCompleteHandlerInput` (`buildCompletionInput`) and dispatches via matching (`Invoke`). |
| `tests/nexusworkercallbacks/` | This integration test and its helpers. |
| `tests/nexusworkercallbacks/fauxsdk/` | Stand-in for SDK API that does not exist yet: `AttachWorkerCallback` builds the `commonpb.Callback` and appends it to `StartNexusOperationOptions.HackRawCompletionCallbacks`. |
| `tests/nexusworkercallbacks/caller/` | The "caller" worker. Registers the `CompletionService`/`OnComplete` handler that receives the callback and records what it got. |
| `tests/nexusworkercallbacks/handler/` | The "handler" worker. Implements the actual SANO (`add`) that the caller invokes. |

## Data flow

```
caller worker                 Temporal server (CHASM)                 caller worker
─────────────                 ───────────────────────                 ─────────────
ExecuteOperation(add) ─────▶  nexusoperation.Operation (SANO)
  + attached callback           runs "add" against handler worker
                                        │
                                SANO reaches terminal state
                                        │
                                scheduleCompletionCallbacks
                                        │
                                callback.Callback state machine
                                (invocableNexusWorker.Invoke)
                                        │
                                DispatchNexusTask ────────────────▶  OnComplete handler runs
                                (temporal.nexus.v1.CompletionService)   (records the outcome)
```

1. **User attaches the worker callback.** At the SANO callsite the caller specifies which task
   queue should receive the completion and an opaque "source context" blob.
   *Today this is done by `fauxsdk.AttachWorkerCallback`; a real SDK surface is TBD.*

2. **Callback is encoded as a `commonpb.Callback` `NexusWorker` variant.** The
   `Callback_NexusWorker` proto carries only `taskqueue_name` and `source_context` (the endpoint /
   service / operation fields were removed). It is appended to the request's completion callbacks
   (`StartNexusOperationExecutionRequest.CompletionCallbacks`).

3. **SANO execution begins in CHASM** (`chasm/lib/nexusoperation`). During initialization,
   `Operation.addCompletionCallbacks` converts each `commonpb.Callback` into a child
   `callback.Callback` component and stores it in `Operation.Callbacks`
   (`chasm.Map[string, *callback.Callback]`), initially in the `STANDBY` state.

4. **SANO reaches a terminal state.** On success/failure/cancel/terminate/timeout, the operation
   state machine calls `Operation.scheduleCompletionCallbacks`, which calls
   `callback.ScheduleStandbyCallbacks`. This transitions each callback `STANDBY → SCHEDULED` and
   enqueues a CHASM invocation task on the outbound queue.

5. **The callback state machine executes.** The invocation task loads the callback and, for the
   `NexusWorker` variant, runs `invocableNexusWorker.Invoke` (`invocable_nexusworker.go`). It:
   - reads the SANO's terminal outcome back through `Operation.GetNexusCompletion`
     (the `callback.CompletionSource`);
   - builds the `nexuspb.OnCompleteHandlerInput` (`buildCompletionInput`), wrapping the outcome
     and the user's `source_context`;
   - encodes it as a `commonpb.Payload` via the standard Temporal payload format (json/protobuf).

6. **MatchingService dispatches the Nexus task.** `Invoke` calls
   `MatchingService.DispatchNexusTask` with the callback's `taskqueue_name`, a `StartOperation`
   request for `CompletionService`/`OnComplete`, and the encoded input. Matching blocks until a
   worker sync-matches the task and its `Start` handler returns, then returns the outcome.

   - **Timeout:** the whole dispatch is bounded by `callback.request.timeout` (default **10s**,
     dynamic-configurable per destination). The worker's `Start` handler must return within that
     window. Starting an *async* operation is fine — only `Start` needs to return in time.
   - **Retries & circuit breaking:** transient failures (RPC error, upstream/worker timeout,
     retryable handler errors) return a retryable result; the callback backs off and retries per
     policy. Retries are surfaced as `DestinationDownError` (`invocableNexusWorker.WrapError`) so
     the outbound queue's per-destination circuit breaker can trip after repeated failures.
     Permanent failures (e.g. an `OperationError` from the worker) fail the callback terminally.

7. **The worker receives the Nexus task.** The caller worker's SDK runtime receives the task and
   routes it to the registered `CompletionService`/`OnComplete` handler
   (`tests/nexusworkercallbacks/caller/nexus.go`).

8. **The worker callback runs.** The `OnComplete` handler is invoked with the decoded
   `OnCompleteHandlerInput` and records it. In this test that's how we assert the callback fired
   and carried the right data.

## Remaining / unimplemented work

- **SDK surface (step 1).** There is no real client API for attaching a worker callback. The test
  uses `fauxsdk.AttachWorkerCallback` plus the `StartNexusOperationOptions.HackRawCompletionCallbacks`
  escape hatch. A first-class SDK type that hides the raw `commonpb.Callback` is TBD.

- **Custom decoding of the input payload (⏭ coming next).** This is the big one and is **not yet
  implemented.** The `OnCompleteHandlerInput` envelope is generated by the Temporal *server*, so it
  has **not** passed through the caller's payload codec (encryption/compression interceptors). The
  worker must therefore *not* run its codec over the outer envelope, or it would corrupt it.
  However, the payloads *nested inside* the outcome / source context originate from user code and
  **do** need decoding. The plan is to set a header on the outer `commonpb.Payload` marking it as
  "do not codec the envelope, but do decode the inner `commonpb.Payload`s". Until this lands, worker
  callbacks only work correctly when no custom payload codec is configured.

- **`SourceOperation` metadata.** `OnCompleteHandlerInput.SourceOperation`
  (endpoint/service/operation/token/links) is left `nil` — see the `TODO` in
  `invocableNexusWorker.buildCompletionInput`. The source operation identity is not carried on the
  trimmed `Callback_NexusWorker` proto, so there is nothing to populate it from yet.

- **Outbound-queue destination routing (`BUG`).** `chasm/lib/callback/statemachine.go` derives the
  task destination from the (nonexistent) Nexus URL for the `NexusWorker` variant, yielding a bogus
  `"://"` destination. This misroutes outbound-queue grouping and per-destination circuit-breaker
  isolation; it should be derived from `taskqueue_name`. See the `BUG:` comments in that file.

- **Callback links.** `fauxsdk.AttachWorkerCallback` sets no `Links`; ideally the callback links
  back to the SANO being created in the same request.

- **Enablement config.** There is no dedicated dynamic-config flag to enable/disable worker
  callbacks per namespace (see the `TODO` in the test's `newEnv`). The test currently piggybacks on
  `EnableChasm` / `EnableCHASMCallbacks` / the nexusoperation flags.

- **Handler API.** The caller registers via `nexus.NewSyncOperation`; the intended API is
  `temporalnexus.NewTemporalOperation(...)` (see the `TODO` in `caller/nexus.go`).

## Running the test

```
go test ./tests/nexusworkercallbacks -v -count=1 -tags=test_dep
```
