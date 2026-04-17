# Worker Commands

Worker commands are server-initiated instructions sent to workers via Nexus. Each worker process has a dedicated control task queue that it polls via Nexus RPC. A single control queue serves one or more workers running within the same process. This enables the server to push actions to workers without relying on heartbeats or long-poll cycles.

```mermaid
graph LR
    subgraph History
        A[Trigger] -->|1| B[Generate Task]
        B -->|2| C[Outbound Queue]
        C -->|3| D[Dispatch]
    end
    subgraph Matching
        E[Control Queue]
    end
    D -->|4| E
    E -->|8| D
    E ~~~ Worker
    subgraph Worker
        G[Poller] -->|6| F[Execute]
    end
    G -.->|5| E
    F -.->|7| E
```

### History

1. **Trigger** -- The server decides whether to initiate a worker command based on a state change (e.g., a user requesting activity cancellation).
2. **Generate Task** -- The trigger logic creates one or more `WorkerCommand` protos and calls `GenerateWorkerCommandsTasks`, which persists them as an outbound task.
3. **Outbound Queue** -- The outbound queue executor picks up the task.
4. **Dispatch** -- The dispatcher sends a `DispatchNexusTask` RPC to matching. This is synchronous — matching holds the request until a worker responds or the request times out.

### Matching

5. **Control Queue** -- Matching holds the command on the control queue until a worker polls it. If no worker is polling, the request times out. There is currently no way to distinguish a permanently gone worker from a temporarily slow one, so the dispatcher simply retries up to a maximum number of attempts before dropping the task. This is acceptable because worker commands are best-effort.

### Worker

6. **Poller** -- The worker's poller picks up the command from the control queue.
7. **Execute** -- The worker executes the command and sends the response back to matching.
8. **Response** -- Matching forwards the response back to the dispatcher in history.

## Adding a new command

1. **Define the proto** -- Add a new variant to `WorkerCommand` in [`message.proto`](https://github.com/temporalio/api/blob/43b4618e84611e594929ac60c970ec163e85c17a/temporal/api/worker/v1/message.proto#L198-L207) (api repo).
2. **Add a trigger** -- Add server-side logic to detect the condition, create the command, and call `GenerateWorkerCommandsTasks` (see [`task_generator.go`](https://github.com/temporalio/temporal/blob/c229fcb98ac88f62aec741212aa3797071057dff/service/history/workflow/task_generator.go#L582-L597)).
3. **Handle in the SDK** -- Add a handler for the new command variant in the SDK's worker command executor.

Dispatch ([`worker_commands_task_dispatcher.go`](../../service/history/worker_commands_task_dispatcher.go)) and serialization ([`task_serializers.go`](../../common/persistence/serialization/task_serializers.go)) are command-agnostic and do not require changes.

## Examples

### Activity cancellation

When a user requests activity cancellation, the server generates a cancel command targeting the worker's control queue (stored in `ActivityInfo` at activity start). The worker receives the command and cancels the running activity directly.

- **History**: Workflow task completed handler detects a pending cancellation, creates the command, and calls `GenerateWorkerCommandsTasks`.
- **Matching**: Handled by existing infrastructure — no command-specific logic needed.
- **Worker**: The worker's command executor matches the cancel command to the running activity and cancels it.
