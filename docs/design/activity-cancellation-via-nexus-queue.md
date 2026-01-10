# Activity Cancellation via Worker Nexus Queue

## Overview

Today, activity cancellation requires users to enable activity heartbeat. This is an opt-in feature and is also billable. As a result, many users don't enable it leading to a degraded experience. This design enables activity cancellation via the per-worker nexus queue (created as part of worker heartbeat infrastructure). When a workflow is cancelled, the history shard notifies workers about activities to cancel via this existing channel, removing the dependency on activity heartbeat for cancellation.

## Solution

Use a dedicated **Worker Control Nexus service** as a control channel to push cancellation notifications directly to workers. This is separate from:
- **Worker heartbeat** (`RecordWorkerHeartbeat`): Unidirectional RPC, fast, fire-and-forget
- **Nexus operations** (`PollNexusTaskQueue`): For handling incoming Nexus operation requests

The Worker Control service provides long-polling for pushed control tasks (cancellations, config updates, etc.).

### Architecture

```
┌─────────────────────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│         History Shard           │     │    Matching     │     │     Worker      │
│                                 │     │                 │     │                 │
│ ActivityInfo.WorkerNexusQueueId │     │                 │     │ PollNexusTask   │
│                                 │     │                 │◄────│ (control endpt) │
│ Cancel Request                  │     │                 │     │                 │
│       │                         │     │                 │     │                 │
│       ▼                         │     │                 │     │                 │
│ ┌───────────────────┐           │     │                 │     │                 │
│ │ Outbound Queue    │           │     │                 │     │                 │
│ │ (persisted task)  │───────────┼────►│ Sync-match ─────┼────►│ StartOperation  │
│ │                   │           │     │                 │     │ (cancel_activity│
│ │ [retry on fail]   │◄──────────┼─────│ (error/timeout) │     │                 │
│ └───────────────────┘           │     │                 │     │                 │
└─────────────────────────────────┘     └─────────────────┘     └─────────────────┘
```

**Key insight:** Persistence and retry logic is in **history's outbound queue** (not transfer queue), like Nexus operations. Matching uses sync-match only. If no worker is polling, history retries with backoff. Outbound queue provides isolation for slow/unavailable workers via multi-cursor.

### Flow Summary

| Step | Component | Action |
|------|-----------|--------|
| 1 | History | On `RecordActivityTaskStarted`, store `WorkerNexusQueueId` in `ActivityInfo` |
| 2 | History | On activity cancel request, generate `ActivityCancelNotificationTask` (persisted in outbound queue) |
| 3 | History | Outbound executor calls `AddWorkerControlTask` to matching |
| 4 | Matching | Sync-match: if worker polling control endpoint → deliver as `StartOperationRequest` |
| 5 | Matching | If no worker polling → return error → history retries with backoff |
| 6 | Worker | Receives `StartOperationRequest` with `operation=cancel_activity` → SDK cancels the activity |

**Note:** Worker heartbeat (`RecordWorkerHeartbeat`) and worker control are separate:
- Heartbeat: Unidirectional RPC, fast, fire-and-forget
- Control: Nexus operations via `PollNexusTaskQueue` on worker-specific endpoint

### API

#### 1. ActivityInfo Enhancement

Store `WorkerNexusQueueId` in `ActivityInfo` during `RecordActivityTaskStarted`.

```protobuf
message ActivityInfo {
    // ... existing fields ...
    string worker_nexus_queue_id = 51;  // Set on activity start
}
```

#### 2. Worker Control Task

A batched request containing multiple control tasks. This allows efficient delivery when
multiple activities on the same worker need cancellation (e.g., workflow cancelled with 10 activities).

```protobuf
// Container for batched control tasks - one Nexus operation can deliver multiple tasks
message WorkerControlPayload {
    repeated WorkerControlTask tasks = 1;
}

message WorkerControlTask {
    string task_id = 1;  // For deduplication
    oneof task {
        CancelActivityTask cancel_activity = 2;
        UpdateConfigTask update_config = 3;
        // Future: more control operations
    }
}

message CancelActivityTask {
    // namespace_id comes from AddWorkerControlTaskRequest
    temporal.api.common.v1.WorkflowExecution workflow_execution = 1;
    int64 scheduled_event_id = 2;
    string activity_id = 3;
    string reason = 4;
}
```

#### 3. Worker Control via Nexus Operations

Worker control uses existing **Nexus operation infrastructure** - no new RPC needed:

- Worker polls `PollNexusTaskQueue` on their control endpoint
- Control tasks arrive as `StartOperationRequest` with specific operation names
- Payload contains the control task data

```
Queue: /temporal-sys/worker-commands/{namespace}/{worker_grouping_key}

StartOperationRequest:
  service = "temporal.worker.control"
  operation = "execute"
  payload = serialized WorkerControlPayload (contains batch of tasks)
```

**Payload structure:**

```protobuf
WorkerControlPayload {
    tasks: [
        WorkerControlTask { cancel_activity: CancelActivityTask{...} },
        WorkerControlTask { cancel_activity: CancelActivityTask{...} },
        // ... more tasks in batch
    ]
}
```

This batched approach efficiently handles scenarios like workflow cancellation
where multiple activities on the same worker need to be cancelled simultaneously.

**Benefits:**
- Uses existing `PollNexusTaskQueue` infrastructure
- No new RPC definition needed
- Follows existing Nexus patterns (see [api#622](https://github.com/temporalio/api/pull/622))

#### 4. Matching Service API

```protobuf
// Sync-match only. Returns error if no worker polling.
message AddWorkerControlTaskRequest {
    string namespace_id = 1;
    string worker_nexus_queue_id = 2;
    WorkerControlTask control_task = 3;
}
```

## Worker Nexus Queue Identification

The worker nexus queue is identified by the `WorkerInstanceKey` from worker heartbeat:

```
Queue Name Format: /temporal-sys/worker-commands/{namespace}/{worker_grouping_key}
```

This queue is:
- Created implicitly when worker first polls with heartbeat
- Automatically cleaned up when worker goes inactive
- Partitioned by worker, not by task queue

## Failure Modes & Mitigations

| Scenario | Impact | Mitigation |
|----------|--------|------------|
| Worker not polling when dispatch attempted | Sync-match fails | History retries with backoff until worker polls |
| History shard failover | Outbound task may be re-executed | Idempotent cancel (cancelling already-cancelled activity is no-op) |
| Worker crashes | Activity gone with worker | No action needed; worker liveness reschedules or activity times out |
| Matching partition restart | No impact | Matching is stateless for control tasks; history retries |

## Backward Compatibility

Workers must advertise support for control tasks. History only generates cancel notification tasks for capable workers.

**Capability advertisement:**
1. Worker includes `supports_control_tasks: true` in poll request
2. `RecordActivityTaskStarted` stores this flag in `ActivityInfo`
3. When generating cancel task, check flag — skip if worker doesn't support

```protobuf
message ActivityInfo {
    // ... existing fields ...
    string worker_nexus_queue_id = 51;
    bool worker_supports_control_tasks = 52;  // NEW
}
```

## Metrics

| Metric | Description |
|--------|-------------|
| `activity_cancel_notification_latency` | Time from cancel request to worker notification |
| `activity_cancel_notification_failures` | Failed delivery attempts |
| `worker_control_task_poll_count` | Control tasks delivered per poll |

## Future Extensions

This infrastructure enables:
1. **Config hot reload:** Push configuration changes to workers
2. **Graceful shutdown:** Notify workers to drain before termination
3. **Priority changes:** Dynamically adjust worker priority
4. **Rate limiting:** Push dynamic rate limits to workers
5. **Feature flags:** Toggle worker features without restart

## Open Questions

1. **Matching behavior:** Sync-match only (like nexus dispatch)
   - Matching does NOT persist control tasks
   - If no worker polling, matching returns error
   - History's outbound queue handles persistence and retries with backoff
   - This follows the same pattern as nexus operation dispatch
   
2. **Deduplication:** How to handle duplicate deliveries on retry?
   - Control tasks include unique `task_id`
   - Workers track recently processed task IDs (in-memory, 5 min window)
   - Duplicate deliveries are idempotent (cancel already-cancelled activity is no-op)

3. **Task expiry:** When should history stop retrying?
   - Recommendation: Stop retrying when activity completes (check mutable state before dispatch)
   - Or: Match activity schedule-to-close timeout

## Implementation Milestones

1. **M1:** Add `worker_nexus_queue_id` to ActivityInfo and populate on task start
2. **M2:** Implement control task protos (`CancelActivityTask`, etc.)
3. **M3:** Implement matching API for worker control endpoint dispatch
4. **M4:** Generate cancel notification tasks on activity cancellation
5. **M5:** SDK integration for handling control operations via `StartOperationRequest`
6. **M6:** Metrics and observability

---

## Appendix: Implementation Details

### A.1 Record Nexus Queue ID on Activity Start

**Location:** `RecordActivityTaskStarted` (history service)

```go
// In service/history/api/recordactivitytaskstarted/api.go
func recordActivityTaskStarted(...) {
    // ... existing logic ...
    
    // Extract worker nexus queue ID from poll request
    workerNexusQueueId := request.PollRequest.GetWorkerNexusQueueId()
    
    mutableState.UpdateActivity(ai.ScheduledEventId, func(ai *persistencespb.ActivityInfo, _ historyi.MutableState) error {
        ai.StartedEventId = common.TransientEventID
        ai.StartedTime = timestamppb.New(ms.timeSource.Now())
        ai.StartedIdentity = identity
        ai.WorkerNexusQueueId = workerNexusQueueId  // NEW
        return nil
    })
}
```

### A.2 Generate Cancel Task on Activity Cancellation

**Location:** `ApplyActivityTaskCancelRequestedEvent` (mutable state)

```go
// In service/history/workflow/mutable_state_impl.go
func (ms *MutableStateImpl) ApplyActivityTaskCancelRequestedEvent(event *historypb.HistoryEvent) error {
    // ... existing logic ...
    
    ai.CancelRequested = true
    ai.CancelRequestId = event.GetEventId()
    
    // NEW: Generate task to notify worker via nexus queue
    if ai.WorkerNexusQueueId != "" && ai.StartedEventId != common.EmptyEventID {
        ms.taskGenerator.GenerateActivityCancelNotificationTask(ai)
    }
    
    return nil
}
```

**Task Generator:**

```go
func (g *TaskGenerator) GenerateActivityCancelNotificationTask(ai *persistencespb.ActivityInfo) error {
    task := &tasks.ActivityCancelNotificationTask{
        WorkflowKey:        g.workflowKey,
        ScheduledEventID:   ai.ScheduledEventId,
        ActivityID:         ai.ActivityId,
        WorkerNexusQueueId: ai.WorkerNexusQueueId,
    }
    return g.AddOutboundTask(task)  // Use outbound queue for external dispatch
}
```

### A.3 Process Cancel Notification Task

**Location:** Outbound queue task executor

```go
func (e *outboundQueueActiveTaskExecutor) processActivityCancelNotificationTask(
    ctx context.Context,
    task *tasks.ActivityCancelNotificationTask,
) error {
    cancelTask := &workerpb.CancelActivityTask{
        NamespaceId:      task.NamespaceID,
        WorkflowExecution: &commonpb.WorkflowExecution{
            WorkflowId: task.WorkflowID,
            RunId:      task.RunID,
        },
        ScheduledEventId: task.ScheduledEventID,
        ActivityId:       task.ActivityID,
        Reason:           "workflow_cancelled",
    }
    
    // Sync-match only. If no worker polling, returns error and history retries.
    return e.matchingClient.AddWorkerControlTask(ctx, &matchingservice.AddWorkerControlTaskRequest{
        NamespaceId:        task.NamespaceID,
        WorkerNexusQueueId: task.WorkerNexusQueueId,
        ControlTask: &workerpb.WorkerControlTask{
            TaskId: uuid.New().String(),
            Task: &workerpb.WorkerControlTask_CancelActivity{
                CancelActivity: cancelTask,
            },
        },
    })
}
```

### A.4 Matching Delivers to Worker

**Location:** Matching service

```go
func (e *matchingEngineImpl) AddWorkerControlTask(
    ctx context.Context,
    request *matchingservice.AddWorkerControlTaskRequest,
) (*matchingservice.AddWorkerControlTaskResponse, error) {
    partition := getWorkerControlPartition(request.WorkerNexusQueueId)
    
    // Sync-match only (like DispatchNexusTask)
    // Returns error if no worker polling - history will retry
    return partition.AddControlTask(ctx, request.ControlTask)
}
```

### A.5 Full Proto Definitions

```protobuf
// temporal/api/worker/v1/message.proto

message WorkerControlTask {
    string task_id = 1;
    google.protobuf.Timestamp created_time = 2;
    
    oneof task {
        CancelActivityTask cancel_activity = 10;
        UpdateConfigTask update_config = 11;
        DrainWorkerTask drain_worker = 12;
    }
}

message CancelActivityTask {
    temporal.api.common.v1.WorkflowExecution workflow_execution = 1;
    int64 scheduled_event_id = 2;
    string activity_id = 3;
    string reason = 4;
}

message UpdateConfigTask {
    map<string, google.protobuf.Any> config = 1;
}

message DrainWorkerTask {
    google.protobuf.Duration drain_timeout = 1;
    string reason = 2;
}
```

```
// Worker Control uses existing Nexus StartOperationRequest
// Queue: /temporal-sys/worker-commands/{namespace}/{worker_grouping_key}
// Worker polls via PollNexusTaskQueue, receives StartOperationRequest

StartOperationRequest:
  service = "temporal.worker.control"
  operation = "cancel_activity" | "update_config" | "drain_worker"
  payload = serialized proto (CancelActivityTask, UpdateConfigTask, etc.)
```

```protobuf
// temporal/server/api/persistence/v1/executions.proto

message ActivityInfo {
    // ... existing fields ...
    string worker_nexus_queue_id = 51;
}
```

```protobuf
// temporal/server/api/matchingservice/v1/request_response.proto

message AddWorkerControlTaskRequest {
    string namespace_id = 1;
    string worker_nexus_queue_id = 2;
    temporal.api.worker.v1.WorkerControlTask control_task = 3;
}

message AddWorkerControlTaskResponse {
}
```
