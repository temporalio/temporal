# Worker Liveness Tracking

## Overview

The Worker CHASM component tracks the health and availability of Temporal workers through periodic heartbeats. It detects inactive workers and reschedules their activities to maintain workflow execution continuity.

## Architecture

### Components

1. **Worker** (`worker.go`) - CHASM entity representing a worker and its persistent state
2. **State Machine** (`statemachine.go`) - Manages worker lifecycle transitions
3. **Task Executors** (`executors.go`) - Handle lease expiry operations

### Key Concepts

- **Worker**: A unique instance of a worker process identified by `WorkerInstanceKey`
- **Lease**: Period for which a worker should be considered alive
- **Heartbeat**: Periodic signal from worker to extend its lease
- **Conflict Token**: Sequence number to enforce ordering and detect lost heartbeat responses
- **Activity Binding**: Association between workers and their assigned activities

## Worker Lifecycle

### States

```
ACTIVE ──────► INACTIVE (terminal)
                   │
                   └──► entity deleted by CHASM
```

#### ACTIVE
- Worker is sending heartbeats regularly
- Lease is being renewed for all activities that are currently bound to it

#### INACTIVE
- Worker lease has expired (no heartbeats received within lease duration)
- Activities are rescheduled to other workers
- **Terminal state**: Entity is deleted by CHASM framework
- Heartbeats in this state return `WORKER_INACTIVE` error

### State Transitions

#### ACTIVE → INACTIVE (Lease Expiry)
**Trigger**: `LeaseExpiryTask` fires when lease deadline is reached  
**Actions**:
- Mark worker as INACTIVE (terminal)
- CHASM framework deletes the entity
- Activities bound to this worker are rescheduled

## Heartbeat Protocol

### Token-Based Sequencing

Each heartbeat response includes an opaque token. The client must include this token in subsequent heartbeats. This enables detection of:

1. **Lost responses**: Server processed heartbeat but client didn't receive the new token
2. **Out-of-order delivery**: Network reordering caused heartbeats to arrive out of sequence. This should not happen since the worker is supposed to send only one heartbeat at a time.

### Error Types

| Error | Meaning | Client Action |
|-------|---------|---------------|
| `WORKER_INACTIVE` | Lease expired, terminal | Re-register with new `WorkerInstanceKey` |
| `TOKEN_MISMATCH` | Token doesn't match (lost response) | Use token from error, continue heartbeating |

### Client Behavior

1. **On success**: Update local token from response, clear pending deltas
2. **On `WORKER_INACTIVE`**: Re-register with a new `WorkerInstanceKey`
3. **On `TOKEN_MISMATCH`**: Update local token from error, keep pending deltas for next heartbeat

### Important Constraints

- Client must wait for each heartbeat response before sending the next
- **Deltas are idempotent**: Server handles duplicate bind/unbind operations gracefully

## Activity Rescheduling for Orphaned Task

Worker liveness alone cannot detect all activity failures. Consider:

1. Worker polls Matching for activity task
2. Matching calls History's `RecordActivityTaskStarted`
3. Activity marked as STARTED
4. **Poll times out** — worker never receives the task

The worker is still alive (heartbeating normally), but this specific activity is orphaned. Worker liveness cannot detect this because the worker doesn't know about the task.

### Solution: Activity Verification Timer
At `RecordActivityTaskStarted`, store `WorkerInstanceKey` in ActivityInfo and schedule a verification timer.

When the timer fires:

1. **Check activity state locally** — if already completed/failed/timed out, discard timer (no RPC needed)
2. **Query worker entity** — if activity still running, check worker state:

| Worker State | Activity Bound? | Action |
|--------------|-----------------|--------|
| ACTIVE | Yes | Discard timer, rely on worker lease expiry |
| ACTIVE | No | Reschedule (poll timeout or binding never sent) |
| INACTIVE | - | Reschedule |
| Not found | - | Reschedule |

 Note: Set the verification timer longer than typical short activities (e.g., 2 minutes) so most complete before it fires, avoiding the cross-shard RPC.

### Design Alternative: Push Notification (Not Chosen)

An alternative approach is to notify the activity when the worker binds it:

```
Worker heartbeat → Worker entity → RPC to Activity → Set "bound" flag
```

This was **not chosen** because it requires a cross-shard **write** to workflow mutable state. The pull approach only requires a cross-shard **read**, which is simpler and has a natural fail-safe (if query fails, reschedule).

## Heartbeat Flow

### End-to-End Process

```
Worker Process ──► Frontend Service ──► History Service ──► CHASM Worker Component
      │                    │                   │                      │
   [heartbeat+token]   [forward]           [update]              [validate token]
      │                    │                   │                      │
      └────────────────────┴───────────────────┴──────────────────────┘
                                    [new token returned]
```

### 1. Worker Process
- Sends periodic heartbeats with:
  - `WorkerInstanceKey`: Unique identifier for worker process
  - `token`: Opaque token from previous response (nil for first heartbeat)
  - `lease_duration`: Requested lease duration (e.g., 60 seconds)
  - `bound_activities`: Activities newly bound since last heartbeat (delta)
  - `unbound_activities`: Activities no longer bound since last heartbeat (delta)

### 2. Frontend Service
- Receives heartbeat from worker
- Forwards to History service via `RecordHeartbeat` RPC

### 3. History Service
- Processes heartbeat through CHASM handler
- Validates token matches current state
- Calculates lease deadline: `current_time + lease_duration`
- Updates Worker component, generates new token

### 4. CHASM Worker Component
- Validates token (returns `TOKEN_MISMATCH` if stale)
- Returns `WORKER_INACTIVE` if worker is in terminal state
- Applies activity binding deltas idempotently
- Schedules `LeaseExpiryTask` for the new deadline
- Returns new token in response

## Configuration

- `Lease duration`: Configurable per heartbeat request. Default: 60 seconds.

## Error Handling

### Network Partitions
- Worker appears inactive due to connectivity loss
- Activities are rescheduled to maintain progress
- Worker must re-register with new `WorkerInstanceKey` when connectivity is restored

### Lost Responses
- Server processed heartbeat but response was lost (deadline exceeded)
- Client retries with stale token → receives `TOKEN_MISMATCH` with current token
- Client updates token and continues normally

### Clock Skew
- Server calculates lease deadline to avoid client clock issues

## Design Decisions

### Why INACTIVE is Terminal (No Resurrection)

1. **No functional benefit**: Activities are already rescheduled when entering INACTIVE, so resurrected worker has no activities anyway
2. **Simpler recovery**: Re-registering with new `WorkerInstanceKey` is simpler

### Why Token in Error Response

When `TOKEN_MISMATCH` occurs, the error includes the current valid token. This avoids:

1. **Extra round trip**: No need for separate `DescribeWorker` call
2. **Race conditions**: Token returned is guaranteed current at rejection time

### Future: Separate RegisterWorker API

For throttling worker registration (protecting against crash loops), a separate `RegisterWorker` API may be added. This allows:

- Rate limiting registration without affecting heartbeat throughput
- Different validation/capabilities for registration vs heartbeat

Current implementation uses heartbeat for both registration and lease renewal.

