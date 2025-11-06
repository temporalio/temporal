# Worker Liveness Tracking

## Overview

The Worker CHASM component tracks the health and availability of Temporal workers through periodic heartbeats. It detects inactive workers and reschedules their activities to maintain workflow execution continuity. The system handles cases where workers become unavailable due to network partitions, crashes, or other failures.

## Architecture

### Components

1. **Worker** (`worker.go`) - CHASM entity representing a worker and its persistent state
2. **State Machine** (`statemachine.go`) - Manages worker lifecycle transitions
3. **Task Executors** (`tasks.go`) - Handle lease expiry and cleanup operations
4. **RPC Handler** (`handler.go`) - Processes heartbeat requests from workers

### Key Concepts

- **Worker**: A unique instance of a worker process identified by `WorkerInstanceKey`
- **Lease**: Period for which a worker should be considered alive
- **Heartbeat**: Periodic signal from worker to extend its lease
- **Activity Binding**: Association between workers and their assigned activities

## Worker Lifecycle

### States

```
ACTIVE ──────► INACTIVE ──────► CLEANED_UP
   │              │                 │
   │              │                 │
   └──────────────┘                 │
      (resurrection)                │
                                    │
                              (terminal state)
```

#### ACTIVE
- Worker is sending heartbeats regularly
- Lease is being renewed for all activities that are currently bound to it

#### INACTIVE  
- Worker lease has expired (no heartbeats received)
- Activities are cancelled and rescheduled to other workers
- Worker can still reconnect (resurrection scenario due it temporary network partition)

#### CLEANED_UP
- Terminal state after cleanup grace period
- All the underlying state is cleaned up
- Heartbeats in this state return errors

### State Transitions

#### ACTIVE → INACTIVE (Lease Expiry)
**Trigger**: `LeaseExpiryTask` fires when lease deadline is reached
**Actions**:
- Mark worker as INACTIVE
- Schedule `WorkerCleanupTask` with configurable delay
- Create and schedule `ActivityRescheduleTask` for each bound activity
- Fan-out tasks to appropriate History service shards

#### INACTIVE → ACTIVE (Worker Resurrection)
**Trigger**: Heartbeat received from previously inactive worker
**Actions**:
- Update lease with new deadline
- Mark worker as ACTIVE
- Previous activities remain cancelled

#### INACTIVE → CLEANED_UP (Cleanup)
**Trigger**: `WorkerCleanupTask` executes after grace period
**Actions**:
- Mark worker as CLEANED_UP (terminal state)
- CHASM will clean up the worker state

## Heartbeat Flow

### End-to-End Process

```
Worker Process ──► Frontened Service ──► History Service ──► CHASM Worker Component
      │                    │                   │                      │
      │                    │                   │                      │
   [heartbeat]         [forward]           [update]              [state machine]
      │                    │                   │                      │
      │                    │                   │                      │
      └────────────────────┴───────────────────┴──────────────────────┘
                                    [lease extended]
```

### 1. Worker Process
- Sends periodic heartbeats with `WorkerHeartbeat` containing:
  - `WorkerInstanceKey`: Unique identifier for worker process
  - `lease_duration`: Requested lease duration (e.g., 30 seconds)
  - `worker metadata`: Capabilities, build ID, etc.
  - `bound_activities`: Activities newly bound to the worker since last heartbeat
  - `unbound_activities`: Activities no longer bound to the worker since last heartbeat

### 2. Frontend Service
- Receives heartbeat from worker
- Forwards to History service via `RecordHeartbeat` RPC

### 3. History Service
- Processes heartbeat through CHASM handler
- Calculates lease deadline: `current_time + lease_duration`
- Updates or creates Worker component

### 4. CHASM Worker Component
- Applies appropriate state transition based on current state
- Updates activity bindings based on `bound_activities` and `unbound_activities`
- Schedules `LeaseExpiryTask` for the new deadline
- Persists updated state with current activity bindings

## Configuration

- `matching.InactiveWorkerCleanupDelay`: Dynamic config. Time to wait before cleaning up inactive workers. Default: 60 minutes.

- `Lease duration`: Configurable per heartbeat request. Default: 1 minute if not specified in the request.

## Error Handling

### Network Partitions
- Worker appears inactive due to connectivity loss
- Activities are rescheduled to maintain progress
- Worker can reconnect and resume with new activities

### Clock Skew
- Server calculates lease deadline to avoid client clock issues
