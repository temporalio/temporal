// Package worker provides a CHASM component for tracking worker heartbeats and lifecycle.
//
// - statemachine.go defines the state transitions, runs the business logic, and schedules tasks.
// - tasks.go defines the executors for processing the scheduled tasks.
//
// # Overview
//
// The Worker component manages the lifecycle of Temporal workers by tracking heartbeats,
// worker metadata, and task activities handled by the worker. Each worker process is
// identified by a unique identifier and maintains a lease through periodic heartbeats.
//
// # Lifecycle States
//
//   - ACTIVE: Worker is sending heartbeats and can receive activities
//   - INACTIVE: Worker lease expired, activities cancelled and rescheduled
//   - CLEANED_UP: Worker session terminated and resources cleaned up (terminal state)
//
// # State Transitions (defined in statemachine.go)
//
//   - ACTIVE → INACTIVE: Lease expires (server assumes worker is down, notifies live activities to be rescheduled)
//   - INACTIVE → ACTIVE: Worker reconnects (network partition recovery)
//   - INACTIVE → CLEANED_UP: Cleanup grace period expires. This is a terminal state.
//
// # Handling Network Partition
//
// When a worker loses connectivity, the server marks it INACTIVE and reschedules its activities.
// If the same worker reconnects, it transitions back to ACTIVE and can receive new activities.
// Previous activities remain cancelled.
package worker

import (
	apipb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/chasm"
	workerpb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
)

const (
	Archetype chasm.Archetype = "Worker"
)

type Worker struct {
	chasm.UnimplementedComponent

	// Persisted state.
	*workerpb.WorkerState
}

// NewWorker creates a new Worker component with the given heartbeat information.
func NewWorker(heartbeat *apipb.WorkerHeartbeat) *Worker {
	return &Worker{
		WorkerState: &workerpb.WorkerState{
			Status:          workerpb.WORKER_STATUS_ACTIVE,
			WorkerHeartbeat: heartbeat,
		},
	}
}

// LifecycleState returns the current lifecycle state of the worker.
func (w *Worker) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch w.Status {
	case workerpb.WORKER_STATUS_CLEANED_UP:
		return chasm.LifecycleStateCompleted
	default:
		// Active workers and inactive workers awaiting cleanup are still running.
		return chasm.LifecycleStateRunning
	}
}

// StateMachineState returns the current status.
func (w *Worker) StateMachineState() workerpb.WorkerStatus {
	return w.Status
}

// SetStateMachineState sets the status.
func (w *Worker) SetStateMachineState(status workerpb.WorkerStatus) {
	w.Status = status
}

// WorkerId returns the unique identifier for this worker.
func (w *Worker) WorkerId() string {
	if w.WorkerHeartbeat == nil {
		return ""
	}
	return w.WorkerHeartbeat.WorkerInstanceKey
}
