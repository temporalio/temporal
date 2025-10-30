// Package worker provides a CHASM component for tracking worker heartbeats and lifecycle.
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
// # State Transitions
//
//   - ACTIVE → INACTIVE: Lease expires (server assumes worker is down, notifies live activities to be rescheduled)
//   - INACTIVE → ACTIVE: Worker reconnects (network partition recovery)
//   - INACTIVE → CLEANED_UP: Cleanup grace period expires
//
// Heartbeats received in CLEANED_UP state return an error as it is a terminal state.
//
// # Network Partition Handling
//
// When a worker loses connectivity, the server marks it INACTIVE and reschedules
// its activities. If the same worker reconnects, it transitions back to ACTIVE
// and can receive new activities. Previous activities remain cancelled.
package worker

import (
	"go.temporal.io/server/chasm"
	workerpb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
)

const (
	Archetype chasm.Archetype = "Worker"
)

// Worker is a Chasm component that tracks worker heartbeats and manages worker lifecycle.
type Worker struct {
	chasm.UnimplementedComponent

	// Persisted state for heartbeat tracking.
	*workerpb.WorkerState
}

// NewWorker creates a new Worker component.
func NewWorker() *Worker {
	return &Worker{
		WorkerState: &workerpb.WorkerState{
			Status: workerpb.WORKER_STATUS_ACTIVE,
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

// State returns the current status (implements StateMachine interface).
func (w *Worker) State() workerpb.WorkerStatus {
	return w.Status
}

// SetState sets the status (implements StateMachine interface).
func (w *Worker) SetState(status workerpb.WorkerStatus) {
	w.Status = status
}
