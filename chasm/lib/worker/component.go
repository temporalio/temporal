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
