// Package worker provides a CHASM component for tracking worker heartbeats and lifecycle.
// See README.md for more details.
package worker

import (
	"fmt"
	"time"

	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
)

const (
	Archetype chasm.Archetype = "Worker"
)

type Worker struct {
	chasm.UnimplementedComponent

	// Persisted state.
	*workerstatepb.WorkerState
}

// NewWorker creates a new Worker component with ACTIVE status.
func NewWorker() *Worker {
	return &Worker{
		WorkerState: &workerstatepb.WorkerState{
			Status: workerstatepb.WORKER_STATUS_ACTIVE,
		},
	}
}

// LifecycleState returns the current lifecycle state of the worker.
func (w *Worker) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch w.Status {
	case workerstatepb.WORKER_STATUS_CLEANED_UP:
		return chasm.LifecycleStateCompleted
	default:
		// Active workers and inactive workers awaiting cleanup are still running.
		return chasm.LifecycleStateRunning
	}
}

// StateMachineState returns the current status.
func (w *Worker) StateMachineState() workerstatepb.WorkerStatus {
	return w.Status
}

// SetStateMachineState sets the status.
func (w *Worker) SetStateMachineState(status workerstatepb.WorkerStatus) {
	w.Status = status
}

// workerID returns the unique identifier for this worker.
func (w *Worker) workerID() string {
	if w.GetWorkerHeartbeat() == nil {
		return ""
	}
	return w.GetWorkerHeartbeat().GetWorkerInstanceKey()
}

// recordHeartbeat processes a heartbeat, updating worker state and extending the lease.
func (w *Worker) recordHeartbeat(ctx chasm.MutableContext, heartbeat *workerpb.WorkerHeartbeat, leaseDuration time.Duration) error {
	w.WorkerHeartbeat = heartbeat

	// Calculate lease deadline
	leaseDeadline := ctx.Now(w).Add(leaseDuration)

	// Apply appropriate state transition based on current status
	switch w.Status {
	case workerstatepb.WORKER_STATUS_ACTIVE:
		return TransitionActiveHeartbeat.Apply(ctx, w, EventHeartbeatReceived{
			LeaseDeadline: leaseDeadline,
		})
	case workerstatepb.WORKER_STATUS_INACTIVE:
		// Handle worker resurrection (example network partition, overloaded worker, etc.)
		return TransitionResurrected.Apply(ctx, w, EventHeartbeatReceived{
			LeaseDeadline: leaseDeadline,
		})
	default:
		// CLEANED_UP or other states - not allowed
		return fmt.Errorf("cannot record heartbeat for worker in state %v", w.Status)
	}
}
