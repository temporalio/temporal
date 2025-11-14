// Package worker provides a CHASM component for tracking worker heartbeats and lifecycle.
// See README.md for more details.
package worker

import (
	"fmt"
	"time"

	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
)

const (
	Archetype chasm.Archetype = "Worker"

	// Default duration for worker leases if not specified in the request.
	defaultLeaseDuration = 1 * time.Minute
)

// Worker is a Chasm component that tracks worker heartbeats and manages worker lifecycle.
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
func (w *Worker) recordHeartbeat(ctx chasm.MutableContext, req *workerstatepb.RecordHeartbeatRequest) (*workerstatepb.RecordHeartbeatResponse, error) {
	// Extract worker heartbeat from request
	frontendReq := req.GetFrontendRequest()
	workerHeartbeat := frontendReq.GetWorkerHeartbeat()[0]

	// TODO: Honor the lease duration from the request.
	leaseDuration := defaultLeaseDuration

	w.WorkerHeartbeat = workerHeartbeat

	// Calculate lease deadline
	leaseDeadline := ctx.Now(w).Add(leaseDuration)

	// Apply appropriate state transition based on current status
	var err error
	switch w.Status {
	case workerstatepb.WORKER_STATUS_ACTIVE:
		err = TransitionActiveHeartbeat.Apply(ctx, w, EventHeartbeatReceived{
			LeaseDeadline: leaseDeadline,
		})
	case workerstatepb.WORKER_STATUS_INACTIVE:
		// Handle worker resurrection (example network partition, overloaded worker, etc.)
		err = TransitionResurrected.Apply(ctx, w, EventHeartbeatReceived{
			LeaseDeadline: leaseDeadline,
		})
	default:
		// CLEANED_UP or other states - not allowed
		err = fmt.Errorf("cannot record heartbeat for worker in state %v", w.Status)
	}

	if err != nil {
		return nil, err
	}

	return &workerstatepb.RecordHeartbeatResponse{}, nil
}
