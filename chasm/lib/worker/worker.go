// Package worker provides a CHASM component for tracking worker heartbeats and lifecycle.
// See README.md for more details.
package worker

import (
	"encoding/binary"
	"time"

	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
)

const (
	Archetype chasm.Archetype = "Worker"
)

// WorkerInactiveError is returned when a heartbeat is received for an inactive worker.
// Client should re-register with a new WorkerInstanceKey.
type WorkerInactiveError struct{}

func (e *WorkerInactiveError) Error() string {
	return "worker is inactive: re-register with new WorkerInstanceKey"
}

// TokenMismatchError is returned when the conflict token doesn't match.
// Client should use the token from the error response and resend heartbeat.
type TokenMismatchError struct {
	CurrentToken []byte
}

func (e *TokenMismatchError) Error() string {
	return "conflict token mismatch: use token from error response"
}

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
	case workerstatepb.WORKER_STATUS_INACTIVE:
		// INACTIVE is the terminal state - CHASM will delete the entity.
		return chasm.LifecycleStateCompleted
	default:
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
// Returns WorkerInactiveError if the worker is inactive (client must re-register).
// Returns TokenMismatchError if the token doesn't match. Client should update its token
// from the error and resend heartbeat with the same payload (deltas since last success).
func (w *Worker) recordHeartbeat(
	ctx chasm.MutableContext,
	heartbeat *workerpb.WorkerHeartbeat,
	token []byte,
	leaseDuration time.Duration,
) (newToken []byte, err error) {
	// Check if worker is inactive (terminal state)
	if w.Status == workerstatepb.WORKER_STATUS_INACTIVE {
		return nil, &WorkerInactiveError{}
	}

	// Validate token (skip for first heartbeat when ConflictToken is 0)
	if w.ConflictToken > 0 && !w.validateConflictToken(token) {
		return nil, &TokenMismatchError{CurrentToken: w.encodeConflictToken()}
	}

	// Update worker state
	w.WorkerHeartbeat = heartbeat

	// Increment conflict token
	w.ConflictToken++
	newToken = w.encodeConflictToken()

	// Calculate lease deadline and apply transition
	leaseDeadline := ctx.Now(w).Add(leaseDuration)
	err = TransitionActiveHeartbeat.Apply(w, ctx, EventHeartbeatReceived{
		LeaseDeadline: leaseDeadline,
	})
	if err != nil {
		return nil, err
	}

	return newToken, nil
}

// encodeConflictToken encodes the conflict token as bytes for the client.
func (w *Worker) encodeConflictToken() []byte {
	token := make([]byte, 8)
	binary.LittleEndian.PutUint64(token, uint64(w.ConflictToken))
	return token
}

// validateConflictToken checks if the provided token matches the current conflict token.
func (w *Worker) validateConflictToken(token []byte) bool {
	if len(token) != 8 {
		return false
	}
	provided := binary.LittleEndian.Uint64(token)
	return provided == uint64(w.ConflictToken)
}

// isLeaseExpiryTaskValid checks if this lease expiry task is valid or if the lease has been renewed.
// Returns (valid, errorReason) where errorReason is non-empty if there's a bug in state machine.
func (w *Worker) isLeaseExpiryTaskValid(attrs chasm.TaskAttributes) (bool, string) {
	// If worker is not active, no point in processing the lease expiry task.
	// A previous lease expiry must have already transitioned it to inactive.
	if w.GetStatus() != workerstatepb.WORKER_STATUS_ACTIVE {
		return false, ""
	}

	// A nil means bug in the state machine.
	if w.GetLeaseExpirationTime() == nil {
		return false, "Lease expiration time is nil"
	}

	// The lease expiry task is valid only if it matches the scheduled lease expiration time.
	// Otherwise, the lease expiry task has been rescheduled.
	return attrs.ScheduledTime.Equal(w.GetLeaseExpirationTime().AsTime()), ""
}
