package workersession

import (
	"time"

	"go.temporal.io/server/chasm"
	workersessionpb "go.temporal.io/server/chasm/lib/workersession/gen/workersessionpb/v1"
	"go.temporal.io/server/common/log"
)

// LeaseExpiryTaskExecutor handles lease expiry events.
type LeaseExpiryTaskExecutor struct {
	logger log.Logger
}

func NewLeaseExpiryTaskExecutor(logger log.Logger) *LeaseExpiryTaskExecutor {
	return &LeaseExpiryTaskExecutor{
		logger: logger,
	}
}

// Execute is called when a lease expiry timer fires.
func (e *LeaseExpiryTaskExecutor) Execute(
	ctx chasm.MutableContext,
	component *WorkerSession,
	attrs chasm.TaskAttributes,
	task *workersessionpb.LeaseExpiryTask,
) error {
	// Validate that this lease expiry is still relevant.
	if !e.isLeaseExpiryStillValid(component, task) {
		e.logger.Debug("Lease expiry task is no longer valid, ignoring")
		return nil
	}

	// Apply the lease expiry transition.
	return TransitionLeaseExpired.Apply(ctx, component, EventLeaseExpired{
		Time: time.Now(),
	})
}

// Validate checks if the lease expiry task is still valid (implements TaskValidator interface).
func (e *LeaseExpiryTaskExecutor) Validate(
	ctx chasm.Context,
	component *WorkerSession,
	attrs chasm.TaskAttributes,
	task *workersessionpb.LeaseExpiryTask,
) (bool, error) {
	return e.isLeaseExpiryStillValid(component, task), nil
}

// isLeaseExpiryStillValid checks if this lease expiry is still the latest lease deadline.
func (e *LeaseExpiryTaskExecutor) isLeaseExpiryStillValid(
	component *WorkerSession,
	task *workersessionpb.LeaseExpiryTask,
) bool {
	// If session is not active, lease expiry is not valid.
	if component.Status != workersessionpb.WORKER_SESSION_STATUS_ACTIVE {
		return false
	}

	// If no lease deadline set, lease expiry is not valid.
	if component.LeaseExpirationTime == nil {
		return false
	}

	// Timer is valid if its deadline is >= the current lease deadline.
	// (i.e., it hasn't been superseded by a newer heartbeat).
	taskDeadline := task.LeaseExpirationTime.AsTime()
	componentDeadline := component.LeaseExpirationTime.AsTime()
	return !taskDeadline.Before(componentDeadline)
}

// SessionCleanupTaskExecutor handles cleanup of expired sessions.
type SessionCleanupTaskExecutor struct {
	logger log.Logger
}

func NewSessionCleanupTaskExecutor(logger log.Logger) *SessionCleanupTaskExecutor {
	return &SessionCleanupTaskExecutor{
		logger: logger,
	}
}

// Execute is called to clean up expired sessions.
func (e *SessionCleanupTaskExecutor) Execute(
	ctx chasm.MutableContext,
	component *WorkerSession,
	attrs chasm.TaskAttributes,
	task *workersessionpb.SessionCleanupTask,
) error {
	// Only clean up if session is expired.
	if component.Status != workersessionpb.WORKER_SESSION_STATUS_EXPIRED {
		return nil // Not expired, nothing to clean up.
	}

	e.logger.Info("Cleaning up expired worker session")

	// Apply the cleanup completed transition.
	return TransitionCleanupCompleted.Apply(ctx, component, EventCleanupCompleted{
		Time: time.Now(),
	})
}

// Validate checks if cleanup is still needed.
func (e *SessionCleanupTaskExecutor) Validate(
	ctx chasm.Context,
	component *WorkerSession,
	attrs chasm.TaskAttributes,
	task *workersessionpb.SessionCleanupTask,
) (bool, error) {
	// Only valid if session is expired.
	return component.Status == workersessionpb.WORKER_SESSION_STATUS_EXPIRED, nil
}
