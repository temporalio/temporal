package worker

import (
	"time"

	"go.temporal.io/server/chasm"
	workerpb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
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
	component *Worker,
	attrs chasm.TaskAttributes,
	task *workerpb.LeaseExpiryTask,
) error {
	// Validate that this lease expiry is still relevant.
	if !e.isLeaseExpiryStillValid(component, attrs) {
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
	component *Worker,
	attrs chasm.TaskAttributes,
	task *workerpb.LeaseExpiryTask,
) (bool, error) {
	return e.isLeaseExpiryStillValid(component, attrs), nil
}

// isLeaseExpiryStillValid checks if this lease expiry is still the latest lease deadline.
func (e *LeaseExpiryTaskExecutor) isLeaseExpiryStillValid(
	component *Worker,
	attrs chasm.TaskAttributes,
) bool {
	// If worker is not active, lease expiry is not valid.
	if component.Status != workerpb.WORKER_STATUS_ACTIVE {
		return false
	}

	// If no lease deadline set, lease expiry is not valid.
	if component.LeaseExpirationTime == nil {
		return false
	}

	// Timer is valid if its deadline is >= the current lease deadline.
	// (i.e., it hasn't been superseded by a newer heartbeat).
	taskDeadline := attrs.ScheduledTime
	componentDeadline := component.LeaseExpirationTime.AsTime()
	return !taskDeadline.Before(componentDeadline)
}

// WorkerCleanupTaskExecutor handles cleanup of inactive workers.
type WorkerCleanupTaskExecutor struct {
	logger log.Logger
}

func NewWorkerCleanupTaskExecutor(logger log.Logger) *WorkerCleanupTaskExecutor {
	return &WorkerCleanupTaskExecutor{
		logger: logger,
	}
}

// Execute is called to clean up inactive workers.
func (e *WorkerCleanupTaskExecutor) Execute(
	ctx chasm.MutableContext,
	component *Worker,
	attrs chasm.TaskAttributes,
	task *workerpb.WorkerCleanupTask,
) error {
	// Only clean up if worker is inactive.
	if component.Status != workerpb.WORKER_STATUS_INACTIVE {
		return nil // Not inactive, nothing to clean up.
	}

	e.logger.Info("Cleaning up inactive worker")

	// Apply the cleanup completed transition.
	return TransitionCleanupCompleted.Apply(ctx, component, EventCleanupCompleted{
		Time: time.Now(),
	})
}

// Validate checks if cleanup is still needed.
func (e *WorkerCleanupTaskExecutor) Validate(
	ctx chasm.Context,
	component *Worker,
	attrs chasm.TaskAttributes,
	task *workerpb.WorkerCleanupTask,
) (bool, error) {
	// Only valid if worker is inactive.
	return component.Status == workerpb.WORKER_STATUS_INACTIVE, nil
}
