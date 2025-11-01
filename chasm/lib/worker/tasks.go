// Tasks that are scheduled for Workers and the corresponding executors.
package worker

import (
	"time"

	"go.temporal.io/server/chasm"
	workerpb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
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
	worker *Worker,
	attrs chasm.TaskAttributes,
	task *workerpb.LeaseExpiryTask,
) error {
	// Validate that this lease expiry is still relevant.
	if !e.isLeaseExpiryTaskValid(worker, attrs) {
		e.logger.Debug("Lease expiry task is no longer valid, ignoring", tag.WorkerId(worker.WorkerId()))
		return nil
	}

	// Apply the lease expiry transition.
	return TransitionLeaseExpired.Apply(ctx, worker, EventLeaseExpired{
		Time: time.Now(),
	})
}

// Validate checks if the lease expiry task is still valid (implements TaskValidator interface).
func (e *LeaseExpiryTaskExecutor) Validate(
	ctx chasm.Context,
	worker *Worker,
	attrs chasm.TaskAttributes,
	task *workerpb.LeaseExpiryTask,
) (bool, error) {
	return e.isLeaseExpiryTaskValid(worker, attrs), nil
}

// isLeaseExpiryTaskValid checks if this lease expiry task is valid or if the lease has been renewed.
func (e *LeaseExpiryTaskExecutor) isLeaseExpiryTaskValid(
	worker *Worker,
	attrs chasm.TaskAttributes,
) bool {
	// If worker is not active, no point in processing the least expiry task.
	// A previous lease expiry must have already transitioned it to inactive.
	if worker.Status != workerpb.WORKER_STATUS_ACTIVE {
		return false
	}

	if worker.LeaseExpirationTime == nil {
		return false
	}

	scheduledLeaseExpirationTime := attrs.ScheduledTime
	workerLeaseExpirationTime := worker.LeaseExpirationTime.AsTime()
	// If the lease has been renewed past the scheduled expiry time, this task is no longer valid.
	return !workerLeaseExpirationTime.After(scheduledLeaseExpirationTime)
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
	worker *Worker,
	attrs chasm.TaskAttributes,
	task *workerpb.WorkerCleanupTask,
) error {
	// Only clean up if worker is inactive.
	if worker.Status != workerpb.WORKER_STATUS_INACTIVE {
		return nil // Not inactive, nothing to clean up.
	}

	e.logger.Info("Cleaning up inactive worker", tag.WorkerId(worker.WorkerId()))

	// Apply the cleanup completed transition.
	return TransitionCleanupCompleted.Apply(ctx, worker, EventCleanupCompleted{
		Time: time.Now(),
	})
}

// Validate checks if cleanup is still needed.
func (e *WorkerCleanupTaskExecutor) Validate(
	ctx chasm.Context,
	worker *Worker,
	attrs chasm.TaskAttributes,
	task *workerpb.WorkerCleanupTask,
) (bool, error) {
	// Only valid if worker is inactive.
	return worker.Status == workerpb.WORKER_STATUS_INACTIVE, nil
}
