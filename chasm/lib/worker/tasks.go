// Task executors.
package worker

import (
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// LeaseExpiryTaskExecutor handles lease expiry events.
type LeaseExpiryTaskExecutor struct {
	logger log.Logger
	config *Config
}

func NewLeaseExpiryTaskExecutor(logger log.Logger, config *Config) *LeaseExpiryTaskExecutor {
	return &LeaseExpiryTaskExecutor{
		logger: logger,
		config: config,
	}
}

// Execute is called when a lease expiry timer fires.
func (e *LeaseExpiryTaskExecutor) Execute(
	ctx chasm.MutableContext,
	worker *Worker,
	attrs chasm.TaskAttributes,
	task *workerstatepb.LeaseExpiryTask,
) error {
	// Calculate cleanup delay from dynamic config.
	namespaceID := ctx.ExecutionKey().NamespaceID
	cleanupDelay := e.config.InactiveWorkerCleanupDelay(namespaceID)

	// Apply the lease expiry transition with cleanup delay.
	return TransitionLeaseExpired.Apply(ctx, worker, EventLeaseExpired{
		CleanupDelay: cleanupDelay,
	})
}

// Validate checks if the lease expiry task is still valid (implements TaskValidator interface).
func (e *LeaseExpiryTaskExecutor) Validate(
	ctx chasm.Context,
	worker *Worker,
	attrs chasm.TaskAttributes,
	task *workerstatepb.LeaseExpiryTask,
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
	if worker.GetStatus() != workerstatepb.WORKER_STATUS_ACTIVE {
		return false
	}

	// A nil means bug in the state machine.
	if worker.GetLeaseExpirationTime() == nil {
		e.logger.Error("Lease expiration time is nil", tag.WorkerID(worker.workerID()))
		return false
	}

	// The lease expiry task is valid only if it matches the scheduled lease expiration time.
	// Otherwise, the lease expiry task has been rescheduled.
	return attrs.ScheduledTime.Equal(worker.GetLeaseExpirationTime().AsTime())
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
	task *workerstatepb.CleanupTask,
) error {
	e.logger.Info("Cleaning up inactive worker", tag.WorkerID(worker.workerID()))

	// Apply the cleanup completed transition.
	return TransitionCleanupCompleted.Apply(ctx, worker, EventCleanupCompleted{})
}

// Validate checks if cleanup is still needed.
func (e *WorkerCleanupTaskExecutor) Validate(
	ctx chasm.Context,
	worker *Worker,
	attrs chasm.TaskAttributes,
	task *workerstatepb.CleanupTask,
) (bool, error) {
	return e.isCleanupTaskValid(ctx, worker, attrs), nil
}

// isCleanupTaskValid checks if this cleanup task is still valid or if the worker has been resurrected.
func (e *WorkerCleanupTaskExecutor) isCleanupTaskValid(
	ctx chasm.Context,
	worker *Worker,
	attrs chasm.TaskAttributes,
) bool {
	// Worker must be inactive
	if worker.GetStatus() != workerstatepb.WORKER_STATUS_INACTIVE {
		return false
	}
	// Worker must have a cleanup time
	if worker.GetCleanupTime() == nil {
		return false
	}

	// The cleanup task is valid only if it matches the scheduled cleanup time.
	// Otherwise, the cleanup task has been rescheduled.
	return attrs.ScheduledTime.Equal(worker.GetCleanupTime().AsTime())
}
