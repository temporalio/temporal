// Tasks that are scheduled for Workers and the corresponding executors.
package worker

import (
	"time"

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
	// Validate that this lease expiry is still relevant.
	if !e.isLeaseExpiryTaskValid(worker, attrs) {
		e.logger.Debug("Lease expiry task is no longer valid, ignoring", tag.WorkerID(worker.WorkerID()))
		return nil
	}

	// Calculate cleanup delay from dynamic config.
	namespaceID := ctx.ExecutionKey().NamespaceID
	cleanupDelay := e.config.InactiveWorkerCleanupDelay(namespaceID)

	// Apply the lease expiry transition with cleanup delay.
	return TransitionLeaseExpired.Apply(ctx, worker, EventLeaseExpired{
		Time:         time.Now(),
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
	if worker.Status != workerstatepb.WORKER_STATUS_ACTIVE {
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
	task *workerstatepb.WorkerCleanupTask,
) error {
	// Only clean up if worker is inactive.
	if worker.Status != workerstatepb.WORKER_STATUS_INACTIVE {
		return nil // Not inactive, nothing to clean up.
	}

	e.logger.Info("Cleaning up inactive worker", tag.WorkerID(worker.WorkerID()))

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
	task *workerstatepb.WorkerCleanupTask,
) (bool, error) {
	// Only valid if worker is inactive.
	return worker.Status == workerstatepb.WORKER_STATUS_INACTIVE, nil
}
