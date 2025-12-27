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
}

func NewLeaseExpiryTaskExecutor(logger log.Logger) *LeaseExpiryTaskExecutor {
	return &LeaseExpiryTaskExecutor{
		logger: logger,
	}
}

// Execute is called when a lease expiry timer fires.
// Transitions the worker to INACTIVE (terminal state).
func (e *LeaseExpiryTaskExecutor) Execute(
	ctx chasm.MutableContext,
	worker *Worker,
	attrs chasm.TaskAttributes,
	task *workerstatepb.LeaseExpiryTask,
) error {
	e.logger.Info("Worker lease expired, marking as inactive",
		tag.WorkerID(worker.workerID()))

	return TransitionLeaseExpired.Apply(ctx, worker, EventLeaseExpired{})
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
	// If worker is not active, no point in processing the lease expiry task.
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
