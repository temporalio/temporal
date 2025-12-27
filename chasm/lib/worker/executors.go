// Task executors.
package worker

import (
	"go.temporal.io/server/chasm"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// workerIDTag returns a log tag for the worker ID.
func workerIDTag(workerID string) tag.ZapTag {
	return tag.NewStringTag("worker-id", workerID)
}

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
		workerIDTag(worker.workerID()))

	return TransitionLeaseExpired.Apply(ctx, worker, EventLeaseExpired{})
}

// Validate checks if the lease expiry task is still valid (implements TaskValidator interface).
func (e *LeaseExpiryTaskExecutor) Validate(
	ctx chasm.Context,
	worker *Worker,
	attrs chasm.TaskAttributes,
	task *workerstatepb.LeaseExpiryTask,
) (bool, error) {
	valid, reason := worker.isLeaseExpiryTaskValid(attrs)
	if !valid && reason != "" {
		e.logger.Error(reason, workerIDTag(worker.workerID()))
	}
	return valid, nil
}
