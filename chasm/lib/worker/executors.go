package worker

import (
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
)

type leaseExpiryTaskExecutor struct {
	chasm.PureTaskHandlerBase
}

func (e *leaseExpiryTaskExecutor) Validate(
	_ chasm.Context,
	group *WorkerGroup,
	invocation chasm.TaskInvocation,
	_ *workerpb.LeaseExpiryTask,
) (bool, error) {
	// Ignore expiry tasks whose deadline no longer matches the current lease.
	// The lease has already been extended, so this task must not close the group.
	return group.Status == workerpb.WORKER_GROUP_STATUS_ACTIVE &&
		invocation.ScheduledTime.Equal(group.GetLeaseExpirationTime().AsTime()), nil
}

func (e *leaseExpiryTaskExecutor) Execute(
	ctx chasm.MutableContext,
	group *WorkerGroup,
	_ chasm.TaskAttributes,
	_ *workerpb.LeaseExpiryTask,
) error {
	return transitionLeaseExpired.Apply(group, ctx, eventLeaseExpired{})
}
