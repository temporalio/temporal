package scheduler

import (
	"go.temporal.io/server/chasm"
	schedulespb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
)

type SchedulerIdleTaskExecutor struct{}

func NewSchedulerIdleTaskExecutor() *SchedulerIdleTaskExecutor {
	return &SchedulerIdleTaskExecutor{}
}

func (r *SchedulerIdleTaskExecutor) Execute(
	ctx chasm.MutableContext,
	scheduler *Scheduler,
	_ chasm.TaskAttributes,
	_ *schedulespb.SchedulerIdleTask,
) error {
	scheduler.Closed = true
	return nil
}

func (r *SchedulerIdleTaskExecutor) Validate(
	ctx chasm.Context,
	scheduler *Scheduler,
	_ chasm.TaskAttributes,
	_ *schedulespb.SchedulerIdleTask,
) (bool, error) {
	return !scheduler.Closed, nil
}
