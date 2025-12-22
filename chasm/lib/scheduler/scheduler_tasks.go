package scheduler

import (
	"time"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.uber.org/fx"
)

type SchedulerIdleTaskExecutorOptions struct {
	fx.In

	Config *Config
}

type SchedulerIdleTaskExecutor struct {
	config *Config
}

func NewSchedulerIdleTaskExecutor(opts SchedulerIdleTaskExecutorOptions) *SchedulerIdleTaskExecutor {
	return &SchedulerIdleTaskExecutor{
		config: opts.Config,
	}
}

func (r *SchedulerIdleTaskExecutor) Execute(
	ctx chasm.MutableContext,
	scheduler *Scheduler,
	_ chasm.TaskAttributes,
	_ *schedulerpb.SchedulerIdleTask,
) error {
	scheduler.Closed = true
	return nil
}

func (r *SchedulerIdleTaskExecutor) Validate(
	ctx chasm.Context,
	scheduler *Scheduler,
	taskAttrs chasm.TaskAttributes,
	task *schedulerpb.SchedulerIdleTask,
) (bool, error) {
	idleTimeTotal := task.IdleTimeTotal.AsDuration()
	idleExpiration, isIdle := scheduler.getIdleExpiration(ctx, idleTimeTotal, time.Time{})

	// If the scheduler has since woken up, or its idle expiration time changed, this
	// task must be obsolete.
	if !isIdle || idleExpiration.Compare(taskAttrs.ScheduledTime) != 0 {
		return false, nil
	}

	return !scheduler.Closed, nil
}
