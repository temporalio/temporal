package scheduler

import (
	"context"
	"go.temporal.io/server/service/history/hsm"
)

func RegisterExecutor(
	registry *hsm.Registry,
	activeExecutorOptions ActiveExecutorOptions,
	standbyExecutorOptions StandbyExecutorOptions,
	config *Config,
) error {
	activeExec := activeExecutor{options: activeExecutorOptions, config: config}
	standbyExec := standbyExecutor{options: standbyExecutorOptions}
	return hsm.RegisterExecutors(
		registry,
		TaskTypeSchedule.ID,
		activeExec.executeScheduleTask,
		standbyExec.executeScheduleTask,
	)
}

type (
	ActiveExecutorOptions struct {
	}

	activeExecutor struct {
		options ActiveExecutorOptions
		config  *Config
	}
)

func (e activeExecutor) executeScheduleTask(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	task ScheduleTask,
) error {
	// TODO(Tianyu): Perform scheduler logic before scheduling self again
	return env.Access(ctx, ref, hsm.AccessWrite, func(node *hsm.Node) error {
		return hsm.MachineTransition(node, func(scheduler Scheduler) (hsm.TransitionOutput, error) {
			return TransitionSchedulerActivate.Apply(scheduler, EventSchedulerActivate{})
		})
	})}

type (
	StandbyExecutorOptions struct{}

	standbyExecutor struct {
		options StandbyExecutorOptions
	}
)

func (e standbyExecutor) executeScheduleTask(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	task ScheduleTask,
) error {
	panic("unimplemented")
}