package dummy

import (
	"context"

	"go.temporal.io/server/service/history/hsm"
)

func RegisterExecutor(
	registry *hsm.Registry,
	taskExecutorOptions TaskExecutorOptions,
) error {
	activeExec := taskExecutor{
		TaskExecutorOptions: taskExecutorOptions,
	}
	if err := hsm.RegisterImmediateExecutor(
		registry,
		activeExec.executeImmediateTask,
	); err != nil {
		return err
	}
	return hsm.RegisterTimerExecutor(
		registry,
		activeExec.executeTimerTask,
	)
}

type (
	TaskExecutorOptions struct {
		ImmediateExecutor hsm.ImmediateExecutor[ImmediateTask]
		TimerExecutor     hsm.TimerExecutor[TimerTask]
	}

	taskExecutor struct {
		TaskExecutorOptions
	}
)

func (e taskExecutor) executeImmediateTask(
	ctx context.Context,
	env hsm.Environment,
	ref hsm.Ref,
	task ImmediateTask,
) error {
	return e.ImmediateExecutor(ctx, env, ref, task)
}

func (e taskExecutor) executeTimerTask(
	env hsm.Environment,
	node *hsm.Node,
	task TimerTask,
) error {
	return e.TimerExecutor(env, node, task)
}
