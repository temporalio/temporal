package scheduler

import (
	"go.temporal.io/server/service/history/consts"
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
	return hsm.RegisterTimerExecutors(
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

// checkParentIsRunning checks that the parent node is running if the operation is attached to a workflow execution.
func checkParentIsRunning(node *hsm.Node) error {
	if node.Parent != nil {
		execution, err := hsm.MachineData[interface{ IsWorkflowExecutionRunning() bool }](node.Parent)
		if err != nil {
			return err
		}
		if !execution.IsWorkflowExecutionRunning() {
			return consts.ErrWorkflowCompleted
		}
	}
	return nil
}

func (e activeExecutor) executeScheduleTask(
	env hsm.Environment,
	node *hsm.Node,
	task ScheduleTask,
) error {
	if err := checkParentIsRunning(node); err != nil {
		return err
	}
	// TODO(Tianyu): Perform scheduler logic before scheduling self again
	return hsm.MachineTransition(node, func(scheduler Scheduler) (hsm.TransitionOutput, error) {
		return TransitionSchedulerActivate.Apply(scheduler, EventSchedulerActivate{})
	})
}

type (
	StandbyExecutorOptions struct{}

	standbyExecutor struct {
		options StandbyExecutorOptions
	}
)

func (e standbyExecutor) executeScheduleTask(
	env hsm.Environment,
	node *hsm.Node,
	task ScheduleTask,
) error {
	panic("unimplemented")
}
