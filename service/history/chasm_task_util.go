package history

import (
	"context"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

// validateChasmSideEffectTask completes validation of a CHASM side effect task
// after mutable state load/physical task validation.
func validateChasmSideEffectTask(
	ctx context.Context,
	ms historyi.MutableState,
	task *tasks.ChasmTask,
) (any, error) {
	// Because CHASM timers can target closed workflows, we need to specifically
	// exclude zombie workflows, instead of merely checking that the workflow is
	// running.
	if ms.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		return nil, consts.ErrWorkflowZombie
	}

	tree := ms.ChasmTree()
	if tree == nil {
		return nil, errNoChasmTree
	}

	taskAttributes := chasm.TaskAttributes{
		ScheduledTime: task.GetVisibilityTime(),
		Destination:   task.Destination,
	}

	taskInstance, err := tree.ValidateSideEffectTask(ctx, taskAttributes, task.Info)
	if err == nil && taskInstance != nil {
		// If a taskInstance is returned, the task is still valid, and we should keep
		// it around.
		return &struct{}{}, nil
	}

	return nil, err
}

// executeChasmSideEffectTask completes execution of a CHASM side effect task
// after physical task validation.
//
// TODO - ExecuteSideEffectTask doesn't need to be on ChasmTree once path
// decoding is done at access time.
func executeChasmSideEffectTask(
	ctx context.Context,
	engine chasm.Engine,
	registry *chasm.Registry,
	tree historyi.ChasmTree,
	task *tasks.ChasmTask,
) error {
	entityKey := chasm.EntityKey{
		NamespaceID: task.NamespaceID,
		BusinessID:  task.WorkflowID,
		EntityID:    task.RunID,
	}

	validate := func(backend chasm.NodeBackend, _ chasm.Context, _ chasm.Component) error {
		// Because CHASM timers can target closed workflows, we need to specifically
		// exclude zombie workflows, instead of merely checking that the workflow is
		// running.
		if backend.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
			return consts.ErrWorkflowZombie
		}

		// Validate task generation. We don't need to refresh tasks as we re-generate
		// CHASM tasks on transaction close.
		taskID := task.TaskID
		tgClock := backend.GetExecutionInfo().TaskGenerationShardClockTimestamp
		if tgClock != 0 && taskID != 0 && taskID < tgClock {
			return consts.ErrStaleReference
		}

		return nil
	}

	taskAttributes := chasm.TaskAttributes{
		ScheduledTime: task.GetVisibilityTime(),
		Destination:   task.Destination,
	}

	engineCtx := chasm.NewEngineContext(ctx, engine)
	return tree.ExecuteSideEffectTask(
		engineCtx,
		registry,
		entityKey,
		taskAttributes,
		task.Info,
		validate,
	)
}
