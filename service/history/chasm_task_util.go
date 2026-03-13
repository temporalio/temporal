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
) (bool, error) {
	// Because CHASM timers can target closed workflows, we need to specifically
	// exclude zombie workflows, instead of merely checking that the workflow is
	// running.
	if ms.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		return false, consts.ErrWorkflowZombie
	}

	tree := ms.ChasmTree()
	if tree == nil {
		return false, errNoChasmTree
	}

	return tree.ValidateSideEffectTask(ctx, task)
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
	executionKey := chasm.ExecutionKey{
		NamespaceID: task.NamespaceID,
		BusinessID:  task.WorkflowID,
		RunID:       task.RunID,
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

	engineCtx := chasm.NewEngineContext(ctx, engine)
	return tree.ExecuteSideEffectTask(
		engineCtx,
		registry,
		executionKey,
		task,
		validate,
	)
}

// discardChasmSideEffectTask runs the discard handler for a CHASM side effect task on standby. If the task's executor
// implements SideEffectDiscardHandler,it calls the handler. Otherwise, it returns ErrTaskDiscarded.
func discardChasmSideEffectTask(
	ctx context.Context,
	engine chasm.Engine,
	registry *chasm.Registry,
	tree historyi.ChasmTree,
	task *tasks.ChasmTask,
) error {
	rt, ok := registry.TaskByID(task.Info.TypeId)
	if !ok || !rt.HasDiscardHandler() {
		return consts.ErrTaskDiscarded
	}

	executionKey := chasm.ExecutionKey{
		NamespaceID: task.NamespaceID,
		BusinessID:  task.WorkflowID,
		RunID:       task.RunID,
	}

	validate := func(backend chasm.NodeBackend, _ chasm.Context, _ chasm.Component) error {
		if backend.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
			return consts.ErrWorkflowZombie
		}

		taskID := task.TaskID
		tgClock := backend.GetExecutionInfo().TaskGenerationShardClockTimestamp
		if tgClock != 0 && taskID != 0 && taskID < tgClock {
			return consts.ErrStaleReference
		}

		return nil
	}

	engineCtx := chasm.NewEngineContext(ctx, engine)
	return tree.ExecuteSideEffectDiscardTask(
		engineCtx,
		registry,
		executionKey,
		task,
		validate,
	)
}
