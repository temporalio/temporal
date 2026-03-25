package history

import (
	"context"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
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
		executionKey,
		task,
		validate,
	)
}

// discardChasmSideEffectTask handles discard of a CHASM side effect task on standby. It first checks if the execution
// still exists on the source (active) cluster — if gone, it silently drops the task by return nil. If the execution
// still exists and the task's executor implements SideEffectTaskDiscarder, it calls the discard handler. Otherwise, it
// returns ErrTaskDiscarded with a warning log.
func discardChasmSideEffectTask(
	ctx context.Context,
	engine chasm.Engine,
	registry *chasm.Registry,
	tree historyi.ChasmTree,
	task *tasks.ChasmTask,
	logger log.Logger,
	clusterName string,
	clientBean client.Bean,
	namespaceRegistry namespace.Registry,
) error {
	if !executionExistsOnSource(
		ctx,
		taskWorkflowKey(task),
		getTaskArchetypeID(task),
		logger,
		clusterName,
		clientBean,
		namespaceRegistry,
		registry,
	) {
		return nil
	}

	rt, ok := registry.TaskByID(task.Info.TypeId)
	if !ok {
		return serviceerror.NewInternal("unknown task type id")
	}
	if !rt.HasDiscardHandler() {
		logger.Warn("Discarding standby CHASM task due to task being pending for too long.", tag.Task(task))
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
		executionKey,
		task,
		validate,
	)
}
