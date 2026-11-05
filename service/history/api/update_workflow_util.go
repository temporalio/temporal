package api

import (
	"context"

	clockspb "go.temporal.io/server/api/clock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow/update"
)

func GetAndUpdateWorkflowWithNew(
	ctx context.Context,
	reqClock *clockspb.VectorClock,
	workflowKey definition.WorkflowKey,
	action UpdateWorkflowActionFunc,
	newWorkflowFn func() (historyi.WorkflowContext, historyi.MutableState, error),
	shard historyi.ShardContext,
	workflowConsistencyChecker WorkflowConsistencyChecker,
) (retError error) {
	workflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		reqClock,
		workflowKey,
		locks.PriorityHigh,
	)
	if err != nil {
		return err
	}
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	return UpdateWorkflowWithNew(shard, ctx, workflowLease, action, newWorkflowFn)
}

func GetAndUpdateWorkflowWithConsistencyCheck(
	ctx context.Context,
	reqClock *clockspb.VectorClock,
	consistencyCheckFn MutableStateConsistencyPredicate,
	workflowKey definition.WorkflowKey,
	action UpdateWorkflowActionFunc,
	newWorkflowFn func() (historyi.WorkflowContext, historyi.MutableState, error),
	shardContext historyi.ShardContext,
	workflowConsistencyChecker WorkflowConsistencyChecker,
) (retError error) {
	workflowLease, err := workflowConsistencyChecker.GetWorkflowLeaseWithConsistencyCheck(
		ctx,
		reqClock,
		consistencyCheckFn,
		workflowKey,
		locks.PriorityHigh,
	)
	if err != nil {
		return err
	}
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	return UpdateWorkflowWithNew(shardContext, ctx, workflowLease, action, newWorkflowFn)
}

func UpdateWorkflowWithNew(
	shardContext historyi.ShardContext,
	ctx context.Context,
	workflowLease WorkflowLease,
	action UpdateWorkflowActionFunc,
	newWorkflowFn func() (historyi.WorkflowContext, historyi.MutableState, error),
) (retError error) {

	// conduct caller action
	postActions, err := action(workflowLease)
	if err != nil {
		return err
	}
	if postActions.Noop {
		return nil
	}

	mutableState := workflowLease.GetMutableState()
	if postActions.CreateWorkflowTask {
		// Create a transfer task to schedule a workflow task only if the workflow is not paused and there is no pending workflow task.
		if !mutableState.HasPendingWorkflowTask() && !mutableState.IsWorkflowExecutionStatusPaused() {
			if _, err := mutableState.AddWorkflowTaskScheduledEvent(
				false,
				enumsspb.WORKFLOW_TASK_TYPE_NORMAL,
			); err != nil {
				return err
			}
		}
	}

	var updateErr error
	if newWorkflowFn != nil {
		newContext, newMutableState, err := newWorkflowFn()
		if err != nil {
			return err
		}
		lastWriteVersion, err := mutableState.GetLastWriteVersion()
		if err != nil {
			return err
		}
		if err = NewWorkflowVersionCheck(shardContext, lastWriteVersion, newMutableState); err != nil {
			return err
		}

		updateErr = workflowLease.GetContext().UpdateWorkflowExecutionWithNewAsActive(
			ctx,
			shardContext,
			newContext,
			newMutableState,
		)
	} else {
		updateErr = workflowLease.GetContext().UpdateWorkflowExecutionAsActive(ctx, shardContext)
	}

	if updateErr != nil {
		return updateErr
	}

	if postActions.AbortUpdates {
		workflowLease.GetContext().UpdateRegistry(ctx).Abort(update.AbortReasonWorkflowCompleted)
	}

	return nil
}
