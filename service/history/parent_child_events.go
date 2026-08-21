package history

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/common/wideevents"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

func parentChildPayloadForStartChildTask(
	task *tasks.StartChildExecutionTask,
	childNamespaceID string,
	childExecution *commonpb.WorkflowExecution,
) wideevents.ParentChildLifecyclePayload {
	payload := wideevents.ParentChildLifecyclePayload{
		ParentNamespaceID:      task.GetNamespaceID(),
		ParentWorkflowID:       task.GetWorkflowID(),
		ParentRunID:            task.GetRunID(),
		ChildNamespaceID:       childNamespaceID,
		ChildWorkflowID:        childExecution.GetWorkflowId(),
		ChildRunID:             childExecution.GetRunId(),
		ParentInitiatedID:      task.InitiatedEventID,
		ParentInitiatedVersion: task.Version,
	}
	populateParentChildTaskInfo(&payload, task)
	return payload
}

func parentChildPayloadForCloseTask(
	task *tasks.CloseExecutionTask,
	parentNamespaceID string,
	parentExecution *commonpb.WorkflowExecution,
	parentInitiatedID int64,
	parentInitiatedVersion int64,
) wideevents.ParentChildLifecyclePayload {
	payload := wideevents.ParentChildLifecyclePayload{
		ParentNamespaceID:      parentNamespaceID,
		ParentWorkflowID:       parentExecution.GetWorkflowId(),
		ParentRunID:            parentExecution.GetRunId(),
		ChildNamespaceID:       task.GetNamespaceID(),
		ChildWorkflowID:        task.GetWorkflowID(),
		ChildRunID:             task.GetRunID(),
		ParentInitiatedID:      parentInitiatedID,
		ParentInitiatedVersion: parentInitiatedVersion,
	}
	populateParentChildTaskInfo(&payload, task)
	return payload
}

func parentChildPayloadForCloseVerification(
	shardContext historyi.ShardContext,
	task *tasks.CloseExecutionTask,
	parentNamespaceID string,
	parentExecution *commonpb.WorkflowExecution,
	parentInitiatedID int64,
	parentInitiatedVersion int64,
	resendParent bool,
) *wideevents.ParentChildLifecyclePayload {
	if !parentChildLifecycleEnabled(shardContext) {
		return nil
	}
	payload := parentChildPayloadForCloseTask(
		task,
		parentNamespaceID,
		parentExecution,
		parentInitiatedID,
		parentInitiatedVersion,
	)
	payload.Phase = wideevents.ParentChildPhaseVerifyChildCompletion
	payload.Details = map[string]any{
		"resend_parent_requested": resendParent,
		"verification_scope":      "passive",
	}
	return &payload
}

func emitParentChildCloseVerificationStarted(
	shardContext historyi.ShardContext,
	payload *wideevents.ParentChildLifecyclePayload,
	resendParent bool,
) {
	if payload == nil || !resendParent {
		return
	}
	event := *payload
	event.Outcome = wideevents.ParentChildOutcomeStarted
	emitParentChildLifecycleEvent(shardContext, event, nil)
}

func emitParentChildCloseVerificationResult(
	shardContext historyi.ShardContext,
	payload *wideevents.ParentChildLifecyclePayload,
	resendParent bool,
	err error,
) {
	if payload == nil {
		return
	}
	outcome, emit := parentChildVerificationOutcome(
		err,
		wideevents.ParentChildOutcomeNotFound,
		wideevents.ParentChildOutcomeCompletionMissing,
	)
	if resendParent {
		switch err.(type) {
		case nil:
			outcome, emit = wideevents.ParentChildOutcomeVerified, true
		case *serviceerror.NamespaceNotFound, *serviceerror.Unimplemented:
			outcome, emit = wideevents.ParentChildOutcomeIgnored, true
		default:
		}
	}
	if !emit {
		return
	}
	event := *payload
	event.Outcome = outcome
	emitParentChildLifecycleEvent(shardContext, event, err)
}

func populateParentChildTaskInfo(
	payload *wideevents.ParentChildLifecyclePayload,
	task tasks.Task,
) {
	payload.LocalTaskID = task.GetTaskID()
	payload.LocalTaskType = task.GetType().String()
	if versionedTask, ok := task.(tasks.HasVersion); ok {
		payload.LocalTaskVersion = versionedTask.GetVersion()
	}
}

func emitParentChildLifecycleEvent(
	shardContext historyi.ShardContext,
	payload wideevents.ParentChildLifecyclePayload,
	err error,
) {
	if !parentChildLifecycleEnabled(shardContext) {
		return
	}
	logger := shardContext.GetEventLogger()
	if logger == nil {
		return
	}

	payload.LocalCluster = shardContext.GetClusterMetadata().GetCurrentClusterName()
	payload.LocalShard = shardContext.GetShardID()
	if err != nil {
		payload.Error = err.Error()
		payload.ErrorType = util.ErrorType(err)
	}
	wideevents.Emit(logger, payload)
}

func parentChildLifecycleEnabled(shardContext historyi.ShardContext) bool {
	return shardContext.GetConfig().EmitReplicationLifecycleEvents()
}

func parentChildVerificationOutcome(
	err error,
	notFoundOutcome wideevents.ParentChildOutcome,
	notReadyOutcome wideevents.ParentChildOutcome,
) (wideevents.ParentChildOutcome, bool) {
	if err == nil {
		return "", false
	}
	switch err.(type) {
	case *serviceerror.NotFound:
		return notFoundOutcome, true
	case *serviceerror.WorkflowNotReady:
		return notReadyOutcome, true
	case *serviceerror.NamespaceNotFound, *serviceerror.Unimplemented:
		return "", false
	default:
		return wideevents.ParentChildOutcomeFailed, true
	}
}
