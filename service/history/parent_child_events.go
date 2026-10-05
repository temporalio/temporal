package history

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/wideevents"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

func emitChildCompletionVerificationStarted(
	shardContext historyi.ShardContext,
	task *tasks.CloseExecutionTask,
	parentNamespaceID string,
	parentWorkflowID string,
	parentRunID string,
	parentInitiatedID int64,
	parentInitiatedVersion int64,
	childWorkflowState string,
	resendParent bool,
	attempt int,
) {
	if !resendParent || !parentChildLifecycleEnabled(shardContext) {
		return
	}
	payload, details := parentChildEventForCloseTask(
		shardContext,
		task,
		parentNamespaceID,
		parentWorkflowID,
		parentRunID,
		parentInitiatedID,
		parentInitiatedVersion,
		childWorkflowState,
		attempt,
	)
	details["phase"] = wideevents.ParentChildPhaseVerifyChildCompletion
	details["outcome"] = wideevents.ParentChildOutcomeStarted
	emitParentChildReplicationEvent(
		shardContext,
		payload,
		wideevents.ReplicationExecuting,
		wideevents.ReplOperationStandbyVerification,
		"Standby child completion verification started with parent resend requested",
		nil,
		details,
	)
}

func emitChildCompletionVerificationResult(
	shardContext historyi.ShardContext,
	task *tasks.CloseExecutionTask,
	parentNamespaceID string,
	parentWorkflowID string,
	parentRunID string,
	parentInitiatedID int64,
	parentInitiatedVersion int64,
	childWorkflowState string,
	resendParent bool,
	attempt int,
	err error,
) {
	if !resendParent || !parentChildLifecycleEnabled(shardContext) {
		return
	}
	outcome, emit := parentChildVerificationOutcome(
		err,
		wideevents.ParentChildOutcomeNotFound,
		wideevents.ParentChildOutcomeCompletionMissing,
	)
	switch err.(type) {
	case nil:
		outcome, emit = wideevents.ParentChildOutcomeVerified, true
	case *serviceerror.NamespaceNotFound, *serviceerror.Unimplemented:
		outcome, emit = wideevents.ParentChildOutcomeIgnored, true
	default:
	}
	if !emit {
		return
	}
	payload, details := parentChildEventForCloseTask(
		shardContext,
		task,
		parentNamespaceID,
		parentWorkflowID,
		parentRunID,
		parentInitiatedID,
		parentInitiatedVersion,
		childWorkflowState,
		attempt,
	)
	details["phase"] = wideevents.ParentChildPhaseVerifyChildCompletion
	details["outcome"] = outcome
	if attempt >= 0 {
		details["attempt"] = attempt
	}
	replicationPhase := wideevents.ReplicationError
	if err == nil || outcome == wideevents.ParentChildOutcomeIgnored {
		replicationPhase = wideevents.ReplicationApplied
	}
	emitParentChildReplicationEvent(
		shardContext,
		payload,
		replicationPhase,
		wideevents.ReplOperationStandbyVerification,
		"Standby child completion verification completed",
		err,
		details,
	)
}

func parentChildEventForCloseTask(
	shardContext historyi.ShardContext,
	task *tasks.CloseExecutionTask,
	parentNamespaceID string,
	parentWorkflowID string,
	parentRunID string,
	parentInitiatedID int64,
	parentInitiatedVersion int64,
	childWorkflowState string,
	attempt int,
) (wideevents.ReplicationLifecyclePayload, map[string]any) {
	payload, details := parentChildEventForTask(shardContext, task, attempt)
	payload.ParentWorkflowID = parentWorkflowID
	payload.ParentRunID = parentRunID
	payload.ParentInitiatedID = parentInitiatedID
	details["parent_namespace_id"] = parentNamespaceID
	details["child_workflow_state"] = childWorkflowState
	details["parent_initiated_version"] = parentInitiatedVersion
	return payload, details
}

func parentChildEventForTask(
	shardContext historyi.ShardContext,
	task tasks.Task,
	attempt int,
) (wideevents.ReplicationLifecyclePayload, map[string]any) {
	namespaceName := ""
	if name, err := shardContext.GetNamespaceRegistry().GetNamespaceName(namespace.ID(task.GetNamespaceID())); err == nil {
		namespaceName = name.String()
	}
	details := map[string]any{
		"event_type":    wideevents.ParentChildLifecycleEventType,
		"local_task_id": task.GetTaskID(),
	}
	if versionedTask, ok := task.(tasks.HasVersion); ok {
		details["version"] = versionedTask.GetVersion()
	}
	return wideevents.ReplicationLifecyclePayload{
		TaskType:    task.GetType().String(),
		Shard:       shardContext.GetShardID(),
		Namespace:   namespaceName,
		NamespaceID: task.GetNamespaceID(),
		WorkflowID:  task.GetWorkflowID(),
		RunID:       task.GetRunID(),
		Attempt:     int32(attempt),
	}, details
}

func emitParentChildReplicationEvent(
	shardContext historyi.ShardContext,
	payload wideevents.ReplicationLifecyclePayload,
	phase wideevents.ReplicationPhase,
	operation string,
	message string,
	err error,
	details map[string]any,
) {
	if phase == wideevents.ReplicationError {
		wideevents.EmitReplicationError(shardContext.GetEventLogger(), payload, operation, message, err, details)
		return
	}
	details["operation"] = operation
	details["message"] = message
	payload.Phase = phase
	payload.Details = details
	if phase == wideevents.ReplicationApplied {
		payload.Outcome = wideevents.ParentChildOutcomeVerified
	}
	wideevents.Emit(shardContext.GetEventLogger(), payload)
}

func parentChildLifecycleEnabled(shardContext historyi.ShardContext) bool {
	return shardContext.GetConfig().EmitReplicationLifecycleEvents()
}

func parentChildVerificationOutcome(
	err error,
	notFoundOutcome string,
	notReadyOutcome string,
) (string, bool) {
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
