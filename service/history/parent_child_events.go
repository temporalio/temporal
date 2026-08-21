package history

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/wideevents"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tasks"
)

func emitFirstWorkflowTaskVerificationResult(
	shardContext historyi.ShardContext,
	task *tasks.StartChildExecutionTask,
	childNamespaceID string,
	childWorkflowID string,
	childRunID string,
	parentWorkflowState string,
	attempt int,
	err error,
) {
	if !parentChildLifecycleEnabled(shardContext) {
		return
	}
	outcome, emit := parentChildVerificationOutcome(
		err,
		wideevents.ParentChildOutcomeChildNotFound,
		wideevents.ParentChildOutcomeFirstWorkflowTaskMissing,
	)
	if !emit {
		return
	}
	payload, details := parentChildEventForStartChildTask(
		shardContext,
		task,
		childNamespaceID,
		childWorkflowID,
		childRunID,
		parentWorkflowState,
		attempt,
	)
	details["phase"] = wideevents.ParentChildPhaseVerifyFirstWorkflowTask
	details["outcome"] = outcome
	emitParentChildReplicationEvent(
		shardContext,
		payload,
		wideevents.ReplicationError,
		wideevents.ReplOperationStandbyVerification,
		"Standby first workflow task verification failed",
		err,
		details,
	)
}

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
	details["resend_parent_requested"] = true
	details["verification_scope"] = "passive"
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
	if !parentChildLifecycleEnabled(shardContext) {
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
	details["resend_parent_requested"] = resendParent
	details["verification_scope"] = "passive"
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

func parentChildEventForStartChildTask(
	shardContext historyi.ShardContext,
	task *tasks.StartChildExecutionTask,
	childNamespaceID string,
	childWorkflowID string,
	childRunID string,
	parentWorkflowState string,
	attempt int,
) (wideevents.ReplicationLifecyclePayload, map[string]any) {
	payload, details := parentChildEventForTask(shardContext, task, attempt)
	details["parent_namespace_id"] = task.GetNamespaceID()
	details["parent_workflow_id"] = task.GetWorkflowID()
	details["parent_run_id"] = task.GetRunID()
	details["parent_workflow_state"] = parentWorkflowState
	details["child_namespace_id"] = childNamespaceID
	details["child_workflow_id"] = childWorkflowID
	details["child_run_id"] = childRunID
	details["parent_initiated_id"] = task.InitiatedEventID
	details["parent_initiated_version"] = task.Version
	return payload, details
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
	details["parent_workflow_id"] = parentWorkflowID
	details["parent_run_id"] = parentRunID
	details["child_namespace_id"] = task.GetNamespaceID()
	details["child_workflow_id"] = task.GetWorkflowID()
	details["child_run_id"] = task.GetRunID()
	details["child_workflow_state"] = childWorkflowState
	details["parent_initiated_id"] = parentInitiatedID
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
		"event_type":      wideevents.ParentChildLifecycleEventType,
		"local_cluster":   shardContext.GetClusterMetadata().GetCurrentClusterName(),
		"local_task_id":   task.GetTaskID(),
		"local_task_type": task.GetType().String(),
	}
	if versionedTask, ok := task.(tasks.HasVersion); ok {
		details["version"] = versionedTask.GetVersion()
	}
	if attempt >= 0 {
		details["attempt"] = attempt
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
