package enums

import (
	enumspb "go.temporal.io/api/enums/v1"
)

// DefaultWorkflowIdReusePolicy returns ALLOW_DUPLICATE if the policy is UNSPECIFIED,
// otherwise returns the original policy.
func DefaultWorkflowIdReusePolicy(f enumspb.WorkflowIdReusePolicy) enumspb.WorkflowIdReusePolicy {
	if f == enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED {
		return enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}
	return f
}

// DefaultWorkflowIdConflictPolicy returns the specified default policy if the conflict policy is UNSPECIFIED,
// otherwise returns the original policy.
func DefaultWorkflowIdConflictPolicy(
	conflictPolicy enumspb.WorkflowIdConflictPolicy,
	defaultPolicy enumspb.WorkflowIdConflictPolicy,
) enumspb.WorkflowIdConflictPolicy {
	if conflictPolicy == enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED {
		return defaultPolicy
	}
	return conflictPolicy
}

// DefaultHistoryEventFilterType returns ALL_EVENT if the filter type is UNSPECIFIED,
// otherwise returns the original filter type.
func DefaultHistoryEventFilterType(f enumspb.HistoryEventFilterType) enumspb.HistoryEventFilterType {
	if f == enumspb.HISTORY_EVENT_FILTER_TYPE_UNSPECIFIED {
		return enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT
	}
	return f
}

// DefaultTaskQueueKind returns NORMAL if the kind is UNSPECIFIED,
// otherwise returns the original kind.
func DefaultTaskQueueKind(f enumspb.TaskQueueKind) enumspb.TaskQueueKind {
	if f == enumspb.TASK_QUEUE_KIND_UNSPECIFIED {
		return enumspb.TASK_QUEUE_KIND_NORMAL
	}
	return f
}

// DefaultParentClosePolicy returns TERMINATE if the policy is UNSPECIFIED,
// otherwise returns the original policy.
func DefaultParentClosePolicy(f enumspb.ParentClosePolicy) enumspb.ParentClosePolicy {
	if f == enumspb.PARENT_CLOSE_POLICY_UNSPECIFIED {
		return enumspb.PARENT_CLOSE_POLICY_TERMINATE
	}
	return f
}

// DefaultQueryRejectCondition returns NONE if the condition is UNSPECIFIED,
// otherwise returns the original condition.
func DefaultQueryRejectCondition(f enumspb.QueryRejectCondition) enumspb.QueryRejectCondition {
	if f == enumspb.QUERY_REJECT_CONDITION_UNSPECIFIED {
		return enumspb.QUERY_REJECT_CONDITION_NONE
	}
	return f
}

// DefaultContinueAsNewInitiator returns WORKFLOW if the initiator is UNSPECIFIED,
// otherwise returns the original initiator.
func DefaultContinueAsNewInitiator(f enumspb.ContinueAsNewInitiator) enumspb.ContinueAsNewInitiator {
	if f == enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED {
		return enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW
	}
	return f
}

// DefaultResetReapplyType returns ALL_ELIGIBLE if the type is UNSPECIFIED,
// otherwise returns the original type.
func DefaultResetReapplyType(f enumspb.ResetReapplyType) enumspb.ResetReapplyType {
	if f == enumspb.RESET_REAPPLY_TYPE_UNSPECIFIED {
		return enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE
	}
	return f
}

// DefaultUpdateWorkflowExecutionLifecycleStage returns COMPLETED if the stage is UNSPECIFIED,
// otherwise returns the original stage.
func DefaultUpdateWorkflowExecutionLifecycleStage(f enumspb.UpdateWorkflowExecutionLifecycleStage) enumspb.UpdateWorkflowExecutionLifecycleStage {
	if f == enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED {
		return enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
	}
	return f
}
