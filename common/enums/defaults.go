package enums

import (
	enumspb "go.temporal.io/api/enums/v1"
)

func SetDefaultWorkflowIdReusePolicy(f *enumspb.WorkflowIdReusePolicy) {
	if *f == enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED {
		*f = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}
}

func SetDefaultWorkflowIdConflictPolicy(
	conflictPolicy *enumspb.WorkflowIdConflictPolicy,
	defaultPolicy enumspb.WorkflowIdConflictPolicy,
) {
	if *conflictPolicy == enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED {
		*conflictPolicy = defaultPolicy
	}
}

func SetDefaultHistoryEventFilterType(f *enumspb.HistoryEventFilterType) {
	if *f == enumspb.HISTORY_EVENT_FILTER_TYPE_UNSPECIFIED {
		*f = enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT
	}
}

func SetDefaultTaskQueueKind(f *enumspb.TaskQueueKind) {
	if *f == enumspb.TASK_QUEUE_KIND_UNSPECIFIED {
		*f = enumspb.TASK_QUEUE_KIND_NORMAL
	}
}

func SetDefaultParentClosePolicy(f *enumspb.ParentClosePolicy) {
	if *f == enumspb.PARENT_CLOSE_POLICY_UNSPECIFIED {
		*f = enumspb.PARENT_CLOSE_POLICY_TERMINATE
	}
}

func SetDefaultQueryRejectCondition(f *enumspb.QueryRejectCondition) {
	if *f == enumspb.QUERY_REJECT_CONDITION_UNSPECIFIED {
		*f = enumspb.QUERY_REJECT_CONDITION_NONE
	}
}

func SetDefaultContinueAsNewInitiator(f *enumspb.ContinueAsNewInitiator) {
	if *f == enumspb.CONTINUE_AS_NEW_INITIATOR_UNSPECIFIED {
		*f = enumspb.CONTINUE_AS_NEW_INITIATOR_WORKFLOW
	}
}

func SetDefaultResetReapplyType(f *enumspb.ResetReapplyType) {
	if *f == enumspb.RESET_REAPPLY_TYPE_UNSPECIFIED {
		*f = enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE
	}
}

func SetDefaultUpdateWorkflowExecutionLifecycleStage(f *enumspb.UpdateWorkflowExecutionLifecycleStage) {
	if *f == enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_UNSPECIFIED {
		*f = enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
	}
}
