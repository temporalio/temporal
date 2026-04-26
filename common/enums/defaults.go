package enums

import (
	enumspb "go.temporal.io/api/enums/v1"
)

func SetDefaultWorkflowIdReusePolicy(f *enumspb.WorkflowIdReusePolicy) {
	if *f == enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED {
		*f = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
	}
}

// SetDefaultWorkflowIDPolicies migrates and applies defaults to both Workflow ID policies.
func SetDefaultWorkflowIDPolicies(
	reusePolicy *enumspb.WorkflowIdReusePolicy,
	conflictPolicy *enumspb.WorkflowIdConflictPolicy,
	defaultConflictPolicy enumspb.WorkflowIdConflictPolicy,
) {
	// Set default conflict policy, if unset
	if *conflictPolicy == enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED {
		//nolint:staticcheck // SA1019: intentional migration of deprecated policy
		if *reusePolicy == enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING {
			// Migrate the deprecated TERMINATE_IF_RUNNING.
			*conflictPolicy = enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING
			*reusePolicy = enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
		} else {
			*conflictPolicy = defaultConflictPolicy
		}
	}

	// Set default reuse policy, if unset
	SetDefaultWorkflowIdReusePolicy(reusePolicy)
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
