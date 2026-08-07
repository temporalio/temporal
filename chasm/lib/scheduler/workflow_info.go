package scheduler

import (
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
)

// applyNewWorkflowExecutionInfo copies fields that the CHASM scheduler forwards
// unchanged from a schedule action to a workflow start request.
func applyNewWorkflowExecutionInfo(
	request *workflowservice.StartWorkflowExecutionRequest,
	info *workflowpb.NewWorkflowExecutionInfo,
) {
	request.WorkflowType = info.WorkflowType
	request.TaskQueue = info.TaskQueue
	request.Input = info.Input
	request.WorkflowExecutionTimeout = info.WorkflowExecutionTimeout
	request.WorkflowRunTimeout = info.WorkflowRunTimeout
	request.WorkflowTaskTimeout = info.WorkflowTaskTimeout
	request.RetryPolicy = info.RetryPolicy
	request.Memo = info.Memo
	request.Header = info.Header
	request.UserMetadata = info.UserMetadata
	request.VersioningOverride = info.VersioningOverride
	request.Priority = info.Priority
}
