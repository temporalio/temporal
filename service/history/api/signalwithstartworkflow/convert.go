package signalwithstartworkflow

import (
	"time"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
)

func ConvertToStartRequest(
	namespaceID namespace.ID,
	request *workflowservice.SignalWithStartWorkflowExecutionRequest,
	now time.Time,
) *historyservice.StartWorkflowExecutionRequest {
	req := &workflowservice.StartWorkflowExecutionRequest{
		Namespace:                request.GetNamespace(),
		WorkflowId:               request.GetWorkflowId(),
		WorkflowType:             request.GetWorkflowType(),
		TaskQueue:                request.GetTaskQueue(),
		Input:                    request.GetInput(),
		WorkflowExecutionTimeout: request.GetWorkflowExecutionTimeout(),
		WorkflowRunTimeout:       request.GetWorkflowRunTimeout(),
		WorkflowTaskTimeout:      request.GetWorkflowTaskTimeout(),
		Identity:                 request.GetIdentity(),
		RequestId:                request.GetRequestId(),
		WorkflowIdReusePolicy:    request.GetWorkflowIdReusePolicy(),
		WorkflowIdConflictPolicy: request.GetWorkflowIdConflictPolicy(),
		RetryPolicy:              request.GetRetryPolicy(),
		CronSchedule:             request.GetCronSchedule(),
		Memo:                     request.GetMemo(),
		SearchAttributes:         request.GetSearchAttributes(),
		Header:                   request.GetHeader(),
		WorkflowStartDelay:       request.GetWorkflowStartDelay(),
		UserMetadata:             request.UserMetadata,
		Links:                    request.GetLinks(),
		VersioningOverride:       request.GetVersioningOverride(),
		Priority:                 request.GetPriority(),
	}

	return common.CreateHistoryStartWorkflowRequest(namespaceID.String(), req, nil, nil, now)
}
