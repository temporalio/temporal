package interceptor

import (
	"go.temporal.io/api/workflowservice/v1"
)

// IsLongPollGetWorkflowExecutionHistoryRequest reports whether req is a
// GetWorkflowExecutionHistory request in long-poll mode.
func IsLongPollGetWorkflowExecutionHistoryRequest(
	req any,
) bool {
	switch request := req.(type) {
	case *workflowservice.GetWorkflowExecutionHistoryRequest:
		return request.GetWaitNewEvent()
	}
	return false
}

// IsLongPollDescribeActivityExecutionRequest reports whether req is a
// DescribeActivityExecution request in long-poll mode.
func IsLongPollDescribeActivityExecutionRequest(
	req any,
) bool {
	switch request := req.(type) {
	case *workflowservice.DescribeActivityExecutionRequest:
		return len(request.GetLongPollToken()) > 0
	}
	return false
}

// IsLongPollDescribeNexusOperationExecutionRequest reports whether req is a
// DescribeNexusOperationExecution request in long-poll mode.
func IsLongPollDescribeNexusOperationExecutionRequest(
	req any,
) bool {
	switch request := req.(type) {
	case *workflowservice.DescribeNexusOperationExecutionRequest:
		return len(request.GetLongPollToken()) > 0
	}
	return false
}
