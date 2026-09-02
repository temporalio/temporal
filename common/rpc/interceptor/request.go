package interceptor

import (
	"go.temporal.io/api/workflowservice/v1"
)

// The following helpers classify whether a request is in long-poll mode. Several APIs are only
// long-running when their long-poll parameter is set; otherwise they are cheap point-reads.
func IsLongPollGetWorkflowExecutionHistoryRequest(
	req any,
) bool {
	switch request := req.(type) {
	case *workflowservice.GetWorkflowExecutionHistoryRequest:
		return request.GetWaitNewEvent()
	}
	return false
}

func IsLongPollDescribeActivityExecutionRequest(
	req any,
) bool {
	switch request := req.(type) {
	case *workflowservice.DescribeActivityExecutionRequest:
		return len(request.GetLongPollToken()) > 0
	}
	return false
}

func IsLongPollDescribeNexusOperationExecutionRequest(
	req any,
) bool {
	switch request := req.(type) {
	case *workflowservice.DescribeNexusOperationExecutionRequest:
		return len(request.GetLongPollToken()) > 0
	}
	return false
}
