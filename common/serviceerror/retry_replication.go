package serviceerror

import (
	errordetailsspb "go.temporal.io/server/api/errordetails/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	// RetryReplication represents retry task v2 error.
	RetryReplication struct {
		Message           string
		NamespaceId       string
		WorkflowId        string
		RunId             string
		StartEventId      int64
		StartEventVersion int64
		EndEventId        int64
		EndEventVersion   int64
		st                *status.Status
	}
)

// NewRetryReplication returns new RetryReplication error.
func NewRetryReplication(
	message string,
	namespaceId string,
	workflowId string,
	runId string,
	startEventId int64,
	startEventVersion int64,
	endEventId int64,
	endEventVersion int64,
) error {
	return &RetryReplication{
		Message:           message,
		NamespaceId:       namespaceId,
		WorkflowId:        workflowId,
		RunId:             runId,
		StartEventId:      startEventId,
		StartEventVersion: startEventVersion,
		EndEventId:        endEventId,
		EndEventVersion:   endEventVersion,
	}
}

// Error returns string message.
func (e *RetryReplication) Error() string {
	return e.Message
}

func (e *RetryReplication) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.Aborted, e.Message)
	st, _ = st.WithDetails(
		&errordetailsspb.RetryReplicationFailure{
			NamespaceId:       e.NamespaceId,
			WorkflowId:        e.WorkflowId,
			RunId:             e.RunId,
			StartEventId:      e.StartEventId,
			StartEventVersion: e.StartEventVersion,
			EndEventId:        e.EndEventId,
			EndEventVersion:   e.EndEventVersion,
		},
	)
	return st
}

func (e *RetryReplication) Equal(err *RetryReplication) bool {
	return e.NamespaceId == err.NamespaceId &&
		e.WorkflowId == err.WorkflowId &&
		e.RunId == err.RunId &&
		e.StartEventId == err.StartEventId &&
		e.StartEventVersion == err.StartEventVersion &&
		e.EndEventId == err.EndEventId &&
		e.EndEventVersion == err.EndEventVersion
}

func newRetryReplication(
	st *status.Status,
	errDetails *errordetailsspb.RetryReplicationFailure,
) error {
	return &RetryReplication{
		Message:           st.Message(),
		NamespaceId:       errDetails.GetNamespaceId(),
		WorkflowId:        errDetails.GetWorkflowId(),
		RunId:             errDetails.GetRunId(),
		StartEventId:      errDetails.GetStartEventId(),
		StartEventVersion: errDetails.GetStartEventVersion(),
		EndEventId:        errDetails.GetEndEventId(),
		EndEventVersion:   errDetails.GetEndEventVersion(),
		st:                st,
	}
}
