package serviceerror

import (
	errordetailsspb "go.temporal.io/server/api/errordetails/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	// FlowControlBlocked indicates that a task can't be dispatched due to flow control.
	FlowControlBlocked struct {
		Message string
		st      *status.Status
	}
)

// NewFlowControlBlocked returns an error indicating that a task can't be dispatched due to flow control.
func NewFlowControlBlocked() error {
	return &FlowControlBlocked{
		Message: "blocked",
	}
}

func (e *FlowControlBlocked) Error() string {
	return e.Message
}

func (e *FlowControlBlocked) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.FailedPrecondition, e.Message)
	st, _ = st.WithDetails(&errordetailsspb.FlowControlBlockedFailure{})
	return st
}

func newFlowControlBlocked(st *status.Status) error {
	return &FlowControlBlocked{
		Message: st.Message(),
		st:      st,
	}
}
