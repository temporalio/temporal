package serviceerror

import (
	errordetailsspb "go.temporal.io/server/api/errordetails/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	// ActivityStartDuringTransition when an activity start is rejected by History because the
	// workflow is in a transitioning between worker deployments.
	// When Matching sees this error it can safely drop the activity task because History will
	// reschedule the activity task after the transition is complete.
	ActivityStartDuringTransition struct {
		Message string
		st      *status.Status
	}
)

func NewActivityStartDuringTransition() error {
	return &ActivityStartDuringTransition{
		Message: "activity start attempted during deployment transition",
	}
}

// Error returns string message.
func (e *ActivityStartDuringTransition) Error() string {
	return e.Message
}

func (e *ActivityStartDuringTransition) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.FailedPrecondition, e.Message)
	st, _ = st.WithDetails(
		&errordetailsspb.ActivityStartDuringTransitionFailure{},
	)
	return st
}

func newActivityStartDuringTransition(st *status.Status) error {
	return &ActivityStartDuringTransition{
		Message: st.Message(),
		st:      st,
	}
}
