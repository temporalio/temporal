package serviceerror

import (
	errordetailsspb "go.temporal.io/server/api/errordetails/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	// ObsoleteDispatchBuildId happens when matching wants to dispatch task to an obsolete build ID. This is expected to
	// happen when a workflow has concurrent tasks (and in some other edge cases) and redirect rules apply to the WF.
	// In that case, tasks already scheduled but not started will become invalid and History reschedules them in the
	// new build ID. Matching will still try dispatching the old tasks but it will face this error.
	// Matching can safely drop tasks which face this error.
	// Deprecated. [cleanup-old-wv]
	ObsoleteDispatchBuildId struct {
		Message string
		st      *status.Status
	}
)

// Deprecated. [cleanup-old-wv]
func NewObsoleteDispatchBuildId(msg string) error {
	return &ObsoleteDispatchBuildId{
		Message: msg,
	}
}

// Error returns string message.
func (e *ObsoleteDispatchBuildId) Error() string {
	return e.Message
}

func (e *ObsoleteDispatchBuildId) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.FailedPrecondition, e.Message)
	st, _ = st.WithDetails(
		&errordetailsspb.ObsoleteDispatchBuildIdFailure{},
	)
	return st
}

func newObsoleteDispatchBuildId(st *status.Status) error {
	return &ObsoleteDispatchBuildId{
		Message: st.Message(),
		st:      st,
	}
}
