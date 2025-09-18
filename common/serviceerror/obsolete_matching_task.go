package serviceerror

import (
	"fmt"

	errordetailsspb "go.temporal.io/server/api/errordetails/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	// ObsoleteMatchingTask happens when History determines a task that Matching wants to dispatch
	// is no longer valid. Some examples:
	// - WFT belongs to a sticky queue but the workflow is not on that sticky queue anymore.
	// - WFT belongs to the normal queue but no the workflow is on a sticky queue.
	// - WFT options were updated and the task was rescheduled
	// - WF or activity task wants to start but their schedule time directive deployment no longer
	//   matched the workflows effective deployment.
	// When Matching receives this error it can safely drop the task because History has already
	// scheduled new Matching tasks on the right task queue and deployment.
	ObsoleteMatchingTask struct {
		Message string
		st      *status.Status
	}
)

func NewObsoleteMatchingTask(msg string) error {
	return &ObsoleteMatchingTask{
		Message: msg,
	}
}

func NewObsoleteMatchingTaskf(format string, args ...any) error {
	return &ObsoleteMatchingTask{
		Message: fmt.Sprintf(format, args...),
	}
}

// Error returns string message.
func (e *ObsoleteMatchingTask) Error() string {
	return e.Message
}

func (e *ObsoleteMatchingTask) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.FailedPrecondition, e.Message)
	st, _ = st.WithDetails(
		&errordetailsspb.ObsoleteMatchingTaskFailure{},
	)
	return st
}

func newObsoleteMatchingTask(st *status.Status) error {
	return &ObsoleteMatchingTask{
		Message: st.Message(),
		st:      st,
	}
}
