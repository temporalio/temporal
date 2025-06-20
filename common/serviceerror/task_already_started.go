package serviceerror

import (
	"fmt"

	errordetailsspb "go.temporal.io/server/api/errordetails/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	// TaskAlreadyStarted represents task already started error.
	TaskAlreadyStarted struct {
		Message string
		st      *status.Status
	}
)

// NewTaskAlreadyStarted returns new TaskAlreadyStarted error.
func NewTaskAlreadyStarted(taskType string) error {
	return &TaskAlreadyStarted{
		Message: fmt.Sprintf("%s task already started.", taskType),
	}
}

// Error returns string message.
func (e *TaskAlreadyStarted) Error() string {
	return e.Message
}

func (e *TaskAlreadyStarted) Status() *status.Status {
	if e.st != nil {
		return e.st
	}

	st := status.New(codes.AlreadyExists, e.Message)
	st, _ = st.WithDetails(
		&errordetailsspb.TaskAlreadyStartedFailure{},
	)
	return st
}

func newTaskAlreadyStarted(st *status.Status) error {
	return &TaskAlreadyStarted{
		Message: st.Message(),
		st:      st,
	}
}
