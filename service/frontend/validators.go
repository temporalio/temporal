package frontend

import (
	"github.com/pborman/uuid"
	executionpb "go.temporal.io/temporal-proto/execution"
)

func validateExecution(w *executionpb.WorkflowExecution) error {
	if w == nil {
		return errExecutionNotSet
	}
	if w.GetWorkflowId() == "" {
		return errWorkflowIDNotSet
	}
	if w.GetRunId() != "" && uuid.Parse(w.GetRunId()) == nil {
		return errInvalidRunID
	}
	return nil
}
