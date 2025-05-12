package frontend

import (
	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
)

func validateExecution(w *commonpb.WorkflowExecution) error {
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
