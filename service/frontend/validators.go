package frontend

import (
	"github.com/pborman/uuid"
	commonproto "go.temporal.io/temporal-proto/common"
)

func validateExecution(w *commonproto.WorkflowExecution) error {
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
