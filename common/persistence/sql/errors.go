package sql

import (
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

func extractCurrentWorkflowConflictError(
	currentRow *sqlplugin.CurrentExecutionsRow,
	message string,
) error {
	if currentRow == nil {
		return &p.CurrentWorkflowConditionFailedError{
			Msg:              message,
			RequestIDs:       nil,
			RunID:            "",
			State:            enumsspb.WORKFLOW_EXECUTION_STATE_UNSPECIFIED,
			Status:           enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED,
			LastWriteVersion: 0,
			StartTime:        nil,
		}
	}

	executionState, err := workflowExecutionStateFromCurrentExecutionsRow(currentRow)
	if err != nil {
		return err
	}

	return &p.CurrentWorkflowConditionFailedError{
		Msg:              message,
		RequestIDs:       executionState.RequestIds,
		RunID:            currentRow.RunID.String(),
		State:            currentRow.State,
		Status:           currentRow.Status,
		LastWriteVersion: currentRow.LastWriteVersion,
		StartTime:        currentRow.StartTime,
	}
}
