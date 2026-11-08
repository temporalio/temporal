package workflow

import (
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/tasktoken"
)

// NewStartedWorkflowTaskToken rebuilds the task token given to the worker for the started workflow task.
// It returns nil if no workflow task is started or its started clock is unknown.
func NewStartedWorkflowTaskToken(key definition.WorkflowKey, info *persistencespb.WorkflowExecutionInfo) *tokenspb.Task {
	if info.GetWorkflowTaskStartedEventId() == common.EmptyEventID || info.GetWorkflowTaskStartedClock() == nil {
		return nil
	}
	return tasktoken.NewWorkflowTaskToken(
		key.NamespaceID,
		key.WorkflowID,
		key.RunID,
		info.GetWorkflowTaskScheduledEventId(),
		info.GetWorkflowTaskStartedEventId(),
		info.GetWorkflowTaskStartedTime(),
		info.GetWorkflowTaskAttempt(),
		info.GetWorkflowTaskStartedClock(),
		info.GetWorkflowTaskVersion(),
	)
}
