package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common"
)

var _ Task = (*WorkflowRunTimeoutTask)(nil)

type (
	WorkflowExecutionTimeoutTask struct {
		NamespaceID string
		WorkflowID  string
		FirstRunID  string

		VisibilityTimestamp time.Time
		TaskID              int64

		// Check the comment in timerQueueTaskExecutorBase.isValidExecutionTimeoutTask()
		// for why version is not needed here
	}
)

func (t *WorkflowExecutionTimeoutTask) GetNamespaceID() string {
	return t.NamespaceID
}

func (t *WorkflowExecutionTimeoutTask) GetWorkflowID() string {
	return t.WorkflowID
}

func (t *WorkflowExecutionTimeoutTask) GetRunID() string {
	// RunID is empty as the task is not for a specific run but a workflow chain
	return ""
}

func (t *WorkflowExecutionTimeoutTask) GetKey() Key {
	return NewKey(t.VisibilityTimestamp, t.TaskID)
}

func (t *WorkflowExecutionTimeoutTask) GetVersion() int64 {
	return common.EmptyVersion
}

func (t *WorkflowExecutionTimeoutTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *WorkflowExecutionTimeoutTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *WorkflowExecutionTimeoutTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *WorkflowExecutionTimeoutTask) SetVisibilityTime(visibilityTime time.Time) {
	t.VisibilityTimestamp = visibilityTime
}

func (t *WorkflowExecutionTimeoutTask) GetCategory() Category {
	return CategoryTimer
}

func (t *WorkflowExecutionTimeoutTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_WORKFLOW_EXECUTION_TIMEOUT
}

func (t *WorkflowExecutionTimeoutTask) String() string {
	return fmt.Sprintf("WorkflowExecutionTimeoutTask{NamespaceID: %v, WorkflowID: %v, FirstRunID: %v, VisibilityTimestamp: %v, TaskID: %v}",
		t.NamespaceID,
		t.WorkflowID,
		t.FirstRunID,
		t.VisibilityTimestamp,
		t.TaskID,
	)
}
