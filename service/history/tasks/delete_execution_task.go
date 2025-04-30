package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*DeleteExecutionTask)(nil)

type (
	DeleteExecutionTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64

		ProcessStage DeleteWorkflowExecutionStage
	}
)

func (a *DeleteExecutionTask) GetKey() Key {
	return NewImmediateKey(a.TaskID)
}

func (a *DeleteExecutionTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *DeleteExecutionTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *DeleteExecutionTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *DeleteExecutionTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

func (a *DeleteExecutionTask) GetCategory() Category {
	return CategoryTransfer
}

func (a *DeleteExecutionTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TRANSFER_DELETE_EXECUTION
}
