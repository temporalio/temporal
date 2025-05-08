package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*DeleteHistoryEventTask)(nil)

type (
	DeleteHistoryEventTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
		BranchToken         []byte
		ProcessStage        DeleteWorkflowExecutionStage
	}
)

func (a *DeleteHistoryEventTask) GetKey() Key {
	return NewKey(a.VisibilityTimestamp, a.TaskID)
}

func (a *DeleteHistoryEventTask) GetVersion() int64 {
	return a.Version
}

func (a *DeleteHistoryEventTask) SetVersion(version int64) {
	a.Version = version
}

func (a *DeleteHistoryEventTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *DeleteHistoryEventTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *DeleteHistoryEventTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *DeleteHistoryEventTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

func (a *DeleteHistoryEventTask) GetCategory() Category {
	return CategoryTimer
}

func (a *DeleteHistoryEventTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_DELETE_HISTORY_EVENT
}
