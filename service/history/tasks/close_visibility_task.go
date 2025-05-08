package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*CloseExecutionVisibilityTask)(nil)

type (
	CloseExecutionVisibilityTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
	}
)

func (t *CloseExecutionVisibilityTask) GetKey() Key {
	return NewImmediateKey(t.TaskID)
}

func (t *CloseExecutionVisibilityTask) GetVersion() int64 {
	return t.Version
}

func (t *CloseExecutionVisibilityTask) SetVersion(version int64) {
	t.Version = version
}

func (t *CloseExecutionVisibilityTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *CloseExecutionVisibilityTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *CloseExecutionVisibilityTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *CloseExecutionVisibilityTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

func (t *CloseExecutionVisibilityTask) GetCategory() Category {
	return CategoryVisibility
}

func (t *CloseExecutionVisibilityTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_VISIBILITY_CLOSE_EXECUTION
}
