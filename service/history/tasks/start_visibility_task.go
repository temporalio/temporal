package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*StartExecutionVisibilityTask)(nil)

type (
	StartExecutionVisibilityTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
	}
)

func (t *StartExecutionVisibilityTask) GetKey() Key {
	return NewImmediateKey(t.TaskID)
}

func (t *StartExecutionVisibilityTask) GetVersion() int64 {
	return t.Version
}

func (t *StartExecutionVisibilityTask) SetVersion(version int64) {
	t.Version = version
}

func (t *StartExecutionVisibilityTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *StartExecutionVisibilityTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *StartExecutionVisibilityTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *StartExecutionVisibilityTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

func (t *StartExecutionVisibilityTask) GetCategory() Category {
	return CategoryVisibility
}

func (t *StartExecutionVisibilityTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_VISIBILITY_START_EXECUTION
}
