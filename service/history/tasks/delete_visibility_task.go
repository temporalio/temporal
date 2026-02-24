package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*DeleteExecutionVisibilityTask)(nil)
var _ HasArchetypeID = (*DeleteExecutionVisibilityTask)(nil)

type (
	DeleteExecutionVisibilityTask struct {
		definition.WorkflowKey
		VisibilityTimestamp            time.Time
		TaskID                         int64
		ArchetypeID                    uint32
		CloseExecutionVisibilityTaskID int64
		CloseTime                      time.Time
	}
)

func (t *DeleteExecutionVisibilityTask) GetKey() Key {
	return NewImmediateKey(t.TaskID)
}

func (t *DeleteExecutionVisibilityTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *DeleteExecutionVisibilityTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *DeleteExecutionVisibilityTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *DeleteExecutionVisibilityTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

func (t *DeleteExecutionVisibilityTask) GetCategory() Category {
	return CategoryVisibility
}

func (t *DeleteExecutionVisibilityTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_VISIBILITY_DELETE_EXECUTION
}

func (t *DeleteExecutionVisibilityTask) GetArchetypeID() uint32 {
	return t.ArchetypeID
}
