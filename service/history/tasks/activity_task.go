package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*ActivityTask)(nil)

type (
	ActivityTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		TaskQueue           string
		ScheduledEventID    int64
		Version             int64
		Stamp               int32
	}
)

func (a *ActivityTask) GetKey() Key {
	return NewImmediateKey(a.TaskID)
}

func (a *ActivityTask) GetVersion() int64 {
	return a.Version
}

func (a *ActivityTask) SetVersion(version int64) {
	a.Version = version
}

func (a *ActivityTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *ActivityTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *ActivityTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *ActivityTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

func (a *ActivityTask) GetCategory() Category {
	return CategoryTransfer
}

func (a *ActivityTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_TASK
}
