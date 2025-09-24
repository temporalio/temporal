package tasks

import (
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*ActivityTimeoutTask)(nil)

type (
	ActivityTimeoutTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		TimeoutType         enumspb.TimeoutType
		EventID             int64
		Attempt             int32
		Stamp               int32
	}
)

func (a *ActivityTimeoutTask) GetKey() Key {
	return NewKey(a.VisibilityTimestamp, a.TaskID)
}

func (a *ActivityTimeoutTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *ActivityTimeoutTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *ActivityTimeoutTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *ActivityTimeoutTask) SetVisibilityTime(t time.Time) {
	a.VisibilityTimestamp = t
}

func (a *ActivityTimeoutTask) GetCategory() Category {
	return CategoryTimer
}

func (a *ActivityTimeoutTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_ACTIVITY_TIMEOUT
}

func (a *ActivityTimeoutTask) GetStamp() int32 {
	return a.Stamp
}

func (a *ActivityTimeoutTask) SetStamp(stamp int32) {
	a.Stamp = stamp
}

func (a *ActivityTimeoutTask) String() string {
	return fmt.Sprintf("ActivityTimeoutTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, TimeoutType: %v, EventID: %v, Attempt: %v, Stamp: %v}",
		a.WorkflowKey.String(),
		a.VisibilityTimestamp,
		a.TaskID,
		a.TimeoutType,
		a.EventID,
		a.Attempt,
		a.Stamp,
	)
}
