package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*UserTimerTask)(nil)

type (
	UserTimerTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		EventID             int64
	}
)

func (u *UserTimerTask) GetKey() Key {
	return NewKey(u.VisibilityTimestamp, u.TaskID)
}

func (u *UserTimerTask) GetTaskID() int64 {
	return u.TaskID
}

func (u *UserTimerTask) SetTaskID(id int64) {
	u.TaskID = id
}

func (u *UserTimerTask) GetVisibilityTime() time.Time {
	return u.VisibilityTimestamp
}

func (u *UserTimerTask) SetVisibilityTime(t time.Time) {
	u.VisibilityTimestamp = t
}

func (u *UserTimerTask) GetCategory() Category {
	return CategoryTimer
}

func (u *UserTimerTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_USER_TIMER
}

func (u *UserTimerTask) String() string {
	return fmt.Sprintf("UserTimerTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, EventID: %v}",
		u.WorkflowKey.String(),
		u.VisibilityTimestamp,
		u.TaskID,
		u.EventID,
	)
}
