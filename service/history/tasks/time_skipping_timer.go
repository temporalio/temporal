package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*TimeSkippingTimerTask)(nil)

type (
	TimeSkippingTimerTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
	}
)

func (t *TimeSkippingTimerTask) GetKey() Key {
	return NewKey(t.VisibilityTimestamp, t.TaskID)
}

func (t *TimeSkippingTimerTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *TimeSkippingTimerTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *TimeSkippingTimerTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *TimeSkippingTimerTask) SetVisibilityTime(ts time.Time) {
	t.VisibilityTimestamp = ts
}

func (t *TimeSkippingTimerTask) GetCategory() Category {
	return CategoryTimer
}

func (t *TimeSkippingTimerTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TIME_SKIPPING
}

func (t *TimeSkippingTimerTask) String() string {
	return fmt.Sprintf("TimeSkippingTimerTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v}",
		t.WorkflowKey.String(),
		t.VisibilityTimestamp,
		t.TaskID,
	)
}
