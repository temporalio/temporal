package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*TimeSkippingTimerTask)(nil)

type (
	// TimeSkippingTimerTask wakes a workflow when the fast-forward configured
	// on its TimeSkippingConfig should take effect.
	TimeSkippingTimerTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		VersionedTransition *persistencespb.VersionedTransition
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

func (t *TimeSkippingTimerTask) SetVisibilityTime(visibilityTime time.Time) {
	t.VisibilityTimestamp = visibilityTime
}

func (t *TimeSkippingTimerTask) GetCategory() Category {
	return CategoryTimer
}

func (t *TimeSkippingTimerTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TIMESKIPPING_TIMER
}

func (t *TimeSkippingTimerTask) String() string {
	vt := t.VersionedTransition
	return fmt.Sprintf("TimeSkippingTimerTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, FailoverVersion: %v, TransitionCount: %v}",
		t.WorkflowKey.String(),
		t.VisibilityTimestamp,
		t.TaskID,
		vt.GetNamespaceFailoverVersion(),
		vt.GetTransitionCount(),
	)
}
