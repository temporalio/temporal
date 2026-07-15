package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*TimeSkippingFastForwardTimerTask)(nil)

type (
	// TimeSkippingFastForwardTimerTask wakes a workflow when the fast-forward configured
	// on its TimeSkippingConfig should take effect.
	//
	// EventID identifies the event (WorkflowExecutionStartedEvent or
	// WorkflowExecutionOptionsUpdatedEvent) that installed the fast-forward this task targets.
	// It is matched against TimeSkippingInfo.FastForward.SourceEventId at
	// firing time to detect superseded tasks: re-configuring the fast-forward emits a new task
	// with a new EventID, and the old task is silently dropped on mismatch.
	TimeSkippingFastForwardTimerTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		EventID             int64
	}
)

func (t *TimeSkippingFastForwardTimerTask) GetKey() Key {
	return NewKey(t.VisibilityTimestamp, t.TaskID)
}

func (t *TimeSkippingFastForwardTimerTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *TimeSkippingFastForwardTimerTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *TimeSkippingFastForwardTimerTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *TimeSkippingFastForwardTimerTask) SetVisibilityTime(visibilityTime time.Time) {
	t.VisibilityTimestamp = visibilityTime
}

func (t *TimeSkippingFastForwardTimerTask) GetCategory() Category {
	return CategoryTimer
}

func (t *TimeSkippingFastForwardTimerTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TIMESKIPPING_TIMER
}

func (t *TimeSkippingFastForwardTimerTask) String() string {
	return fmt.Sprintf("TimeSkippingTimerTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, EventID: %v}",
		t.WorkflowKey.String(),
		t.VisibilityTimestamp,
		t.TaskID,
		t.EventID,
	)
}
