package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*TimeSkippingTimerTask)(nil)

type (
	// TimeSkippingTimerTask wakes a workflow when the elapsed-duration bound configured
	// on its TimeSkippingConfig is reached, so the executor can emit a disable transition.
	//
	// EventID identifies the event (WorkflowExecutionStartedEvent or
	// WorkflowExecutionOptionsUpdatedEvent) that installed the bound this task targets.
	// It is matched against TimeSkippingInfo.CurrentElapsedDurationBound.SourceEventId at
	// firing time to detect superseded tasks: re-configuring the bound emits a new task
	// with a new EventID, and the old task is silently dropped on mismatch.
	// todo@time-skipping: replication related feature (ndc)
	TimeSkippingTimerTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		EventID             int64
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
	return fmt.Sprintf("TimeSkippingTimerTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, EventID: %v}",
		t.WorkflowKey.String(),
		t.VisibilityTimestamp,
		t.TaskID,
		t.EventID,
	)
}
