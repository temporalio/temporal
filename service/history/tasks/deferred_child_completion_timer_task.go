package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*DeferredChildCompletionTimerTask)(nil)

type (
	// DeferredChildCompletionTimerTask wakes a parent when its own clock reaches a
	// deferred child's close time, so the completion can be written without skipping
	// past in-flight work.
	DeferredChildCompletionTimerTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		InitiatedEventID    int64
	}
)

func (t *DeferredChildCompletionTimerTask) GetKey() Key {
	return NewKey(t.VisibilityTimestamp, t.TaskID)
}

func (t *DeferredChildCompletionTimerTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *DeferredChildCompletionTimerTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *DeferredChildCompletionTimerTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *DeferredChildCompletionTimerTask) SetVisibilityTime(visibilityTime time.Time) {
	t.VisibilityTimestamp = visibilityTime
}

func (t *DeferredChildCompletionTimerTask) GetCategory() Category {
	return CategoryTimer
}

func (t *DeferredChildCompletionTimerTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_DEFERRED_CHILD_COMPLETION_TIMER
}

func (t *DeferredChildCompletionTimerTask) String() string {
	return fmt.Sprintf("DeferredChildCompletionTimerTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, InitiatedEventID: %v}",
		t.WorkflowKey.String(),
		t.VisibilityTimestamp,
		t.TaskID,
		t.InitiatedEventID,
	)
}
