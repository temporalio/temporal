package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*ActivityCancelControlTask)(nil)

type (
	ActivityCancelControlTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64

		// ScheduledEventIDs of activities to cancel (batched by worker).
		ScheduledEventIDs []int64
		WorkerInstanceKey string
	}
)

func (t *ActivityCancelControlTask) GetKey() Key {
	return NewImmediateKey(t.TaskID)
}

func (t *ActivityCancelControlTask) GetVersion() int64 {
	return t.Version
}

func (t *ActivityCancelControlTask) SetVersion(version int64) {
	t.Version = version
}

func (t *ActivityCancelControlTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *ActivityCancelControlTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *ActivityCancelControlTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *ActivityCancelControlTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

func (t *ActivityCancelControlTask) GetCategory() Category {
	return CategoryTransfer
}

func (t *ActivityCancelControlTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TRANSFER_ACTIVITY_CANCEL_CONTROL
}

func (t *ActivityCancelControlTask) String() string {
	return fmt.Sprintf("ActivityCancelControlTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, ScheduledEventIDs: %v, WorkerInstanceKey: %v, Version: %v}",
		t.WorkflowKey.String(),
		t.VisibilityTimestamp,
		t.TaskID,
		t.ScheduledEventIDs,
		t.WorkerInstanceKey,
		t.Version,
	)
}
