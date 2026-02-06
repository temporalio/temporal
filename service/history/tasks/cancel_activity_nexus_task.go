package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*CancelActivityNexusTask)(nil)

type (
	CancelActivityNexusTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64

		// ScheduledEventIDs of activities to cancel (batched by worker).
		ScheduledEventIDs []int64
		WorkerInstanceKey string
	}
)

func (t *CancelActivityNexusTask) GetKey() Key {
	return NewImmediateKey(t.TaskID)
}

func (t *CancelActivityNexusTask) GetVersion() int64 {
	return t.Version
}

func (t *CancelActivityNexusTask) SetVersion(version int64) {
	t.Version = version
}

func (t *CancelActivityNexusTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *CancelActivityNexusTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *CancelActivityNexusTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *CancelActivityNexusTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

func (t *CancelActivityNexusTask) GetCategory() Category {
	return CategoryTransfer
}

func (t *CancelActivityNexusTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TRANSFER_CANCEL_ACTIVITY_NEXUS
}

func (t *CancelActivityNexusTask) String() string {
	return fmt.Sprintf("CancelActivityNexusTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, ScheduledEventIDs: %v, WorkerInstanceKey: %v, Version: %v}",
		t.WorkflowKey.String(),
		t.VisibilityTimestamp,
		t.TaskID,
		t.ScheduledEventIDs,
		t.WorkerInstanceKey,
		t.Version,
	)
}
