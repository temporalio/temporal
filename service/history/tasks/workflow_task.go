package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*WorkflowTask)(nil)

type (
	WorkflowTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		TaskQueue           string
		ScheduledEventID    int64
		Version             int64
		Stamp               int32
	}
)

func (d *WorkflowTask) GetKey() Key {
	return NewImmediateKey(d.TaskID)
}

func (d *WorkflowTask) GetVersion() int64 {
	return d.Version
}

func (d *WorkflowTask) SetVersion(version int64) {
	d.Version = version
}

func (d *WorkflowTask) GetTaskID() int64 {
	return d.TaskID
}

func (d *WorkflowTask) SetTaskID(id int64) {
	d.TaskID = id
}

func (d *WorkflowTask) GetVisibilityTime() time.Time {
	return d.VisibilityTimestamp
}

func (d *WorkflowTask) SetVisibilityTime(timestamp time.Time) {
	d.VisibilityTimestamp = timestamp
}

func (d *WorkflowTask) GetCategory() Category {
	return CategoryTransfer
}

func (d *WorkflowTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TRANSFER_WORKFLOW_TASK
}

func (d *WorkflowTask) String() string {
	return fmt.Sprintf("WorkflowTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, TaskQueue: %v, ScheduledEventID: %v, Version: %v}",
		d.WorkflowKey.String(),
		d.VisibilityTimestamp,
		d.TaskID,
		d.TaskQueue,
		d.ScheduledEventID,
		d.Version,
	)
}
