package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*ArchiveExecutionTask)(nil)

type (
	// ArchiveExecutionTask is the task which archives both the history and visibility of a workflow execution and then
	// produces a retention timer task to delete the data.
	ArchiveExecutionTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
	}
)

func (a *ArchiveExecutionTask) GetKey() Key {
	return NewKey(a.VisibilityTimestamp, a.TaskID)
}

func (a *ArchiveExecutionTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *ArchiveExecutionTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *ArchiveExecutionTask) GetVersion() int64 {
	return a.Version
}

func (a *ArchiveExecutionTask) GetCategory() Category {
	return CategoryArchival
}

func (a *ArchiveExecutionTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_ARCHIVAL_ARCHIVE_EXECUTION
}

func (a *ArchiveExecutionTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *ArchiveExecutionTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}
