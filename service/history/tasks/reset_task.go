package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*ResetWorkflowTask)(nil)

type (
	ResetWorkflowTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
	}
)

func (a *ResetWorkflowTask) GetKey() Key {
	return NewImmediateKey(a.TaskID)
}

func (a *ResetWorkflowTask) GetVersion() int64 {
	return a.Version
}

func (a *ResetWorkflowTask) SetVersion(version int64) {
	a.Version = version
}

func (a *ResetWorkflowTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *ResetWorkflowTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *ResetWorkflowTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *ResetWorkflowTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

func (a *ResetWorkflowTask) GetCategory() Category {
	return CategoryTransfer
}

func (a *ResetWorkflowTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TRANSFER_RESET_WORKFLOW
}
