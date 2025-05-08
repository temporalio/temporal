package tasks

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*CancelExecutionTask)(nil)

type (
	CancelExecutionTask struct {
		definition.WorkflowKey
		VisibilityTimestamp     time.Time
		TaskID                  int64
		TargetNamespaceID       string
		TargetWorkflowID        string
		TargetRunID             string
		TargetChildWorkflowOnly bool
		InitiatedEventID        int64
		Version                 int64
	}
)

func (u *CancelExecutionTask) GetKey() Key {
	return NewImmediateKey(u.TaskID)
}

func (u *CancelExecutionTask) GetVersion() int64 {
	return u.Version
}

func (u *CancelExecutionTask) SetVersion(version int64) {
	u.Version = version
}

func (u *CancelExecutionTask) GetTaskID() int64 {
	return u.TaskID
}

func (u *CancelExecutionTask) SetTaskID(id int64) {
	u.TaskID = id
}

func (u *CancelExecutionTask) GetVisibilityTime() time.Time {
	return u.VisibilityTimestamp
}

func (u *CancelExecutionTask) SetVisibilityTime(timestamp time.Time) {
	u.VisibilityTimestamp = timestamp
}

func (u *CancelExecutionTask) GetCategory() Category {
	return CategoryTransfer
}

func (u *CancelExecutionTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TRANSFER_CANCEL_EXECUTION
}
