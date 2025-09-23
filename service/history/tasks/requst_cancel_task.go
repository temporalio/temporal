package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*CancelExecutionTask)(nil)

type (
	CancelExecutionTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		// Deprecated: the TargetNamespaceID from event instead.
		TargetNamespaceID string
		// Deprecated: the TargetWorkflowID from event instead.
		TargetWorkflowID string
		// Deprecated: the TargetRunID from event instead.
		TargetRunID string
		// Deprecated: the TargetChildWorkflowOnly from event instead.
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

func (u *CancelExecutionTask) String() string {
	return fmt.Sprintf("CancelExecutionTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, TargetNamespaceID: %v, TargetWorkflowID: %v, TargetRunID: %v, TargetChildWorkflowOnly: %v, InitiatedEventID: %v, Version: %v}",
		u.WorkflowKey.String(),
		u.VisibilityTimestamp,
		u.TaskID,
		u.TargetNamespaceID,
		u.TargetWorkflowID,
		u.TargetRunID,
		u.TargetChildWorkflowOnly,
		u.InitiatedEventID,
		u.Version,
	)
}
