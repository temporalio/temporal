package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*SignalExecutionTask)(nil)

type (
	SignalExecutionTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		// Deprecated: Get TargetNamespaceID from event instead.
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

func (u *SignalExecutionTask) GetKey() Key {
	return NewImmediateKey(u.TaskID)
}

func (u *SignalExecutionTask) GetVersion() int64 {
	return u.Version
}

func (u *SignalExecutionTask) SetVersion(version int64) {
	u.Version = version
}

func (u *SignalExecutionTask) GetTaskID() int64 {
	return u.TaskID
}

func (u *SignalExecutionTask) SetTaskID(id int64) {
	u.TaskID = id
}

func (u *SignalExecutionTask) GetVisibilityTime() time.Time {
	return u.VisibilityTimestamp
}

func (u *SignalExecutionTask) SetVisibilityTime(timestamp time.Time) {
	u.VisibilityTimestamp = timestamp
}

func (u *SignalExecutionTask) GetCategory() Category {
	return CategoryTransfer
}

func (u *SignalExecutionTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TRANSFER_SIGNAL_EXECUTION
}

func (u *SignalExecutionTask) String() string {
	return fmt.Sprintf("SignalExecutionTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, TargetNamespaceID: %v, TargetWorkflowID: %v, TargetRunID: %v, TargetChildWorkflowOnly: %v, InitiatedEventID: %v, Version: %v}",
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
