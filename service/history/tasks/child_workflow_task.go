package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*StartChildExecutionTask)(nil)

type (
	StartChildExecutionTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		// Deprecated: Get TargetNamespaceID from mutable state.
		TargetNamespaceID string
		// Deprecated: Get TargetWorkflowID from mutable state.
		TargetWorkflowID string
		InitiatedEventID int64
		Version          int64
	}
)

func (u *StartChildExecutionTask) GetKey() Key {
	return NewImmediateKey(u.TaskID)
}

func (u *StartChildExecutionTask) GetVersion() int64 {
	return u.Version
}

func (u *StartChildExecutionTask) SetVersion(version int64) {
	u.Version = version
}

func (u *StartChildExecutionTask) GetTaskID() int64 {
	return u.TaskID
}

func (u *StartChildExecutionTask) SetTaskID(id int64) {
	u.TaskID = id
}

func (u *StartChildExecutionTask) GetVisibilityTime() time.Time {
	return u.VisibilityTimestamp
}

func (u *StartChildExecutionTask) SetVisibilityTime(timestamp time.Time) {
	u.VisibilityTimestamp = timestamp
}

func (u *StartChildExecutionTask) GetCategory() Category {
	return CategoryTransfer
}

func (u *StartChildExecutionTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TRANSFER_START_CHILD_EXECUTION
}

func (u *StartChildExecutionTask) String() string {
	return fmt.Sprintf("StartChildExecutionTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, TargetNamespaceID: %v, TargetWorkflowID: %v, InitiatedEventID: %v, Version: %v}",
		u.WorkflowKey.String(),
		u.VisibilityTimestamp,
		u.TaskID,
		u.TargetNamespaceID,
		u.TargetWorkflowID,
		u.InitiatedEventID,
		u.Version,
	)
}
