package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*CloseExecutionTask)(nil)

type (
	CloseExecutionTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
		DeleteAfterClose    bool
		DeleteProcessStage  DeleteWorkflowExecutionStage
	}
)

func (a *CloseExecutionTask) GetKey() Key {
	return NewImmediateKey(a.TaskID)
}

func (a *CloseExecutionTask) GetVersion() int64 {
	return a.Version
}

func (a *CloseExecutionTask) SetVersion(version int64) {
	a.Version = version
}

func (a *CloseExecutionTask) GetTaskID() int64 {
	return a.TaskID
}

func (a *CloseExecutionTask) SetTaskID(id int64) {
	a.TaskID = id
}

func (a *CloseExecutionTask) GetVisibilityTime() time.Time {
	return a.VisibilityTimestamp
}

func (a *CloseExecutionTask) SetVisibilityTime(timestamp time.Time) {
	a.VisibilityTimestamp = timestamp
}

func (a *CloseExecutionTask) GetCategory() Category {
	return CategoryTransfer
}

func (a *CloseExecutionTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_TRANSFER_CLOSE_EXECUTION
}

func (a *CloseExecutionTask) String() string {
	return fmt.Sprintf("CloseExecutionTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, Version: %v, DeleteAfterClose: %v, DeleteProcessStage: %v}",
		a.WorkflowKey.String(),
		a.VisibilityTimestamp,
		a.TaskID,
		a.Version,
		a.DeleteAfterClose,
		a.DeleteProcessStage,
	)
}
