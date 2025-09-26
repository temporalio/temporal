package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*WorkflowRunTimeoutTask)(nil)

type (
	WorkflowRunTimeoutTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		Version             int64
	}
)

func (u *WorkflowRunTimeoutTask) GetKey() Key {
	return NewKey(u.VisibilityTimestamp, u.TaskID)
}

func (u *WorkflowRunTimeoutTask) GetVersion() int64 {
	return u.Version
}

func (u *WorkflowRunTimeoutTask) SetVersion(version int64) {
	u.Version = version
}

func (u *WorkflowRunTimeoutTask) GetTaskID() int64 {
	return u.TaskID
}

func (u *WorkflowRunTimeoutTask) SetTaskID(id int64) {
	u.TaskID = id
}

func (u *WorkflowRunTimeoutTask) GetVisibilityTime() time.Time {
	return u.VisibilityTimestamp
}

func (u *WorkflowRunTimeoutTask) SetVisibilityTime(t time.Time) {
	u.VisibilityTimestamp = t
}

func (u *WorkflowRunTimeoutTask) GetCategory() Category {
	return CategoryTimer
}

func (u *WorkflowRunTimeoutTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_WORKFLOW_RUN_TIMEOUT
}

func (u *WorkflowRunTimeoutTask) String() string {
	return fmt.Sprintf("WorkflowRunTimeoutTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, Version: %v}",
		u.WorkflowKey.String(),
		u.VisibilityTimestamp,
		u.TaskID,
		u.Version,
	)
}
