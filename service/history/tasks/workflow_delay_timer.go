package tasks

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*WorkflowBackoffTimerTask)(nil)

type (
	WorkflowBackoffTimerTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		// Version is included for task versioning infrastructure but is not
		// explicitly checked against workflow start version in the timer task
		// executors. For standby execution, this is safe because this task only
		// fires when mutable state's next event ID is 2 (workflow started event
		// only), meaning the start version is immutable and guaranteed to match.
		// See executeWorkflowBackoffTimerTask in timer_queue_standby_task_executor.go.
		Version             int64
		WorkflowBackoffType enumsspb.WorkflowBackoffType
	}
)

func (r *WorkflowBackoffTimerTask) GetKey() Key {
	return NewKey(r.VisibilityTimestamp, r.TaskID)
}

func (r *WorkflowBackoffTimerTask) GetVersion() int64 {
	return r.Version
}

func (r *WorkflowBackoffTimerTask) SetVersion(version int64) {
	r.Version = version
}

func (r *WorkflowBackoffTimerTask) GetTaskID() int64 {
	return r.TaskID
}

func (r *WorkflowBackoffTimerTask) SetTaskID(id int64) {
	r.TaskID = id
}

func (r *WorkflowBackoffTimerTask) GetVisibilityTime() time.Time {
	return r.VisibilityTimestamp
}

func (r *WorkflowBackoffTimerTask) SetVisibilityTime(t time.Time) {
	r.VisibilityTimestamp = t
}

func (r *WorkflowBackoffTimerTask) GetCategory() Category {
	return CategoryTimer
}

func (r *WorkflowBackoffTimerTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_WORKFLOW_BACKOFF_TIMER
}

func (r *WorkflowBackoffTimerTask) String() string {
	return fmt.Sprintf("WorkflowBackoffTimerTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, Version: %v, WorkflowBackoffType: %v}",
		r.WorkflowKey.String(),
		r.VisibilityTimestamp,
		r.TaskID,
		r.Version,
		r.WorkflowBackoffType,
	)
}
