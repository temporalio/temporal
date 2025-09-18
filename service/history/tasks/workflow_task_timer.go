package tasks

import (
	"fmt"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
	ctasks "go.temporal.io/server/common/tasks"
)

const (
	// SpeculativeWorkflowTaskScheduleToStartTimeout is the timeout for a speculative workflow task on a normal task queue.
	// Default ScheduleToStart timeout for a sticky task queue is 5 seconds.
	// Setting this value also to 5 seconds to match the sticky queue timeout.
	SpeculativeWorkflowTaskScheduleToStartTimeout = 5 * time.Second
)

var _ Task = (*WorkflowTaskTimeoutTask)(nil)

type (
	WorkflowTaskTimeoutTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64
		EventID             int64
		ScheduleAttempt     int32
		TimeoutType         enumspb.TimeoutType
		Version             int64
		Stamp               int32

		// InMemory field is not persisted in the database.
		InMemory bool

		// state is used by speculative WT only.
		state atomic.Uint32 // of type ctasks.State
	}
)

func (d *WorkflowTaskTimeoutTask) GetKey() Key {
	return NewKey(d.VisibilityTimestamp, d.TaskID)
}

func (d *WorkflowTaskTimeoutTask) GetVersion() int64 {
	return d.Version
}

func (d *WorkflowTaskTimeoutTask) SetVersion(version int64) {
	d.Version = version
}

func (d *WorkflowTaskTimeoutTask) GetTaskID() int64 {
	return d.TaskID
}

func (d *WorkflowTaskTimeoutTask) SetTaskID(id int64) {
	d.TaskID = id
}

func (d *WorkflowTaskTimeoutTask) GetVisibilityTime() time.Time {
	return d.VisibilityTimestamp
}

func (d *WorkflowTaskTimeoutTask) SetVisibilityTime(t time.Time) {
	d.VisibilityTimestamp = t
}

func (d *WorkflowTaskTimeoutTask) GetCategory() Category {
	if d.InMemory {
		return CategoryMemoryTimer
	}
	return CategoryTimer
}

func (d *WorkflowTaskTimeoutTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_WORKFLOW_TASK_TIMEOUT
}

// Cancel and State are used by in-memory WorkflowTaskTimeoutTask (for speculative WT) only.
// TODO (alex): They need to be moved to speculativeWorkflowTaskTimeoutExecutable
// and workflowTaskStateMachine should somehow signal that executable directly.
// Major refactoring needs to be done to achieve that.
func (d *WorkflowTaskTimeoutTask) Cancel() {
	d.state.Store(uint32(ctasks.TaskStateCancelled))
}
func (d *WorkflowTaskTimeoutTask) State() ctasks.State {
	return ctasks.State(d.state.Load())
}

func (d *WorkflowTaskTimeoutTask) String() string {
	return fmt.Sprintf("WorkflowTaskTimeoutTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, EventID: %v, ScheduleAttempt: %v, TimeoutType: %v, Version: %v, InMemory: %v}",
		d.WorkflowKey.String(),
		d.VisibilityTimestamp,
		d.TaskID,
		d.EventID,
		d.ScheduleAttempt,
		d.TimeoutType,
		d.Version,
		d.InMemory,
	)
}
