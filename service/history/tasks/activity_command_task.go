package tasks

import (
	"fmt"
	"time"

	workerpb "go.temporal.io/api/worker/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

var _ Task = (*WorkerCommandsTask)(nil)
var _ HasDestination = (*WorkerCommandsTask)(nil)

type (
	// WorkerCommandsTask sends commands to workers via Nexus.
	WorkerCommandsTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64

		// Commands to send to the worker.
		Commands []*workerpb.WorkerCommand
		// Destination is the worker control task queue for outbound queue grouping.
		Destination string
	}
)

func (t *WorkerCommandsTask) GetKey() Key {
	return NewImmediateKey(t.TaskID)
}

func (t *WorkerCommandsTask) GetTaskID() int64 {
	return t.TaskID
}

func (t *WorkerCommandsTask) SetTaskID(id int64) {
	t.TaskID = id
}

func (t *WorkerCommandsTask) GetVisibilityTime() time.Time {
	return t.VisibilityTimestamp
}

func (t *WorkerCommandsTask) SetVisibilityTime(timestamp time.Time) {
	t.VisibilityTimestamp = timestamp
}

func (t *WorkerCommandsTask) GetCategory() Category {
	return CategoryOutbound
}

func (t *WorkerCommandsTask) GetType() enumsspb.TaskType {
	return enumsspb.TASK_TYPE_ACTIVITY_COMMAND
}

// GetDestination implements HasDestination for outbound queue grouping.
func (t *WorkerCommandsTask) GetDestination() string {
	return t.Destination
}

func (t *WorkerCommandsTask) String() string {
	return fmt.Sprintf("WorkerCommandsTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, Commands: %d, Destination: %v}",
		t.WorkflowKey.String(),
		t.VisibilityTimestamp,
		t.TaskID,
		len(t.Commands),
		t.Destination,
	)
}
