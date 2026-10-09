package tasks

import (
	"fmt"
	"time"

	workerpb "go.temporal.io/api/worker/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

const (
	WorkerCommandsTaskGroup = "worker_commands"
	// WorkerCommandsTaskDestination is set to a fixed value so that all worker commands
	// for a namespace are handled by a single scheduler group, reducing the cardinality
	// of in-memory resources (circuit breaker, rate limiter, worker pool) from
	// O(worker instances) to O(namespaces).
	WorkerCommandsTaskDestination = "worker_commands"
)

var _ Task = (*WorkerCommandsTask)(nil)
var _ HasDestination = (*WorkerCommandsTask)(nil)
var _ HasOutboundTaskGroup = (*WorkerCommandsTask)(nil)

type (
	// WorkerCommandsTask sends commands to workers via Nexus.
	WorkerCommandsTask struct {
		definition.WorkflowKey
		VisibilityTimestamp time.Time
		TaskID              int64

		// Commands to send to the worker.
		Commands []*workerpb.WorkerCommand
		// Destination is the worker's control task queue.
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
	return enumsspb.TASK_TYPE_WORKER_COMMANDS
}

// GetDestination returns WorkerCommandsTaskDestination so that all worker
// commands in a namespace share a single scheduler group.
func (t *WorkerCommandsTask) GetDestination() string {
	return WorkerCommandsTaskDestination
}

// OutboundTaskGroup returns a dedicated task group for worker commands,
// isolating them from Nexus operation and callback tasks in the outbound
// queue scheduler.
func (t *WorkerCommandsTask) OutboundTaskGroup() string {
	return WorkerCommandsTaskGroup
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
