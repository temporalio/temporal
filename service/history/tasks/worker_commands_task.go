package tasks

import (
	"fmt"
	"time"

	workerpb "go.temporal.io/api/worker/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/definition"
)

// The outbound queue allocates per-{TaskGroup, NamespaceID, Destination} in-memory resources
// (circuit breaker, rate limiter, worker pool). Using an empty destination groups all worker
// commands in a namespace under one key, bounding cardinality to O(namespaces) instead of
// O(worker instances) which would grow unboundedly with ephemeral worker churn.
const (
	WorkerCommandsTaskGroup       = "worker_commands"
	WorkerCommandsTaskDestination = ""
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
		// ControlQueue is the task queue to send worker commands to.
		ControlQueue string
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

// GetDestination returns WorkerCommandsTaskDestination (empty) so that worker
// commands are grouped by {WorkerCommandsTaskGroup, NamespaceID} only.
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
	return fmt.Sprintf("WorkerCommandsTask{WorkflowKey: %s, VisibilityTimestamp: %v, TaskID: %v, Commands: %d, ControlQueue: %v}",
		t.WorkflowKey.String(),
		t.VisibilityTimestamp,
		t.TaskID,
		len(t.Commands),
		t.ControlQueue,
	)
}
