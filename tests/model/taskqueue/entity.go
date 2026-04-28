// Package taskqueue models Temporal task queue lifecycle as a property-test
// component. Task queues come into existence implicitly when a worker polls
// or a workflow is scheduled on them; the observer reflects that into
// World.TaskQueues from any RPC carrying a task queue.
package taskqueue

import enumspb "go.temporal.io/api/enums/v1"

type Status int

const (
	StatusRegistered Status = iota
)

// Entity is the entity record stored in World.TaskQueues.
type Entity struct {
	Name      string
	Namespace string
	Kind      enumspb.TaskQueueKind
	Type      enumspb.TaskQueueType
	Status    Status
}
