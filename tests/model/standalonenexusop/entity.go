// Package standalonenexusop models the standalone Nexus operation lifecycle
// (StartNexusOperationExecution / Poll / Respond / Describe / Terminate /
// RequestCancel / Delete) as a property-test component.
package standalonenexusop

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
)

type Status int

const (
	StatusRunning Status = iota
	StatusCompleted
	StatusCanceled
	StatusTerminated
)

// Entity is the entity record stored in World.Operations.
type Entity struct {
	Namespace       string // FK -> Namespace.Name
	OperationID     string // unique within Namespace
	RequestID       string
	RunID           string
	Status          Status
	CancelRequested bool
}

// IsTerminal reports whether s is an absorbing status.
func IsTerminal(s Status) bool {
	return s == StatusCompleted || s == StatusCanceled || s == StatusTerminated
}

// ToProto translates the model status to the corresponding API enum.
func ToProto(s Status) enumspb.NexusOperationExecutionStatus {
	switch s {
	case StatusRunning:
		return enumspb.NEXUS_OPERATION_EXECUTION_STATUS_RUNNING
	case StatusCompleted:
		return enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED
	case StatusCanceled:
		return enumspb.NEXUS_OPERATION_EXECUTION_STATUS_CANCELED
	case StatusTerminated:
		return enumspb.NEXUS_OPERATION_EXECUTION_STATUS_TERMINATED
	default:
		panic(fmt.Sprintf("unknown standalone nexus operation status: %d", s))
	}
}

// TaskKind tags a Nexus task as either a start invocation or a cancellation.
type TaskKind string

const (
	TaskKindStart  TaskKind = "start"
	TaskKindCancel TaskKind = "cancel"
)

// TaskOutcome tags the lifecycle stage of a single task event.
type TaskOutcome string

const (
	TaskOutcomeDispatched TaskOutcome = "dispatched"
	TaskOutcomeCompleted  TaskOutcome = "completed"
)

// TaskEvent is a domain fact recorded by the driver: a task was dispatched
// or completed for an operation. Rules read these to enforce causality.
type TaskEvent struct {
	OperationID string
	Kind        TaskKind
	Outcome     TaskOutcome
}

func (e *TaskEvent) Key() string { return e.OperationID }
