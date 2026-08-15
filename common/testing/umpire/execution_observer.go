package umpire

import (
	"context"
	"errors"
)

// ExecutionObservationKind identifies a neutral action-window or verdict observation.
type ExecutionObservationKind string

const (
	ExecutionActionStart  ExecutionObservationKind = "action_start"
	ExecutionActionFinish ExecutionObservationKind = "action_finish"
	ExecutionVerdict      ExecutionObservationKind = "verdict"
)

const (
	ExecutionOutcomeStarted   = "started"
	ExecutionOutcomeSucceeded = "succeeded"
	ExecutionOutcomeRejected  = "rejected"
	ExecutionOutcomeFailed    = "failed"
)

// ExecutionObservation is the shared, transport-neutral record emitted by both Umpire runtimes.
type ExecutionObservation struct {
	Kind       ExecutionObservationKind
	Scope      string
	Action     string
	Property   string
	Phase      string
	Outcome    string
	ErrorClass string
	Checkpoint string
	Pass       bool
	Violations int
}

// MonitorSafetyProperty returns the stable property identity for a runtime safety checkpoint.
func MonitorSafetyProperty(checkpoint string) string {
	return "monitor-safety:" + checkpoint
}

// ActionEndpointProperty returns the stable property identity for an action endpoint check.
func ActionEndpointProperty(action string) string {
	return "action-endpoint:" + action
}

// ExecutionObserver receives action-window and verdict observations synchronously.
type ExecutionObserver interface {
	ObserveExecution(context.Context, ExecutionObservation) error
}

// ExecutionErrorClass reduces runtime errors to stable, non-secret categories.
func ExecutionErrorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "error"
	}
}
