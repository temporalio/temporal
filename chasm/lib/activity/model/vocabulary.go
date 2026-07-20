// Package model is a test-only behavioral model of the CHASM activity archetype: an
// implementation-independent description of how an activity should behave. It should never be
// imported by the server binary.
//
// Drivers can be written that realize these events for Standalone Activity or for Workflow Activity.
package model

// Config contains start-time activity options.
type Config struct {
	MaxAttempts int32 // 0 = unlimited
}

// EventKind enumerates the events the model covers.
type EventKind int

const (
	Poll EventKind = iota
	RespondCompleted
	RespondFailed
	BackoffElapses
	Pause
)

// Event is an EventKind together with flags describing the RPC
type Event struct {
	Kind EventKind

	Retryable bool // RespondFailed: the failure is retryable.
}
