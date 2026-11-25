package failpoint

import "fmt"

// Action is what a failpoint does when it executes. The values match the FailpointAction proto
// enum.
type Action int32

const (
	Unspecified Action = iota
	Off
	Return
	Panic
	Block
	Pause
	Call
	// On fires without a value. Switch-style call sites check it with Active.
	On
)

var actionNames = [...]string{"Unspecified", "Off", "Return", "Panic", "Block", "Pause", "Call", "On"}

func (a Action) String() string {
	if a >= 0 && int(a) < len(actionNames) {
		return actionNames[a]
	}
	return fmt.Sprintf("Action(%d)", int32(a))
}

// EventKind is what an Event reports.
type EventKind int32

const (
	// ExecutionEvent reports an execution of an enabled failpoint.
	ExecutionEvent EventKind = iota
	// FallbackActivationEvent reports that a fallback (see Fallback) decided whether it acts on a
	// key name, on the first execution of the name. Key is unscoped, and Activated is the decision.
	FallbackActivationEvent
	// FallbackFireEvent reports an execution of a key that a fallback fired. Action is Return if
	// it injected an error and On if a call site takes its branch; Execution is the number of the
	// execution of the key name in the key's scope.
	FallbackFireEvent
)

// Event describes one execution of an enabled failpoint, or a decision of a fallback.
type Event struct {
	// Kind is ExecutionEvent, the zero value, unless a fallback reports the event.
	Kind   EventKind
	Key    Key
	Action Action
	// Fired reports whether the failpoint fired, as opposed to being skipped by its Trigger.
	Fired bool
	// Execution is the 1-based number of this execution since the trigger was last set.
	Execution int64
	// TypeMismatch reports that the failpoint fired with a return value that the call site
	// could not accept, so the call site continued as if it had not fired.
	TypeMismatch bool
	// Activated is the decision of a FallbackActivationEvent.
	Activated bool
}
