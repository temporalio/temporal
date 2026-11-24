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

// Event describes one execution of an enabled failpoint.
type Event struct {
	Key    Key
	Action Action
	// Fired reports whether the failpoint fired, as opposed to being skipped by its Trigger.
	Fired bool
	// Execution is the 1-based number of this execution since the trigger was last set.
	Execution int64
	// TypeMismatch reports that the failpoint fired with a return value that the call site
	// could not accept, so the call site continued as if it had not fired.
	TypeMismatch bool
}
