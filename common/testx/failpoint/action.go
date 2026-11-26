package failpoint

import "fmt"

// Action is what a failpoint does when it executes.
type Action int32

const (
	// ActionNone means that no action is configured: the failpoint is disabled or was never
	// configured, so its executions don't fire.
	ActionNone Action = iota
	// ActionOn activates a boolean switch failpoint (e.g. "skip cache" or "use smaller batch")
	// without returning a specific value or error. Call sites check it with Trip.
	ActionOn
)

func (a Action) String() string {
	switch a {
	case ActionNone:
		return "None"
	case ActionOn:
		return "On"
	default:
		return fmt.Sprintf("Action(%d)", int32(a))
	}
}
