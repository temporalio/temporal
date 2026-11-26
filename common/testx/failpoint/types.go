package failpoint

import "fmt"

// Action is what a failpoint does when it executes.
type Action int32

const (
	ActionUnspecified Action = iota
	ActionOff
	ActionReturn
	// ActionOn activates a boolean switch failpoint (e.g. "skip sync match" or "use smaller batch")
	// without returning a specific value or error. Call sites check it with ExecuteSwitch.
	ActionOn
)

func (a Action) String() string {
	switch a {
	case ActionUnspecified:
		return "Unspecified"
	case ActionOff:
		return "Off"
	case ActionReturn:
		return "Return"
	case ActionOn:
		return "On"
	default:
		return fmt.Sprintf("Action(%d)", int32(a))
	}
}
