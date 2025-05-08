package hsm

import (
	"errors"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
)

// ErrNotCherryPickable should be returned by CherryPick if an event should not be cherry picked for whatever reason.
var ErrNotCherryPickable = errors.New("event not cherry pickable")

// EventDefinition is a definition for a history event for a given event type.
type EventDefinition interface {
	Type() enumspb.EventType
	// IsWorkflowTaskTrigger returns a boolean indicating whether this event type should trigger a workflow task.
	IsWorkflowTaskTrigger() bool
	// Apply a history event to the state machine. Triggered during replication and workflow reset.
	Apply(root *Node, event *historypb.HistoryEvent) error
	// Cherry pick (a.k.a "reapply") an event from a different history branch.
	// Implementations should apply the event to the machine state and return nil in case the event is cherry-pickable.
	// Command events should never be cherry picked as we rely on the workflow to reschedule them.
	// Return [ErrNotCherryPickable], [ErrStateMachineNotFound], or [ErrInvalidTransition] to skip cherry picking. Any
	// other error is considered fatal and will abort the cherry pick process.
	CherryPick(root *Node, event *historypb.HistoryEvent, resetReapplyExcludeTypes map[enumspb.ResetReapplyExcludeType]struct{}) error
}
