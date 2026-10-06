package workflow

import (
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/chasm"
)

// EventStore is the history event store that workflow components add events to. MSPointer
// implements it with mutable state.
type EventStore interface {
	// AddHistoryEvent adds an event to history. While a workflow task is started the event is
	// buffered: its ID is common.BufferedEventID until the workflow task closes.
	AddHistoryEvent(t enumspb.EventType, setAttributes func(*historypb.HistoryEvent)) *historypb.HistoryEvent
}

var _ EventStore = chasm.MSPointer{}

func (w *Workflow) eventStore(_ chasm.MutableContext) EventStore {
	return w.MSPointer
}
