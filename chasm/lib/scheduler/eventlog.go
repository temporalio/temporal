package scheduler

import (
	"slices"
	"unicode/utf8"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EventLog is a CHASM component that keeps a bounded, human-readable history
// of state changes for its parent component. Entries are not used for any
// scheduler computation.
type EventLog struct {
	chasm.UnimplementedComponent

	*schedulerpb.EventLog
}

// NewEventLog returns an initialized EventLog component, intended to be parented
// under any component that wants to record events.
func NewEventLog(ctx chasm.MutableContext) *EventLog {
	return &EventLog{
		EventLog: &schedulerpb.EventLog{},
	}
}

func (e *EventLog) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

// LogEvent appends an event with the given message. Messages longer than the
// configured maximum length are truncated at a UTF-8 rune boundary; once the
// log exceeds the configured maximum entries, the earliest entries are dropped.
func (e *EventLog) LogEvent(ctx chasm.MutableContext, msg string) {
	tw := tweakablesFromContext(ctx)
	maxEntries, maxMessageLen := tw.EventLogMaxEntries, tw.EventLogMaxMessageLen

	if len(msg) > maxMessageLen {
		// Back off to the nearest UTF-8 rune boundary so we don't split a
		// multibyte rune.
		truncateAt := maxMessageLen
		for truncateAt > 0 && !utf8.RuneStart(msg[truncateAt]) {
			truncateAt--
		}
		msg = msg[:truncateAt]
	}
	e.Events = append(e.Events, &schedulerpb.Event{
		Time:    timestamppb.New(ctx.Now(e)),
		Message: msg,
	})
	if keepFrom := len(e.Events) - maxEntries; keepFrom > 0 {
		// Clone so the dropped entries don't stay reachable via the backing array.
		e.Events = slices.Clone(e.Events[keepFrom:])
	}
}
