package scheduler

import (
	"slices"
	"unicode/utf8"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// Maximum number of EventLog entries retained.
	maxEventLogEntries = 30

	// Maximum length of an EventLog message; longer messages are truncated.
	maxEventLogMessageLen = 1000
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

// LogEvent appends an event with the given message. Messages longer than
// maxEventLogMessageLen are truncated; once the log has more than
// maxEventLogEntries entries, the earliest entries are dropped.
func (e *EventLog) LogEvent(ctx chasm.MutableContext, msg string) {
	if len(msg) > maxEventLogMessageLen {
		// Back off to the nearest UTF-8 rune boundary so we don't split a
		// multibyte rune.
		truncateAt := maxEventLogMessageLen
		for truncateAt > 0 && !utf8.RuneStart(msg[truncateAt]) {
			truncateAt--
		}
		msg = msg[:truncateAt]
	}
	e.Events = append(e.Events, &schedulerpb.Event{
		Time:    timestamppb.New(ctx.Now(e)),
		Message: msg,
	})
	if keepFrom := len(e.Events) - maxEventLogEntries; keepFrom > 0 {
		// Clone so the dropped entries don't stay reachable via the backing array.
		e.Events = slices.Clone(e.Events[keepFrom:])
	}
}
