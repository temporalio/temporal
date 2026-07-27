// Package model is an implementation-independent vocabulary for specifyin a sequence of events (a
// 'trace') in the lifetime of an activity. Drivers exist that can realize these events for both
// Standalone Activity and Workflow Activity.
package model

import "fmt"

// EventType enumerates the events a driver can realize.
type EventType int

const (
	// RPC events
	PollType EventType = iota
	RespondFailedType
	PauseType

	// Timer events

	// Timeout task timers elapsing (a timer may or may not have actually fired)
	ScheduleToStartElapsesType
	ScheduleToCloseElapsesType
	StartToCloseElapsesType
	HeartbeatElapsesType

	// Dispatch-delay timers elapsing. On elapse the delayed dispatch becomes available.
	StartDelayElapsesType
	BackoffElapsesType
)

// Event carries the variant flags that affect the outcome.
type Event struct {
	Type EventType

	Retryable bool // RespondFailed: the failure is retryable. Whether it actually retries also depends on the retry policy.
}

// Canonical Event values for the variants frequently used in traces
var (
	Poll                   = Event{Type: PollType}
	FailRetryably          = Event{Type: RespondFailedType, Retryable: true}
	Pause                  = Event{Type: PauseType}
	StartToCloseElapses    = Event{Type: StartToCloseElapsesType}
	ScheduleToCloseElapses = Event{Type: ScheduleToCloseElapsesType}
	ScheduleToStartElapses = Event{Type: ScheduleToStartElapsesType}
	HeartbeatElapses       = Event{Type: HeartbeatElapsesType}
	StartDelayElapses      = Event{Type: StartDelayElapsesType}
	BackoffElapses         = Event{Type: BackoffElapsesType}
)

// String is a stable label for an event type, for failure reports.
func (t EventType) String() string {
	switch t {
	case PollType:
		return "Poll"
	case RespondFailedType:
		return "RespondFailed"
	case PauseType:
		return "Pause"
	case ScheduleToStartElapsesType:
		return "ScheduleToStartElapses"
	case ScheduleToCloseElapsesType:
		return "ScheduleToCloseElapses"
	case StartToCloseElapsesType:
		return "StartToCloseElapses"
	case HeartbeatElapsesType:
		return "HeartbeatElapses"
	case StartDelayElapsesType:
		return "StartDelayElapses"
	case BackoffElapsesType:
		return "BackoffElapses"
	default:
		return fmt.Sprintf("EventType(%d)", int(t))
	}
}

// String names an event and appends the flags that affect its outcome.
func (e Event) String() string {
	if e.Type == RespondFailedType {
		if e.Retryable {
			return "RespondFailed[retryable=true]"
		}
		return "RespondFailed[retryable=false]"
	}
	return e.Type.String()
}
