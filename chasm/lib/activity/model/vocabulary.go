// Package model is an implementation-independent vocabulary for specifying a sequence of events (a
// 'trace') in the lifetime of an activity. Drivers exist that can realize these events for both
// Standalone Activity and Workflow Activity.
package model

import "fmt"

// EventType enumerates the events a driver can realize.
type EventType int

const (
	// RPC events
	PollType EventType = iota
	HeartbeatType
	RespondCompletedType
	RespondCompletedByIDType
	RespondFailedType
	RespondFailedByIDType
	RespondCanceledType
	RequestCancelType
	TerminateType
	PauseType
	UnpauseType
	ResetType
	UpdateOptionsType

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

	KeepPaused          bool     // Reset: a paused activity stays paused across the reset.
	ResetHeartbeat      bool     // Reset: discard the persisted heartbeat checkpoint instead of carrying it into the new attempt.
	HasHeartbeatDetails bool     // Failure response: attach last_heartbeat_details, to be stored as the activity's heartbeat progress.
	Failure             *Failure // RespondFailed: the failure to send, or nil to respond with no failure at all (as a worker may). A nil failure is retryable.
}

// Failure specifies the failure a RespondFailed event sends.
type Failure struct {
	Type         FailureType
	Retryable    bool // controls the non-retryable flag for application and server failures.
	LargeMessage bool // sends a failure message exceeding activityFailureSizeLimit
}

// FailureType identifies the kind of failure a RespondFailed event reports.
type FailureType int

const (
	ApplicationFailureType FailureType = iota
	ServerFailureType
	StartToCloseTimeoutFailureType
	HeartbeatTimeoutFailureType
	ScheduleToStartTimeoutFailureType
	ScheduleToCloseTimeoutFailureType
	UnknownFailureType
)

// Canonical Event values for the variants frequently used in traces
var (
	Poll               = Event{Type: PollType}
	Heartbeat          = Event{Type: HeartbeatType}
	Complete           = Event{Type: RespondCompletedType}
	CompleteByID       = Event{Type: RespondCompletedByIDType}
	FailRetryably      = Event{Type: RespondFailedType, Failure: &Failure{Retryable: true}}
	FailNonRetryably   = Event{Type: RespondFailedType, Failure: &Failure{Retryable: false}}
	FailWithoutFailure = Event{Type: RespondFailedType}
	// FailByIDRetryablyWithServerFailure reports a retryable ServerFailure through the by-ID API. Unlike
	// the by-token API, the by-ID API accepts a non-application failure, so only this variant can carry a
	// ServerFailure to the handler.
	FailByIDRetryablyWithServerFailure              = Event{Type: RespondFailedByIDType, Failure: &Failure{Type: ServerFailureType, Retryable: true}}
	FailByIDRetryablyWithStartToCloseTimeoutFailure = Event{Type: RespondFailedByIDType, Failure: &Failure{Type: StartToCloseTimeoutFailureType}}
	FailByIDRetryablyWithHeartbeatTimeoutFailure    = Event{Type: RespondFailedByIDType, Failure: &Failure{Type: HeartbeatTimeoutFailureType}}
	FailByIDWithScheduleToStartTimeoutFailure       = Event{Type: RespondFailedByIDType, Failure: &Failure{Type: ScheduleToStartTimeoutFailureType}}
	FailByIDWithScheduleToCloseTimeoutFailure       = Event{Type: RespondFailedByIDType, Failure: &Failure{Type: ScheduleToCloseTimeoutFailureType}}
	FailByIDRetryablyWithUnknownFailure             = Event{Type: RespondFailedByIDType, Failure: &Failure{Type: UnknownFailureType}}
	RespondCanceled                                 = Event{Type: RespondCanceledType}
	RequestCancel                                   = Event{Type: RequestCancelType}
	Terminate                                       = Event{Type: TerminateType}
	Pause                                           = Event{Type: PauseType}
	ResetKeepPaused                                 = Event{Type: ResetType, KeepPaused: true}
	Unpause                                         = Event{Type: UnpauseType}
	Reset                                           = Event{Type: ResetType}
	ResetClearingHeartbeat                          = Event{Type: ResetType, ResetHeartbeat: true}
	UpdateOptions                                   = Event{Type: UpdateOptionsType}
	StartToCloseElapses                             = Event{Type: StartToCloseElapsesType}
	ScheduleToCloseElapses                          = Event{Type: ScheduleToCloseElapsesType}
	ScheduleToStartElapses                          = Event{Type: ScheduleToStartElapsesType}
	HeartbeatElapses                                = Event{Type: HeartbeatElapsesType}
	StartDelayElapses                               = Event{Type: StartDelayElapsesType}
	BackoffElapses                                  = Event{Type: BackoffElapsesType}
)

// String is a label for an event type.
func (t EventType) String() string {
	switch t {
	case PollType:
		return "Poll"
	case HeartbeatType:
		return "Heartbeat"
	case RespondCompletedType:
		return "RespondCompleted"
	case RespondCompletedByIDType:
		return "RespondCompletedByID"
	case RespondFailedType:
		return "RespondFailed"
	case RespondFailedByIDType:
		return "RespondFailedByID"
	case RespondCanceledType:
		return "RespondCanceled"
	case RequestCancelType:
		return "RequestCancel"
	case TerminateType:
		return "Terminate"
	case PauseType:
		return "Pause"
	case UnpauseType:
		return "Unpause"
	case ResetType:
		return "Reset"
	case UpdateOptionsType:
		return "UpdateOptions"
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

// String is a label for an event; it includes flags that affect its outcome.
func (e Event) String() string {
	switch e.Type {
	case RespondFailedType, RespondFailedByIDType:
		if e.Failure == nil {
			return fmt.Sprintf("%s[failureOmitted,heartbeatDetails=%v]", e.Type.String(), e.HasHeartbeatDetails)
		}
		if e.Failure.Type == ApplicationFailureType || e.Failure.Type == ServerFailureType {
			return fmt.Sprintf("%s[retryable=%v,heartbeatDetails=%v,failureType=%d]", e.Type.String(), e.Failure.Retryable, e.HasHeartbeatDetails, e.Failure.Type)
		}
		return fmt.Sprintf("%s[heartbeatDetails=%v,failureType=%d]", e.Type.String(), e.HasHeartbeatDetails, e.Failure.Type)
	case ResetType:
		return fmt.Sprintf("%s[keepPaused=%v,resetHeartbeat=%v]", e.Type.String(), e.KeepPaused, e.ResetHeartbeat)
	default:
		return e.Type.String()
	}
}
