package callback

import (
	"fmt"
	"net/url"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// EventScheduled is triggered when the callback is meant to be scheduled for the first time - when its Trigger
// condition is met.
type EventScheduled struct{}

var TransitionScheduled = chasm.NewTransition(
	[]callbackspb.CallbackStatus{callbackspb.CALLBACK_STATUS_STANDBY},
	callbackspb.CALLBACK_STATUS_SCHEDULED,
	func(cb *Callback, ctx chasm.MutableContext, event EventScheduled) error {
		u, err := url.Parse(cb.Callback.GetNexus().GetUrl())
		if err != nil {
			return fmt.Errorf("failed to parse URL: %v: %w", cb.Callback, err)
		}
		ctx.AddTask(cb, chasm.TaskAttributes{Destination: u.Scheme + "://" + u.Host}, &callbackspb.InvocationTask{})
		return nil
	},
)

// EventRescheduled is triggered when the callback is meant to be rescheduled after backing off from a previous attempt.
type EventRescheduled struct{}

var TransitionRescheduled = chasm.NewTransition(
	[]callbackspb.CallbackStatus{callbackspb.CALLBACK_STATUS_BACKING_OFF},
	callbackspb.CALLBACK_STATUS_SCHEDULED,
	func(cb *Callback, ctx chasm.MutableContext, event EventRescheduled) error {
		cb.NextAttemptScheduleTime = nil
		u, err := url.Parse(cb.Callback.GetNexus().GetUrl())
		if err != nil {
			return fmt.Errorf("failed to parse URL: %v: %w", cb.Callback, err)
		}
		ctx.AddTask(
			cb,
			chasm.TaskAttributes{Destination: u.Scheme + "://" + u.Host},
			&callbackspb.InvocationTask{Attempt: cb.Attempt},
		)
		return nil
	},
)

// EventAttemptFailed is triggered when an attempt is failed with a retryable error.
type EventAttemptFailed struct {
	Time        time.Time
	Err         error
	RetryPolicy backoff.RetryPolicy
}

var TransitionAttemptFailed = chasm.NewTransition(
	[]callbackspb.CallbackStatus{callbackspb.CALLBACK_STATUS_SCHEDULED},
	callbackspb.CALLBACK_STATUS_BACKING_OFF,
	func(cb *Callback, ctx chasm.MutableContext, event EventAttemptFailed) error {
		// Record the event.
		now := ctx.Now(cb)
		cb.recordAttempt(now)
		cb.CloseTime = timestamppb.New(now)

		// Use 0 for elapsed time as we don't limit the retry by time (for now).
		nextDelay := event.RetryPolicy.ComputeNextDelay(0, int(cb.Attempt), event.Err)
		nextAttemptScheduleTime := event.Time.Add(nextDelay)
		cb.NextAttemptScheduleTime = timestamppb.New(nextAttemptScheduleTime)
		cb.LastAttemptFailure = &failurepb.Failure{
			Message: event.Err.Error(),
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable: false,
				},
			},
		}
		ctx.AddTask(
			cb,
			chasm.TaskAttributes{ScheduledTime: nextAttemptScheduleTime},
			&callbackspb.BackoffTask{Attempt: cb.Attempt},
		)
		return nil
	},
)

// EventFailed is triggered when an attempt is failed with a non-retryable error.
type EventFailed struct {
	Time time.Time
	Err  error
}

var TransitionFailed = chasm.NewTransition(
	[]callbackspb.CallbackStatus{callbackspb.CALLBACK_STATUS_SCHEDULED},
	callbackspb.CALLBACK_STATUS_FAILED,
	func(cb *Callback, ctx chasm.MutableContext, event EventFailed) error {
		// Record the event.
		now := ctx.Now(cb)
		cb.recordAttempt(now)
		cb.CloseTime = timestamppb.New(now)

		cb.LastAttemptFailure = &failurepb.Failure{
			Message: event.Err.Error(),
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable: true,
				},
			},
		}
		return nil
	},
)

// EventSucceeded is triggered when an attempt succeeds.
type EventSucceeded struct {
	Time time.Time
}

var TransitionSucceeded = chasm.NewTransition(
	[]callbackspb.CallbackStatus{callbackspb.CALLBACK_STATUS_SCHEDULED},
	callbackspb.CALLBACK_STATUS_SUCCEEDED,
	func(cb *Callback, ctx chasm.MutableContext, event EventSucceeded) error {
		now := ctx.Now(cb)
		cb.recordAttempt(now)
		cb.LastAttemptFailure = nil
		return nil
	},
)

// EventTerminated is triggered when the callback is forcefully terminated.
type EventTerminated struct {
	Reason string
}

var TransitionTerminated = chasm.NewTransition(
	[]callbackspb.CallbackStatus{
		callbackspb.CALLBACK_STATUS_STANDBY,
		callbackspb.CALLBACK_STATUS_SCHEDULED,
		callbackspb.CALLBACK_STATUS_BACKING_OFF,
	},
	callbackspb.CALLBACK_STATUS_TERMINATED,
	func(cb *Callback, ctx chasm.MutableContext, event EventTerminated) error {
		now := ctx.Now(cb)
		cb.CloseTime = timestamppb.New(now)

		reason := event.Reason
		if reason == "" {
			reason = "callback execution terminated"
		}

		cb.Failure = &failurepb.Failure{
			Message:     reason,
			FailureInfo: &failurepb.Failure_TerminatedFailureInfo{},
		}
		return nil
	},
)

// EventTimedOut is triggered when the callback's schedule-to-close timeout fires.
type EventTimedOut struct{}

var TransitionTimedOut = chasm.NewTransition(
	[]callbackspb.CallbackStatus{
		callbackspb.CALLBACK_STATUS_STANDBY,
		callbackspb.CALLBACK_STATUS_SCHEDULED,
		callbackspb.CALLBACK_STATUS_BACKING_OFF,
	},
	callbackspb.CALLBACK_STATUS_FAILED,
	func(cb *Callback, ctx chasm.MutableContext, event EventTimedOut) error {
		now := ctx.Now(cb)
		cb.CloseTime = timestamppb.New(now)

		cb.Failure = &failurepb.Failure{
			Message: "callback execution timed out",
			FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
				TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
					TimeoutType: enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
				},
			},
		}
		return nil
	},
)
