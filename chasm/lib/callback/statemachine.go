package callback

import (
	"fmt"
	"net/url"
	"time"

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
		u, err := url.Parse(cb.GetCallback().GetNexus().GetUrl())
		if err != nil {
			return fmt.Errorf("failed to parse URL: %v: %w", cb.GetCallback(), err)
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
		cb.ClearNextAttemptScheduleTime()
		u, err := url.Parse(cb.GetCallback().GetNexus().GetUrl())
		if err != nil {
			return fmt.Errorf("failed to parse URL: %v: %w", cb.GetCallback(), err)
		}
		ctx.AddTask(
			cb,
			chasm.TaskAttributes{Destination: u.Scheme + "://" + u.Host},
			callbackspb.InvocationTask_builder{Attempt: cb.GetAttempt()}.Build(),
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
		cb.recordAttempt(event.Time)
		// Use 0 for elapsed time as we don't limit the retry by time (for now).
		nextDelay := event.RetryPolicy.ComputeNextDelay(0, int(cb.GetAttempt()), event.Err)
		nextAttemptScheduleTime := event.Time.Add(nextDelay)
		cb.SetNextAttemptScheduleTime(timestamppb.New(nextAttemptScheduleTime))
		cb.SetLastAttemptFailure(failurepb.Failure_builder{
			Message: event.Err.Error(),
			ApplicationFailureInfo: failurepb.ApplicationFailureInfo_builder{
				NonRetryable: false,
			}.Build(),
		}.Build())
		ctx.AddTask(
			cb,
			chasm.TaskAttributes{ScheduledTime: nextAttemptScheduleTime},
			callbackspb.BackoffTask_builder{Attempt: cb.GetAttempt()}.Build(),
		)
		return nil
	},
)

// EventFailed is triggered when an attempt is failed with a non retryable error.
type EventFailed struct {
	Time time.Time
	Err  error
}

var TransitionFailed = chasm.NewTransition(
	[]callbackspb.CallbackStatus{callbackspb.CALLBACK_STATUS_SCHEDULED},
	callbackspb.CALLBACK_STATUS_FAILED,
	func(cb *Callback, ctx chasm.MutableContext, event EventFailed) error {
		cb.recordAttempt(event.Time)
		cb.SetLastAttemptFailure(failurepb.Failure_builder{
			Message: event.Err.Error(),
			ApplicationFailureInfo: failurepb.ApplicationFailureInfo_builder{
				NonRetryable: true,
			}.Build(),
		}.Build())
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
		cb.recordAttempt(event.Time)
		cb.ClearLastAttemptFailure()
		return nil
	},
)
