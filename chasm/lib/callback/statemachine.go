package callback

import (
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
	func(ctx chasm.MutableContext, cb *Callback, event EventScheduled) error {
		ctx.AddTask(cb, chasm.TaskAttributes{}, &callbackspb.InvocationTask{})
		return nil
	},
)

// EventRescheduled is triggered when the callback is meant to be rescheduled after backing off from a previous attempt.
type EventRescheduled struct{}

var TransitionRescheduled = chasm.NewTransition(
	[]callbackspb.CallbackStatus{callbackspb.CALLBACK_STATUS_BACKING_OFF},
	callbackspb.CALLBACK_STATUS_SCHEDULED,
	func(mctx chasm.MutableContext, cb *Callback, event EventRescheduled) error {
		cb.NextAttemptScheduleTime = nil
		mctx.AddTask(cb, chasm.TaskAttributes{ScheduledTime: time.Time{}}, &callbackspb.InvocationTask{})
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
	func(mctx chasm.MutableContext, cb *Callback, event EventAttemptFailed) error {
		cb.recordAttempt(event.Time)
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
		mctx.AddTask(cb, chasm.TaskAttributes{ScheduledTime: time.Time{}}, &callbackspb.InvocationTask{})
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
	func(mctx chasm.MutableContext, cb *Callback, event EventFailed) error {
		cb.recordAttempt(event.Time)
		cb.LastAttemptFailure = &failurepb.Failure{
			Message: event.Err.Error(),
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable: true,
				},
			},
		}
		mctx.AddTask(cb, chasm.TaskAttributes{ScheduledTime: time.Time{}}, &callbackspb.InvocationTask{})
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
	func(mctx chasm.MutableContext, cb *Callback, event EventSucceeded) error {
		cb.recordAttempt(event.Time)
		cb.LastAttemptFailure = nil
		mctx.AddTask(cb, chasm.TaskAttributes{}, &callbackspb.InvocationTask{})
		return nil
	},
)
