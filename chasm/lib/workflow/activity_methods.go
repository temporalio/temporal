package workflow

import (
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
)

var _ activity.ActivityStore = (*Workflow)(nil)

// ActivityScheduledEventID returns the ID of the ActivityTaskScheduled event of an activity of this
// workflow.
func (w *Workflow) ActivityScheduledEventID(ctx chasm.Context, a *activity.Activity) (int64, bool) {
	for id, field := range w.Activities {
		if field.Get(ctx) == a {
			return id, true
		}
	}
	return 0, false
}

// RecordCompleted implements activity.ActivityStore. After the activity records its outcome, the
// workflow adds the ActivityTaskStarted event, if the last attempt started, and the outcome event,
// as the server does for an activity in mutable state. The activity keeps only the identity of the
// worker that last responded, so that identity is recorded in both events; the server records the
// polling worker's identity in the started event.
func (w *Workflow) RecordCompleted(
	ctx chasm.MutableContext,
	a *activity.Activity,
	applyFn func(ctx chasm.MutableContext) error,
) error {
	if err := applyFn(ctx); err != nil {
		return err
	}
	scheduledEventID, ok := w.ActivityScheduledEventID(ctx, a)
	if !ok {
		return serviceerror.NewInternal("activity not found in workflow")
	}
	var startedEventID int64
	if attempt := a.LastAttempt.Get(ctx); attempt.GetStartedTime() != nil {
		event, err := addAndApplyHistoryEvent[ActivityTaskStartedEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
				ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
					ScheduledEventId: scheduledEventID,
					Identity:         attempt.GetLastWorkerIdentity(),
					RequestId:        attempt.GetStartRequestId(),
					Attempt:          attempt.GetCount(),
					LastFailure:      attempt.GetLastFailureDetails().GetFailure(),
				},
			}
		})
		if err != nil {
			return err
		}
		startedEventID = event.GetEventId()
	}
	return w.addActivityOutcomeEvent(ctx, a, scheduledEventID, startedEventID)
}

// addActivityOutcomeEvent adds the event recording the outcome of an activity. The activity's
// status changes after RecordCompleted returns, so the outcome is read from its data.
func (w *Workflow) addActivityOutcomeEvent(
	ctx chasm.MutableContext,
	a *activity.Activity,
	scheduledEventID int64,
	startedEventID int64,
) error {
	outcome := a.Outcome.Get(ctx)
	failure := a.TerminalFailure(ctx)
	identity := a.LastAttempt.Get(ctx).GetLastWorkerIdentity()
	var err error
	switch {
	case outcome.GetSuccessful() != nil:
		_, err = addAndApplyHistoryEvent[ActivityTaskCompletedEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
				ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
					Result:           outcome.GetSuccessful().GetOutput(),
					ScheduledEventId: scheduledEventID,
					StartedEventId:   startedEventID,
					Identity:         identity,
				},
			}
		})
	case failure.GetCanceledFailureInfo() != nil:
		_, err = addAndApplyHistoryEvent[ActivityTaskCanceledEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_ActivityTaskCanceledEventAttributes{
				ActivityTaskCanceledEventAttributes: &historypb.ActivityTaskCanceledEventAttributes{
					Details:          failure.GetCanceledFailureInfo().GetDetails(),
					ScheduledEventId: scheduledEventID,
					StartedEventId:   startedEventID,
					Identity:         identity,
				},
			}
		})
	case failure.GetTimeoutFailureInfo() != nil:
		_, err = addAndApplyHistoryEvent[ActivityTaskTimedOutEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_ActivityTaskTimedOutEventAttributes{
				ActivityTaskTimedOutEventAttributes: &historypb.ActivityTaskTimedOutEventAttributes{
					Failure:          failure,
					ScheduledEventId: scheduledEventID,
					StartedEventId:   startedEventID,
					RetryState:       outcome.GetRetryState(),
				},
			}
		})
	default:
		_, err = addAndApplyHistoryEvent[ActivityTaskFailedEventDefinition](w, ctx, func(e *historypb.HistoryEvent) {
			e.Attributes = &historypb.HistoryEvent_ActivityTaskFailedEventAttributes{
				ActivityTaskFailedEventAttributes: &historypb.ActivityTaskFailedEventAttributes{
					Failure:          failure,
					ScheduledEventId: scheduledEventID,
					StartedEventId:   startedEventID,
					Identity:         identity,
					RetryState:       outcome.GetRetryState(),
				},
			}
		})
	}
	return err
}
