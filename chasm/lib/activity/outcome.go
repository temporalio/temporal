package activity

import (
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Recording of the terminal outcome of an activity execution.

// recordScheduleToStartOrCloseTimeoutFailure records schedule-to-start or schedule-to-close timeout outcomes. Such
// timeouts are not retried, so we set the outcome failure directly and leave the attempt failure as is.
func (a *Activity) recordScheduleToStartOrCloseTimeoutFailure(
	ctx chasm.MutableContext,
	timeoutType enumspb.TimeoutType,
	message string,
	cause *failurepb.Failure,
) error {
	failure := &failurepb.Failure{
		Message: message,
		Cause:   cause,
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType:          timeoutType,
				LastHeartbeatDetails: a.lastHeartbeatDetails(ctx),
			},
		},
	}

	a.Outcome.Get(ctx).Variant = &activitypb.ActivityOutcome_Failed_{
		Failed: &activitypb.ActivityOutcome_Failed{
			Failure: failure,
		},
	}

	return nil
}

// Transition bodies. Each is invoked from a chasm.Transition in statemachine.go, which
// has already validated that the activity is in a legal source state.

// applyCompleted is the body of TransitionCompleted: it records the successful outcome.
func (a *Activity) applyCompleted(ctx chasm.MutableContext, event completeEvent) error {
	return a.StoreOrSelf(ctx).RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
		req := event.req.GetCompleteRequest()

		attemptWasStarted := a.hasAttemptInProgress()
		attempt := a.LastAttempt.Get(ctx)
		if !attemptWasStarted {
			// RespondActivityTaskCompletedById can complete an activity when no attempt is in
			// progress.
			attempt.StartedTime = timestamppb.New(ctx.Now(a))
			if a.FirstAttemptStartedTime == nil {
				a.FirstAttemptStartedTime = attempt.StartedTime
			}
		}
		attempt.CompleteTime = timestamppb.New(ctx.Now(a))
		attempt.LastWorkerIdentity = req.GetIdentity()
		outcome := a.Outcome.Get(ctx)
		outcome.Variant = &activitypb.ActivityOutcome_Successful_{
			Successful: &activitypb.ActivityOutcome_Successful{
				Output: req.GetResult(),
			},
		}

		a.emitOnCompletedMetrics(ctx, event.baseHandler, event.enrichedHandler, req.GetResult(), attemptWasStarted)

		return nil
	})
}

// applyFailed is the body of TransitionFailed: it records a terminal failure outcome.
// Not to be confused with applyFailedAttempt/recordFailedAttempt, which record the
// failure of a single attempt that may still be retried.
func (a *Activity) applyFailed(ctx chasm.MutableContext, event failedEvent) error {
	return a.StoreOrSelf(ctx).RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
		req := event.req.GetFailedRequest()
		a.Outcome.Get(ctx).RetryState = event.retryState

		attempt := a.LastAttempt.Get(ctx)
		attempt.LastWorkerIdentity = req.GetIdentity()

		// A worker may respond failed without a Failure. Synthesize a generic terminal failure so
		// the closed activity still exposes a consumable outcome; otherwise PollActivityExecution
		// returns a nil outcome and a client cannot tell the closed activity apart from one that
		// simply has no result yet, and polls forever. (Workflow activities tolerate a nil failure
		// because the SDK wraps the failed history event in an ActivityError; a standalone activity
		// returns the raw outcome with no such wrapper.)
		failure := req.GetFailure()
		if failure == nil {
			failure = &failurepb.Failure{Message: "activity task failed without failure details"}
		}

		if err := a.recordFailedAttempt(ctx, 0, activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_UNSPECIFIED, failure, ctx.Now(a), true); err != nil {
			return err
		}

		a.emitOnFailedMetrics(ctx, event.baseHandler, event.enrichedHandler, req.GetFailure())

		return nil
	})
}

// applyTerminated is the body of TransitionTerminated: it records the terminated outcome.
func (a *Activity) applyTerminated(ctx chasm.MutableContext, event terminateEvent) error {
	return a.StoreOrSelf(ctx).RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
		a.TerminateState = &activitypb.ActivityTerminateState{
			RequestId: event.request.RequestID,
		}
		outcome := a.Outcome.Get(ctx)
		failure := &failurepb.Failure{
			Message: event.request.Reason,
			FailureInfo: &failurepb.Failure_TerminatedFailureInfo{
				TerminatedFailureInfo: &failurepb.TerminatedFailureInfo{
					Identity: event.request.Identity,
				},
			},
		}
		outcome.Variant = &activitypb.ActivityOutcome_Failed_{
			Failed: &activitypb.ActivityOutcome_Failed{
				Failure: failure,
			},
		}

		a.emitOnTerminatedMetrics(event.metricsHandler)

		return nil
	})
}

// applyCanceled is the body of TransitionCanceled: it records the canceled outcome.
func (a *Activity) applyCanceled(ctx chasm.MutableContext, event cancelEvent) error {
	return a.StoreOrSelf(ctx).RecordCompleted(ctx, func(ctx chasm.MutableContext) error {
		outcome := a.Outcome.Get(ctx)
		failure := &failurepb.Failure{
			Message: "Activity canceled",
			FailureInfo: &failurepb.Failure_CanceledFailureInfo{
				CanceledFailureInfo: &failurepb.CanceledFailureInfo{
					Details:  event.details,
					Identity: a.GetCancelState().GetIdentity(),
				},
			},
		}
		outcome.Variant = &activitypb.ActivityOutcome_Failed_{
			Failed: &activitypb.ActivityOutcome_Failed{
				Failure: failure,
			},
		}

		a.emitOnCanceledMetrics(ctx, event.metricsHandler, event.fromStatus)

		return nil
	})
}
