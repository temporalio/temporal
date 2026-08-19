package activity

import (
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
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
