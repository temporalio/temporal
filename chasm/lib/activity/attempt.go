// A note on times and terminology:
//
// We name 3 times in the lifecycle of an activity attempt:
//
// schedule_time - the time at which the activity entered SCHEDULED state
// dispatch_time - the time at which the activity task is due to be dispatched to Matching (AddActivityTask)
// start_time    - the time at which the activity enters STARTED state (Matching task picked up by poller)
//
// They are always ordered as: (schedule_time) <= (dispatch_time) < (start_time).
//
// A ScheduleToStart timeout applies to the time between dispatch and start. If there is a delay
// before dispatch (i.e. a start delay on the first attempt, or a backoff interval / next retry
// delay on a second or subsequent attempt) then schedule_time < dispatch_time. Otherwise, they are
// equal.
//
// The main Activity struct has a.ScheduleTime which is the schedule time of the first
// attempt; i.e. the time at which the activity was created. This is never changed.
//
// The naming situation is not perfectly clean. See e.g. the comment below on
// nextAttemptDispatchTime (which is called next_attempt_schedule_time in the public API).

package activity

import (
	"fmt"
	"slices"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	commonfailure "go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/util"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (a *Activity) createAddActivityTaskRequest(ctx chasm.Context, namespaceID string) (*matchingservice.AddActivityTaskRequest, error) {
	// Get latest component ref and unmarshal into proto ref
	componentRef, err := ctx.Ref(a)
	if err != nil {
		return nil, err
	}

	// Note: No need to set the vector clock here, as the components track version conflicts for read/write
	// TODO: Need to fill in VersionDirective once we decide how to handle versioning for standalone activities
	return &matchingservice.AddActivityTaskRequest{
		NamespaceId:            namespaceID,
		ScheduleToStartTimeout: a.ScheduleToStartTimeout,
		TaskQueue:              a.GetTaskQueue(),
		Priority:               a.GetPriority(),
		ComponentRef:           componentRef,
		Stamp:                  a.LastAttempt.Get(ctx).GetStamp(),
	}, nil
}

// dispatchTimeForAttempt returns the dispatch time of the given attempt.
func (a *Activity) dispatchTimeForAttempt(attempt *activitypb.ActivityAttemptState) *timestamppb.Timestamp {
	if dispatchTime := attempt.GetDispatchTime(); dispatchTime != nil {
		return dispatchTime
	}
	if attempt.GetCount() == 1 {
		return timestamppb.New(a.firstDispatchTime())
	}
	return dispatchTimeForRetry(attempt)
}

// dispatchTimeForRetry computes the time a retried attempt will be dispatched to Matching,
// as complete_time + retry_interval. Returns nil if either field is missing or zero.
func dispatchTimeForRetry(attempt *activitypb.ActivityAttemptState) *timestamppb.Timestamp {
	retryInterval := attempt.GetCurrentRetryInterval()
	completeTime := attempt.GetCompleteTime()
	if retryInterval != nil && retryInterval.AsDuration() > 0 && completeTime != nil {
		return timestamppb.New(completeTime.AsTime().Add(retryInterval.AsDuration()))
	}
	return nil
}

// nextAttemptDispatchTime is the dispatch_time of the attempt that is currently being waited for.
// It is null when the dispatch time has passed, in terminal states, and when paused or when an
// attempt is in progress, since in those states the dispatch time of a future attempt is unknown:
// we do not even know if there will be a next attempt.
//
// In the public Describe API response of SAA and WFA, this has the name next_attempt_schedule_time.
// In that field name, the term "schedule_time" is actually a dispatch time; specifically, the
// dispatch time defined by this method.
//
// For WFA, next_attempt_schedule_time is null prior to the first attempt since start delay is not
// supported, hence the activity is due to be dispatched to Matching as soon as the activity is
// created. But for SAA, if there's a start delay, then next_attempt_schedule_time is the
// dispatch_time (non-null).
func (a *Activity) nextAttemptDispatchTime(ctx chasm.Context, attempt *activitypb.ActivityAttemptState) *timestamppb.Timestamp {
	if a.hasAttemptInProgress() || a.isPaused() || a.isTerminal() {
		return nil
	}
	if t := a.dispatchTimeForAttempt(attempt); t != nil {
		if t.AsTime().After(ctx.Now(a)) {
			return t
		}
	}
	return nil
}

// currentRetryInterval is the retry interval if the activity is currently waiting for a retry; nil otherwise.
func (a *Activity) currentRetryInterval(ctx chasm.Context, attempt *activitypb.ActivityAttemptState) *durationpb.Duration {
	if a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED {
		if t := a.dispatchTimeForAttempt(attempt); t != nil {
			if t.AsTime().After(ctx.Now(a)) {
				return attempt.GetCurrentRetryInterval()
			}
		}
	}
	return nil
}

// recordFailedAttempt records any failures resulting from a tried attempt, including worker application failures and
// start-to-close timeouts. Since the calls come from retried attempts we update the attempt failure info but leave
// the outcome failure empty to avoid duplication.
func (a *Activity) recordFailedAttempt(
	ctx chasm.MutableContext,
	retryInterval time.Duration,
	retryIntervalSource activitypb.ActivityRetryIntervalSource,
	failure *failurepb.Failure,
	currentTime time.Time,
	noRetriesLeft bool,
) error {
	attempt := a.LastAttempt.Get(ctx)

	attemptFailure := failure
	if !noRetriesLeft {
		// Similar to workflow activity, truncate only retryable failure, not the final one.
		attemptFailure = truncateRetryableFailure(ctx, failure)
	}

	attempt.LastFailureDetails = &activitypb.ActivityAttemptState_LastFailureDetails{
		Failure: attemptFailure,
		Time:    timestamppb.New(currentTime),
	}
	attempt.CompleteTime = timestamppb.New(currentTime)

	if noRetriesLeft {
		attempt.CurrentRetryInterval = nil
		attempt.CurrentRetryIntervalSource = activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_UNSPECIFIED
	} else {
		attempt.CurrentRetryInterval = durationpb.New(retryInterval)
		attempt.CurrentRetryIntervalSource = retryIntervalSource
	}
	return nil
}

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

// truncateRetryableFailure caps the size of a failure retained in the activity's state while it
// retries, mirroring MutableStateImpl.truncateRetryableActivityFailure for workflow activities.
func truncateRetryableFailure(ctx chasm.Context, attemptFailure *failurepb.Failure) *failurepb.Failure {
	actCtx := activityContextFromChasm(ctx)
	sizeLimit := actCtx.config.MutableStateActivityFailureSizeLimitError(ctx.NamespaceEntry().Name().String())
	if attemptFailure.Size() <= sizeLimit {
		return attemptFailure
	}

	// nonRetryable is set to false here as only failures of attempts that will be retried are
	// truncated, so the value is only for visibility/debugging purposes.
	serverFailure := commonfailure.NewServerFailure(common.FailureReasonFailureExceedsLimit, false)
	serverFailure.Cause = commonfailure.Truncate(attemptFailure, sizeLimit)
	return serverFailure
}

// tryReschedule attempts to reschedule the activity for retry. It handles the cases of pause and
// reset requests that were received while the last attempt was in progress. failureRetryable
// reports whether the failure itself permits a retry (timeouts always do; RespondActivityTaskFailed
// depends on the failure).
func (a *Activity) tryReschedule(
	ctx chasm.MutableContext,
	failureRetryable bool,
	overridingRetryInterval time.Duration,
	failure *failurepb.Failure,
) (enumspb.RetryState, error) {
	status := a.GetStatus()
	if status == activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED {
		event := rescheduleEvent{failure: failure}
		if a.ResetShouldPause {
			return enumspb.RETRY_STATE_IN_PROGRESS, TransitionResetAttemptFailedToPaused.Apply(a, ctx, event)
		}
		return enumspb.RETRY_STATE_IN_PROGRESS, TransitionResetAttemptFailedToScheduled.Apply(a, ctx, event)
	}
	if a.GetRetryPolicy() == nil {
		return enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET, nil
	}
	if status == activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED {
		return enumspb.RETRY_STATE_CANCEL_REQUESTED, nil
	}
	retryState, retryInterval := a.shouldRetry(ctx, overridingRetryInterval)
	if !failureRetryable {
		retryState = enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE
	}
	if retryState != enumspb.RETRY_STATE_IN_PROGRESS {
		return retryState, nil
	}
	retryIntervalSource := activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY
	if overridingRetryInterval > 0 {
		retryIntervalSource = activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_WORKER_OVERRIDE
	}
	event := rescheduleEvent{retryInterval: retryInterval, retryIntervalSource: retryIntervalSource, failure: failure}
	switch status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED:
		return enumspb.RETRY_STATE_IN_PROGRESS, TransitionAttemptFailedWhilePauseRequested.Apply(a, ctx, event)
	default:
		return enumspb.RETRY_STATE_IN_PROGRESS, TransitionRescheduled.Apply(a, ctx, event)
	}
}

func (a *Activity) shouldRetry(ctx chasm.Context, overridingRetryInterval time.Duration) (enumspb.RetryState, time.Duration) {
	if !TransitionRescheduled.Possible(a) &&
		!TransitionAttemptFailedWhilePauseRequested.Possible(a) {
		return enumspb.RETRY_STATE_UNSPECIFIED, 0
	}
	attempt := a.LastAttempt.Get(ctx)
	retryPolicy := a.RetryPolicy
	enoughTime, retryInterval := a.hasEnoughTimeForRetry(ctx, overridingRetryInterval)

	if retryPolicy.GetMaximumAttempts() > 0 && attempt.GetCount() >= retryPolicy.GetMaximumAttempts() {
		return enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED, retryInterval
	}
	if !enoughTime {
		return enumspb.RETRY_STATE_TIMEOUT, retryInterval
	}
	return enumspb.RETRY_STATE_IN_PROGRESS, retryInterval
}

// timeoutRetryable reports whether a StartToClose or Heartbeat timeout may be retried under the retry
// policy.
func (a *Activity) timeoutRetryable(timeoutType enumspb.TimeoutType) bool {
	return !slices.Contains(
		a.GetRetryPolicy().GetNonRetryableErrorTypes(),
		retrypolicy.TimeoutFailureTypePrefix+timeoutType.String(),
	)
}

// hasEnoughTimeForRetry checks if there is enough time left in the schedule-to-close timeout. If sufficient time
// remains, it will also return a valid retry interval.
func (a *Activity) hasEnoughTimeForRetry(ctx chasm.Context, overridingRetryInterval time.Duration) (bool, time.Duration) {
	attempt := a.LastAttempt.Get(ctx)

	// Use overriding retry interval if provided, else calculate based on retry policy
	retryInterval := overridingRetryInterval
	if retryInterval <= 0 {
		retryInterval = backoff.CalculateExponentialRetryInterval(a.RetryPolicy, attempt.Count)
	}

	scheduleToClose := a.GetScheduleToCloseTimeout().AsDuration()
	if scheduleToClose == 0 {
		return true, retryInterval
	}

	deadline := a.scheduleToCloseDeadline()
	return ctx.Now(a).Add(retryInterval).Before(deadline), retryInterval
}

func (a *Activity) firstDispatchTime() time.Time {
	return a.ScheduleTime.AsTime().Add(a.GetStartDelay().AsDuration())
}

func (a *Activity) newActivityDispatchTask(ctx chasm.Context) *activitypb.ActivityDispatchTask {
	dispatchReason := activitypb.DISPATCH_REASON_IMMEDIATE
	if a.GetFirstAttemptStartedTime() != nil {
		dispatchReason = activitypb.DISPATCH_REASON_RETRY
	} else if a.GetStartDelay().AsDuration() > 0 {
		dispatchReason = activitypb.DISPATCH_REASON_START_DELAY
	}

	return &activitypb.ActivityDispatchTask{
		Stamp:            a.LastAttempt.Get(ctx).GetStamp(),
		DispatchReason:   dispatchReason,
		StartDelayBucket: startDelayBucket(a.GetStartDelay().AsDuration()),
	}
}

func startDelayBucket(delay time.Duration) activitypb.StartDelayBucket {
	switch {
	case delay <= 0:
		return activitypb.START_DELAY_BUCKET_NONE
	case delay < time.Minute:
		return activitypb.START_DELAY_BUCKET_LT_1M
	case delay < 10*time.Minute:
		return activitypb.START_DELAY_BUCKET_1M_10M
	case delay < time.Hour:
		return activitypb.START_DELAY_BUCKET_10M_1H
	case delay < 6*time.Hour:
		return activitypb.START_DELAY_BUCKET_1H_6H
	case delay < 24*time.Hour:
		return activitypb.START_DELAY_BUCKET_6H_1D
	case delay < 7*24*time.Hour:
		return activitypb.START_DELAY_BUCKET_1D_7D
	case delay <= 30*24*time.Hour:
		return activitypb.START_DELAY_BUCKET_7D_30D
	default:
		return activitypb.START_DELAY_BUCKET_GT_30D
	}
}

// reissueDispatchAndScheduleToStart re-emits the ActivityDispatchTask and ScheduleToStart timeout task for
// a SCHEDULED activity. Retries fire at the retry time; first attempts dispatch now, lifted to
// honor any pending start_delay.
func (a *Activity) reissueDispatchAndScheduleToStart(ctx chasm.MutableContext, attempt *activitypb.ActivityAttemptState) {
	var dispatchTime time.Time
	if retryDispatchTime := dispatchTimeForRetry(attempt); retryDispatchTime != nil {
		dispatchTime = retryDispatchTime.AsTime()
	} else {
		dispatchTime = a.dispatchTimeRespectingStartDelay(ctx.Now(a))
	}
	attempt.DispatchTime = timestamppb.New(dispatchTime)
	ctx.AddTask(
		a,
		chasm.TaskAttributes{ScheduledTime: dispatchTime},
		a.newActivityDispatchTask(ctx),
	)
	if timeout := a.GetScheduleToStartTimeout().AsDuration(); timeout > 0 {
		ctx.AddTask(
			a,
			chasm.TaskAttributes{ScheduledTime: dispatchTime.Add(timeout)},
			&activitypb.ScheduleToStartTimeoutTask{Stamp: attempt.GetStamp()},
		)
	}
}

// reissueRunningAttemptTimers re-emits the StartToClose and Heartbeat timeout tasks for the
// currently-running attempt, anchored to the attempt's StartedTime. Called from options-update
// paths after stamp bump so the old tasks are invalidated and replaced with the (possibly
// updated) timeouts. No-op unless the activity is in a status where a worker holds the task token
// (STARTED / CANCEL_REQUESTED / PAUSE_REQUESTED / RESET_REQUESTED).
func (a *Activity) reissueRunningAttemptTimers(ctx chasm.MutableContext, attempt *activitypb.ActivityAttemptState) {
	if !a.hasAttemptInProgress() {
		return
	}
	if timeout := a.GetStartToCloseTimeout().AsDuration(); timeout > 0 {
		deadline := attempt.GetStartedTime().AsTime().Add(timeout)
		ctx.AddTask(
			a,
			chasm.TaskAttributes{ScheduledTime: deadline},
			&activitypb.StartToCloseTimeoutTask{Stamp: attempt.GetStamp()},
		)
	}
	if hbTimeout := a.GetHeartbeatTimeout().AsDuration(); hbTimeout > 0 {
		// Next heartbeat fires at max(last recorded heartbeat, current attempt start) + heartbeat timeout.
		lastHb, _ := a.LastHeartbeat.TryGet(ctx)
		lastHbTime := util.MaxTime(
			lastHb.GetRecordedTime().AsTime(),
			attempt.GetStartedTime().AsTime(),
		).Add(hbTimeout)
		ctx.AddTask(
			a,
			chasm.TaskAttributes{ScheduledTime: lastHbTime},
			&activitypb.HeartbeatTimeoutTask{Stamp: attempt.GetStamp()},
		)
	}
}

// dispatchTimeRespectingStartDelay advances a candidate dispatch time t to the first dispatch time
// (ScheduleTime + start_delay) while the activity has not yet been picked up by a worker, so that
// pre-dispatch re-scheduling (unpause, reset, options update) honors any remaining start_delay.
// Returns t unchanged if the first attempt has already started.
func (a *Activity) dispatchTimeRespectingStartDelay(t time.Time) time.Time {
	if a.GetFirstAttemptStartedTime() != nil {
		return t
	}
	if dispatchTime := a.firstDispatchTime(); dispatchTime.After(t) {
		return dispatchTime
	}
	return t
}

// reissueScheduleToClose bumps the ScheduleToCloseStamp and re-emits the ScheduleToClose timeout task
// at the current deadline.
func (a *Activity) reissueScheduleToClose(ctx chasm.MutableContext) {
	if deadline := a.scheduleToCloseDeadline(); !deadline.IsZero() {
		a.ScheduleToCloseStamp++
		ctx.AddTask(
			a,
			chasm.TaskAttributes{ScheduledTime: deadline},
			&activitypb.ScheduleToCloseTimeoutTask{Stamp: a.GetScheduleToCloseStamp()},
		)
	}
}

// scheduleToCloseDeadline returns the absolute time at which the ScheduleToClose timeout expires,
// accounting for start delay. Returns zero time if no ScheduleToClose timeout is set.
func (a *Activity) scheduleToCloseDeadline() time.Time {
	timeout := a.GetScheduleToCloseTimeout().AsDuration()
	if timeout == 0 {
		return time.Time{}
	}
	return a.firstDispatchTime().Add(timeout)
}

func createStartToCloseTimeoutFailure() *failurepb.Failure {
	return &failurepb.Failure{
		Message: fmt.Sprintf(common.FailureReasonActivityTimeout, enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String()),
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
			},
		},
	}
}

func createHeartbeatTimeoutFailure() *failurepb.Failure {
	return &failurepb.Failure{
		Message: fmt.Sprintf(common.FailureReasonActivityTimeout, enumspb.TIMEOUT_TYPE_HEARTBEAT.String()),
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: enumspb.TIMEOUT_TYPE_HEARTBEAT,
			},
		},
	}
}
