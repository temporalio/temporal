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
	"math"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	commonfailure "go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/primitives/timestamp"
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
	// TODO: Populate VersionDirective when standalone activity versioning is supported, including handoff redispatch.
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

	if attempt.GetCount() == 1 && attempt.GetPendingRetryType() != activitypb.ACTIVITY_RETRY_TYPE_HANDOFF {
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
		attempt.PendingRetryType = activitypb.ACTIVITY_RETRY_TYPE_UNSPECIFIED
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

type retryEvent struct {
	// Set only for server-fired timeouts.
	serverTimeoutType       enumspb.TimeoutType
	overridingRetryInterval time.Duration
	failure                 *failurepb.Failure
}

func (a *Activity) retryTypeForFailure() activitypb.ActivityRetryType {
	return retryTypeForFailure(a.GetAsyncHandoffOptions() != nil, a.GetAwaitingCompletion())
}

func (a *Activity) retryPolicyAndCount(ctx chasm.Context, retryType activitypb.ActivityRetryType) (*commonpb.RetryPolicy, int32) {
	attempt := a.LastAttempt.Get(ctx)
	if retryType == activitypb.ACTIVITY_RETRY_TYPE_HANDOFF {
		return a.GetAsyncHandoffOptions().GetRetryPolicy(), attempt.GetAsyncHandoffState().GetCount()
	}

	return a.GetRetryPolicy(), attempt.GetCount()
}

// tryReschedule selects retryability, limits and backoff from the same policy and failed ordinal.
func (a *Activity) tryReschedule(ctx chasm.MutableContext, event retryEvent) (enumspb.RetryState, error) {
	var retryType activitypb.ActivityRetryType
	if event.serverTimeoutType == enumspb.TIMEOUT_TYPE_UNSPECIFIED {
		retryType = a.retryTypeForFailure()
	} else {
		retryType = retryTypeForTimeout(a.GetAsyncHandoffOptions() != nil, a.GetAwaitingCompletion(), event.serverTimeoutType)
	}

	if retryType == activitypb.ACTIVITY_RETRY_TYPE_UNSPECIFIED {
		return enumspb.RETRY_STATE_UNSPECIFIED, serviceerror.NewInternal("invalid activity retry event")
	}

	startToCloseExpiredWhileScheduled := a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED &&
		event.serverTimeoutType == enumspb.TIMEOUT_TYPE_START_TO_CLOSE && a.GetAsyncHandoffOptions() != nil &&
		a.LastAttempt.Get(ctx).GetAsyncHandoffState().GetStartToCloseStartedTime() != nil
	if !a.hasAttemptInProgress() && !startToCloseExpiredWhileScheduled {
		return enumspb.RETRY_STATE_UNSPECIFIED, serviceerror.NewInternal("activity retry has no active attempt")
	}

	policy, count := a.retryPolicyAndCount(ctx, retryType)
	if count <= 0 {
		return enumspb.RETRY_STATE_UNSPECIFIED, serviceerror.NewInternal("activity retry counter is not initialized")
	}

	retryable := retrypolicy.IsRetryableFailure(event.failure, policy.GetNonRetryableErrorTypes())
	if event.serverTimeoutType != enumspb.TIMEOUT_TYPE_UNSPECIFIED {
		retryable = retrypolicy.IsRetryableTimeout(event.serverTimeoutType, policy.GetNonRetryableErrorTypes())
	}

	status := a.GetStatus()
	if status == activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED {
		reset := rescheduleEvent{failure: event.failure, retryType: retryType}
		if a.ResetShouldPause {
			return enumspb.RETRY_STATE_IN_PROGRESS, TransitionResetAttemptFailedToPaused.Apply(a, ctx, reset)
		}

		return enumspb.RETRY_STATE_IN_PROGRESS, TransitionResetAttemptFailedToScheduled.Apply(a, ctx, reset)
	}

	if policy == nil {
		return enumspb.RETRY_STATE_RETRY_POLICY_NOT_SET, nil
	}

	if status == activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED {
		return enumspb.RETRY_STATE_CANCEL_REQUESTED, nil
	}

	if !retryable {
		return enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE, nil
	}

	retryState, retryInterval := a.shouldRetry(ctx, policy, count, event.overridingRetryInterval)
	source := activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY
	if event.overridingRetryInterval > 0 {
		source = activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_WORKER_OVERRIDE
	}

	reschedule := rescheduleEvent{retryInterval: retryInterval, retryIntervalSource: source, failure: event.failure, retryType: retryType}
	if retryState != enumspb.RETRY_STATE_IN_PROGRESS {
		return retryState, nil
	}

	if status == activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED {
		return retryState, TransitionAttemptFailedWhilePauseRequested.Apply(a, ctx, reschedule)
	}

	if startToCloseExpiredWhileScheduled {
		return retryState, TransitionStartToCloseExpiredWhileScheduled.Apply(a, ctx, reschedule)
	}

	return retryState, TransitionRescheduled.Apply(a, ctx, reschedule)
}

func (a *Activity) shouldRetry(ctx chasm.Context, policy *commonpb.RetryPolicy, count int32, overridingRetryInterval time.Duration) (enumspb.RetryState, time.Duration) {
	enoughTime, retryInterval := a.hasEnoughTimeForRetry(ctx, policy, count, overridingRetryInterval)
	if policy.GetMaximumAttempts() > 0 && count >= policy.GetMaximumAttempts() {
		return enumspb.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED, retryInterval
	}

	if !enoughTime {
		return enumspb.RETRY_STATE_TIMEOUT, retryInterval
	}

	return enumspb.RETRY_STATE_IN_PROGRESS, retryInterval
}

func (a *Activity) hasEnoughTimeForRetry(ctx chasm.Context, policy *commonpb.RetryPolicy, count int32, overridingRetryInterval time.Duration) (bool, time.Duration) {
	retryInterval := overridingRetryInterval
	if retryInterval <= 0 {
		retryInterval = backoff.CalculateExponentialRetryInterval(policy, count)
	}

	if a.GetScheduleToCloseTimeout().AsDuration() == 0 {
		return true, retryInterval
	}

	return ctx.Now(a).Add(retryInterval).Before(a.scheduleToCloseDeadline()), retryInterval
}

func (a *Activity) firstDispatchTime() time.Time {
	return a.ScheduleTime.AsTime().Add(a.GetStartDelay().AsDuration())
}

func (a *Activity) newActivityDispatchTask(ctx chasm.Context) *activitypb.ActivityDispatchTask {
	attempt := a.LastAttempt.Get(ctx)
	dispatchReason := activitypb.DISPATCH_REASON_IMMEDIATE
	if attempt.GetCount() > 1 || attempt.GetPendingRetryType() == activitypb.ACTIVITY_RETRY_TYPE_HANDOFF {
		dispatchReason = activitypb.DISPATCH_REASON_RETRY
	} else if a.GetFirstAttemptStartedTime() == nil && a.GetStartDelay().AsDuration() > 0 {
		dispatchReason = activitypb.DISPATCH_REASON_START_DELAY
	}

	return &activitypb.ActivityDispatchTask{
		Stamp:            attempt.GetStamp(),
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

// reissueRunningAttemptTimers also reissues a carried start-to-close window during handoff backoff.
func (a *Activity) reissueRunningAttemptTimers(ctx chasm.MutableContext, attempt *activitypb.ActivityAttemptState) error {
	if a.GetAsyncHandoffOptions() != nil {
		return a.issueHandoffTimers(ctx, true, a.hasAttemptInProgress())
	}

	if !a.hasAttemptInProgress() {
		return nil
	}

	return a.issueAttemptTimers(ctx, attempt)
}

func (a *Activity) issueAttemptTimers(ctx chasm.MutableContext, attempt *activitypb.ActivityAttemptState) error {
	if timeout := a.GetStartToCloseTimeout().AsDuration(); timeout > 0 {
		ctx.AddTask(a, chasm.TaskAttributes{ScheduledTime: attempt.GetStartedTime().AsTime().Add(timeout)}, &activitypb.StartToCloseTimeoutTask{Stamp: attempt.GetStamp()})
	}

	if timeout := a.GetHeartbeatTimeout().AsDuration(); timeout > 0 {
		lastHb, _ := a.LastHeartbeat.TryGet(ctx)
		deadline := util.MaxTime(timestamp.TimeValue(lastHb.GetRecordedTime()), timestamp.TimeValue(attempt.GetStartedTime())).Add(timeout)
		ctx.AddTask(a, chasm.TaskAttributes{ScheduledTime: deadline}, &activitypb.HeartbeatTimeoutTask{Stamp: attempt.GetStamp()})
	}

	return nil
}

func (a *Activity) handoffDeadlines(ctx chasm.Context, deliveryActive bool) (handoffDeadlines, error) {
	attempt := a.LastAttempt.Get(ctx)
	state := attempt.GetAsyncHandoffState()
	heartbeat, _ := a.LastHeartbeat.TryGet(ctx)
	options := a.GetAsyncHandoffOptions()

	return calculateHandoffDeadlines(handoffDeadlineInput{
		StartToCloseTimeout:         a.GetStartToCloseTimeout().AsDuration(),
		StartToHandoffTimeout:       options.GetStartToHandoffTimeout().AsDuration(),
		HandoffToCloseTimeout:       options.GetHandoffToCloseTimeout().AsDuration(),
		PreHandoffHeartbeatTimeout:  options.GetHeartbeatTimeout().AsDuration(),
		PostHandoffHeartbeatTimeout: a.GetHeartbeatTimeout().AsDuration(),
		TaskStartedAt:               timestamp.TimeValue(attempt.GetStartedTime()),
		StartToCloseStartedAt:       timestamp.TimeValue(state.GetStartToCloseStartedTime()),
		HandoffAt:                   timestamp.TimeValue(a.GetAsyncHandoffTime()),
		LastHeartbeatAt:             timestamp.TimeValue(heartbeat.GetRecordedTime()),
		DeliveryActive:              deliveryActive,
		AwaitingCompletion:          a.GetAwaitingCompletion(),
	})
}

func (a *Activity) issueHandoffTimers(ctx chasm.MutableContext, issueStartToCloseTimer, deliveryActive bool) error {
	deadlines, err := a.handoffDeadlines(ctx, deliveryActive)
	if err != nil {
		return err
	}

	attempt := a.LastAttempt.Get(ctx)
	state := attempt.GetAsyncHandoffState()
	if issueStartToCloseTimer && state.GetStartToCloseStartedTime() != nil {
		state.StartToCloseStamp++

		if !deadlines.StartToClose.IsZero() {
			ctx.AddTask(a, chasm.TaskAttributes{ScheduledTime: deadlines.StartToClose}, &activitypb.HandoffStartToCloseTimeoutTask{Stamp: state.GetStartToCloseStamp()})
		}
	}

	if !deadlines.StartToHandoff.IsZero() {
		ctx.AddTask(a, chasm.TaskAttributes{ScheduledTime: deadlines.StartToHandoff}, &activitypb.StartToHandoffTimeoutTask{Stamp: attempt.GetStamp()})
	}

	if !deadlines.HandoffToClose.IsZero() {
		ctx.AddTask(a, chasm.TaskAttributes{ScheduledTime: deadlines.HandoffToClose}, &activitypb.HandoffToCloseTimeoutTask{Stamp: attempt.GetStamp()})
	}

	if !deadlines.Heartbeat.IsZero() {
		ctx.AddTask(a, chasm.TaskAttributes{ScheduledTime: deadlines.Heartbeat}, &activitypb.HeartbeatTimeoutTask{Stamp: attempt.GetStamp()})
	}

	return nil
}

func (a *Activity) clearStartToCloseState(ctx chasm.MutableContext) {
	if state := a.LastAttempt.Get(ctx).GetAsyncHandoffState(); state != nil {
		state.StartToCloseStartedTime = nil
		state.StartToCloseStamp++
	}
}

// startAttemptTimers issues timers for the current activity task and records its start-to-close
// start time if absent. Handoff retries preserve the existing start-to-close start time.
func (a *Activity) startAttemptTimers(ctx chasm.MutableContext, attempt *activitypb.ActivityAttemptState) error {
	attempt.PendingRetryType = activitypb.ACTIVITY_RETRY_TYPE_UNSPECIFIED

	if a.GetAsyncHandoffOptions() == nil {
		return a.issueAttemptTimers(ctx, attempt)
	}

	state := attempt.GetAsyncHandoffState()
	if state == nil || state.GetCount() <= 0 {
		return serviceerror.NewInternal("activity handoff attempt is not initialized")
	}

	issueStartToCloseTimer := state.StartToCloseStartedTime == nil
	if issueStartToCloseTimer {
		state.StartToCloseStartedTime = attempt.GetStartedTime()
	}

	return a.issueHandoffTimers(ctx, issueStartToCloseTimer, true)
}

func (a *Activity) resetHandoffAttempt(ctx chasm.MutableContext) {
	attempt := a.LastAttempt.Get(ctx)
	a.clearStartToCloseState(ctx)
	attempt.PendingRetryType = activitypb.ACTIVITY_RETRY_TYPE_UNSPECIFIED
	if a.GetAsyncHandoffOptions() != nil {
		if attempt.AsyncHandoffState == nil {
			attempt.AsyncHandoffState = &activitypb.ActivityAttemptState_AsyncHandoffState{}
		}

		attempt.AsyncHandoffState.Count = 1
	} else if attempt.AsyncHandoffState != nil {
		// Reset only the counter; keep the stamp so future start-to-close timers bump it instead of
		// reusing an old value.
		attempt.AsyncHandoffState.Count = 0
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

func createActivityTimeoutFailure(timeoutType enumspb.TimeoutType) *failurepb.Failure {
	return &failurepb.Failure{
		Message:     fmt.Sprintf(common.FailureReasonActivityTimeout, timeoutType.String()),
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{TimeoutType: timeoutType}},
	}
}

func retryTypeForFailure(optionsPresent, awaitingCompletion bool) activitypb.ActivityRetryType {
	if optionsPresent && !awaitingCompletion {
		return activitypb.ACTIVITY_RETRY_TYPE_HANDOFF
	}

	return activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY
}

// retryTypeForTimeout classifies authoritative server timers, not worker-reported failures.
func retryTypeForTimeout(optionsPresent, awaitingCompletion bool, timeoutType enumspb.TimeoutType) activitypb.ActivityRetryType {
	switch timeoutType {
	case enumspb.TIMEOUT_TYPE_START_TO_CLOSE:
		return activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY
	case enumspb.TIMEOUT_TYPE_HEARTBEAT:
		return retryTypeForFailure(optionsPresent, awaitingCompletion)
	case enumspb.TIMEOUT_TYPE_START_TO_HANDOFF:
		if !optionsPresent {
			return activitypb.ACTIVITY_RETRY_TYPE_UNSPECIFIED
		}

		return activitypb.ACTIVITY_RETRY_TYPE_HANDOFF
	case enumspb.TIMEOUT_TYPE_HANDOFF_TO_CLOSE:
		if !optionsPresent {
			return activitypb.ACTIVITY_RETRY_TYPE_UNSPECIFIED
		}

		return activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY
	default:
		return activitypb.ACTIVITY_RETRY_TYPE_UNSPECIFIED
	}
}

// activeHeartbeatTimeout selects the configured timeout for an opted-in activity's current phase.
func activeHeartbeatTimeout(awaitingCompletion bool, activityTimeout, handoffTimeout time.Duration) time.Duration {
	if !awaitingCompletion {
		return handoffTimeout
	}

	return activityTimeout
}

func nextRetryCounts(optionsPresent bool, typ activitypb.ActivityRetryType, activityAttempt, handoffAttempt int32) (nextActivityAttempt, nextHandoffAttempt int32, err error) {
	if activityAttempt <= 0 || optionsPresent && handoffAttempt <= 0 {
		return 0, 0, serviceerror.NewInternal("activity retry counters must be positive")
	}

	switch typ {
	case activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY:
		if activityAttempt == math.MaxInt32 {
			return 0, 0, serviceerror.NewInternal("activity attempt counter overflow")
		}

		if optionsPresent {
			return activityAttempt + 1, 1, nil
		}

		return activityAttempt + 1, 0, nil
	case activitypb.ACTIVITY_RETRY_TYPE_HANDOFF:
		if !optionsPresent {
			return 0, 0, serviceerror.NewInternal("handoff retry requires async handoff options")
		}

		if handoffAttempt == math.MaxInt32 {
			return 0, 0, serviceerror.NewInternal("handoff attempt counter overflow")
		}

		return activityAttempt, handoffAttempt + 1, nil
	default:
		return 0, 0, serviceerror.NewInternal("invalid activity retry type")
	}
}

// handoffDeadlineInput describes timeouts affected by async handoff. The caller calculates schedule-to-start
// and schedule-to-close separately because their scheduling rules are unchanged by handoff.
type handoffDeadlineInput struct {
	StartToCloseTimeout, StartToHandoffTimeout, HandoffToCloseTimeout time.Duration
	PreHandoffHeartbeatTimeout, PostHandoffHeartbeatTimeout           time.Duration

	// Pickup time of the current activity task; changes on each handoff retry.
	TaskStartedAt time.Time
	// First pickup time used for start-to-close, preserved across handoff retries.
	// Cleared when the attempt ends or fully pauses; the next pickup sets a new start time.
	StartToCloseStartedAt time.Time
	// Accepted handoff time; may be historical when AwaitingCompletion is false.
	HandoffAt       time.Time
	LastHeartbeatAt time.Time

	// True while the current task is running or awaiting external completion.
	// False during retry backoff, queue wait, or full pause.
	DeliveryActive bool
	// True after handoff for the current task; requires DeliveryActive.
	AwaitingCompletion bool
}

type handoffDeadlines struct {
	StartToClose, StartToHandoff, HandoffToClose, Heartbeat time.Time
}

// calculateHandoffDeadlines requires opted-in, normalized options. A nonzero StartToCloseStartedAt
// keeps start-to-close active independently of DeliveryActive. Active deliveries require both
// TaskStartedAt and StartToCloseStartedAt, even when their timeouts are disabled.
func calculateHandoffDeadlines(input handoffDeadlineInput) (handoffDeadlines, error) {
	if input.AwaitingCompletion && !input.DeliveryActive {
		return handoffDeadlines{}, serviceerror.NewInternal("activity completion phase has no active delivery")
	}

	if input.DeliveryActive && input.TaskStartedAt.IsZero() {
		return handoffDeadlines{}, serviceerror.NewInternal("active activity task has no start time")
	}

	if input.DeliveryActive && input.StartToCloseStartedAt.IsZero() {
		return handoffDeadlines{}, serviceerror.NewInternal("active start-to-close budget has no start time")
	}

	if input.AwaitingCompletion && input.HandoffAt.IsZero() {
		return handoffDeadlines{}, serviceerror.NewInternal("activity completion phase has no handoff time")
	}

	var result handoffDeadlines
	if !input.StartToCloseStartedAt.IsZero() && input.StartToCloseTimeout > 0 {
		result.StartToClose = input.StartToCloseStartedAt.Add(input.StartToCloseTimeout)
	}

	if !input.DeliveryActive {
		return result, nil
	}

	phaseStartedAt := input.TaskStartedAt
	if input.AwaitingCompletion {
		phaseStartedAt = input.HandoffAt
		if input.HandoffToCloseTimeout > 0 {
			result.HandoffToClose = phaseStartedAt.Add(input.HandoffToCloseTimeout)
		}
	} else if input.StartToHandoffTimeout > 0 {
		result.StartToHandoff = phaseStartedAt.Add(input.StartToHandoffTimeout)
	}

	heartbeatTimeout := activeHeartbeatTimeout(input.AwaitingCompletion, input.PostHandoffHeartbeatTimeout, input.PreHandoffHeartbeatTimeout)
	if heartbeatTimeout > 0 {
		heartbeatStartedAt := phaseStartedAt
		if input.LastHeartbeatAt.After(heartbeatStartedAt) {
			heartbeatStartedAt = input.LastHeartbeatAt
		}

		result.Heartbeat = heartbeatStartedAt.Add(heartbeatTimeout)
	}

	return result, nil
}
