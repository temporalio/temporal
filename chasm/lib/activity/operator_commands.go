package activity

import (
	"fmt"
	"math/rand"
	"time"

	apiactivitypb "go.temporal.io/api/activity/v1" //nolint:importas
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/activityoptions"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/util"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

//revive:disable-next-line:cognitive-complexity
func (a *Activity) UpdateActivityExecutionOptions(
	ctx chasm.MutableContext,
	req *activitypb.UpdateActivityExecutionOptionsRequest,
) (*activitypb.UpdateActivityExecutionOptionsResponse, error) {
	frontendReq := req.GetFrontendRequest()
	requestID := frontendReq.GetRequestId()
	if requestID != "" && requestID == a.GetLastUpdateOptionsRequestId() {
		// A repeated request ID returns the current options, which may differ from the original response.
		return a.updateActivityExecutionOptionsResponse(), nil
	}

	switch a.Status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
		activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
		activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED:
		return nil, serviceerror.NewFailedPreconditionf("Cannot update options for activity in state %s", a.Status.String())
	default:
	}

	if a.ResetRestoreOptions {
		return nil, serviceerror.NewFailedPrecondition(
			"cannot update options while a deferred Reset(RestoreOriginalOptions) is pending")
	}

	if frontendReq.GetRestoreOriginal() {
		if err := validateOriginalOptionsRestorable(a.GetOriginalOptions()); err != nil {
			return nil, err
		}
	}

	updateFields := map[string]struct{}{}
	if mask := frontendReq.GetUpdateMask(); mask != nil {
		updateFields = util.ParseFieldMask(mask)
	}

	_, hasStartDelayInMask := updateFields["startDelay"]
	if hasStartDelayInMask {
		newDelay := frontendReq.GetActivityOptions().GetStartDelay()
		if err := validateStartDelay(newDelay); err != nil {
			return nil, err
		}
		if newDelay.AsDuration() > 0 {
			actCtx := activityContextFromChasm(ctx)
			if !actCtx.config.StartDelayEnabled(frontendReq.GetNamespace()) {
				return nil, serviceerror.NewInvalidArgument("start_delay is not enabled for this namespace")
			}
		}
		inDelayWindow := a.firstDispatchTime().After(ctx.Now(a))
		pausedBeforeFirstAttempt := a.GetFirstAttemptStartedTime() == nil && a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED
		if !(inDelayWindow || pausedBeforeFirstAttempt) { //nolint:staticcheck // QF1001: DeMorgan rearrangement would not be an improvement
			return nil, serviceerror.NewFailedPrecondition(
				"cannot update start_delay: the first activity attempt has already been dispatched")
		}
	}

	attempt := a.LastAttempt.Get(ctx)

	if frontendReq.GetRestoreOriginal() {
		ogOptions := a.GetOriginalOptions()
		a.TaskQueue = common.CloneProto(ogOptions.GetTaskQueue())
		a.ScheduleToCloseTimeout = common.CloneProto(ogOptions.GetScheduleToCloseTimeout())
		a.ScheduleToStartTimeout = common.CloneProto(ogOptions.GetScheduleToStartTimeout())
		a.StartToCloseTimeout = common.CloneProto(ogOptions.GetStartToCloseTimeout())
		a.HeartbeatTimeout = common.CloneProto(ogOptions.GetHeartbeatTimeout())
		a.RetryPolicy = common.CloneProto(ogOptions.GetRetryPolicy())
		a.Priority = common.CloneProto(ogOptions.GetPriority())
		// start_delay only governs the first dispatch. Once the first attempt has started, restoring
		// the original value would shift ScheduleToClose without affecting dispatch timing.
		if a.GetFirstAttemptStartedTime() == nil {
			a.StartDelay = common.CloneProto(ogOptions.GetStartDelay())
		}
	} else {
		if err := a.mergeActivityOptions(frontendReq); err != nil {
			return nil, err
		}
	}

	// Recalculate policy-derived retry intervals based on the (possibly updated) retry policy.
	// Worker-provided NextRetryDelay values are preserved for their already-scheduled retry.
	if a.shouldRecalculateCurrentRetryInterval(attempt, frontendReq.GetRestoreOriginal(), updateFields) {
		newInterval := backoff.CalculateExponentialRetryInterval(a.RetryPolicy, attempt.GetCount()-1)
		attempt.CurrentRetryInterval = durationpb.New(newInterval)
	}

	// Recreate the ScheduleToClose task at the (possibly updated) deadline.
	a.reissueScheduleToClose(ctx)

	attempt.Stamp++

	a.reissueRunningAttemptTimers(ctx, attempt)
	if a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED {
		a.reissueDispatchAndScheduleToStart(ctx, attempt)
	}

	metricsHandler := a.enrichedMetricsHandler(ctx, metrics.ActivityUpdateOptionsScope)
	a.emitOnUpdateOptionsMetrics(metricsHandler)

	if requestID != "" {
		a.LastUpdateOptionsRequestId = requestID
	}
	return a.updateActivityExecutionOptionsResponse(), nil
}

func (a *Activity) updateActivityExecutionOptionsResponse() *activitypb.UpdateActivityExecutionOptionsResponse {
	return &activitypb.UpdateActivityExecutionOptionsResponse{
		FrontendResponse: &workflowservice.UpdateActivityExecutionOptionsResponse{
			ActivityOptions: &apiactivitypb.ActivityOptions{
				TaskQueue:              a.GetTaskQueue(),
				ScheduleToCloseTimeout: a.GetScheduleToCloseTimeout(),
				ScheduleToStartTimeout: a.GetScheduleToStartTimeout(),
				StartToCloseTimeout:    a.GetStartToCloseTimeout(),
				HeartbeatTimeout:       a.GetHeartbeatTimeout(),
				RetryPolicy:            a.GetRetryPolicy(),
				Priority:               a.GetPriority(),
				StartDelay:             a.GetStartDelay(),
			},
		},
	}
}

// shouldRecalculateCurrentRetryInterval reports whether the pending retry's CurrentRetryInterval
// should be re-derived from the (possibly updated) retry policy. All of the following must hold:
//
//   - the activity is waiting to retry, i.e. in SCHEDULED or PAUSED state (a running attempt has no
//     pending backoff to recalculate);
//   - a retry is actually pending (CurrentRetryInterval is set);
//   - the update touches the retry policy: either RestoreOriginal (which replaces the whole policy),
//     or the update mask includes "retryPolicy" or one of its subfields;
//   - the pending interval is still policy-derived (CurrentRetryIntervalSource is RETRY_POLICY).
//     A worker-provided NextRetryDelay override is preserved regardless of its value. An
//     UNSPECIFIED source (attempt state persisted before this field existed) is treated the same
//     as an override, since we can no longer tell whether it was policy-derived or a worker
//     override, and preserving it is the safer default.
func (a *Activity) shouldRecalculateCurrentRetryInterval(
	attempt *activitypb.ActivityAttemptState,
	restoreOriginal bool,
	updateFields map[string]struct{},
) bool {
	status := a.GetStatus()
	if status != activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED &&
		status != activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED {
		return false
	}

	if attempt.GetCurrentRetryInterval() == nil {
		return false
	}

	if !restoreOriginal {
		if !util.FieldMaskHasPathOrSubPath(updateFields, "retryPolicy") {
			return false
		}
	}

	return attempt.GetCurrentRetryIntervalSource() == activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY
}

// mergeActivityOptions applies the field mask from the request to the activity state.
// The structure mirrors the field-mask logic in service/history/api/updateactivityoptions/api.go
func (a *Activity) mergeActivityOptions(
	req *workflowservice.UpdateActivityExecutionOptionsRequest,
) error {
	updateFields := util.ParseFieldMask(req.GetUpdateMask())

	// Build an ActivityOptions view of the current Activity state so we can use the shared merge function.
	ao := &apiactivitypb.ActivityOptions{
		TaskQueue:              common.CloneProto(a.TaskQueue),
		ScheduleToCloseTimeout: common.CloneProto(a.ScheduleToCloseTimeout),
		ScheduleToStartTimeout: common.CloneProto(a.ScheduleToStartTimeout),
		StartToCloseTimeout:    common.CloneProto(a.StartToCloseTimeout),
		HeartbeatTimeout:       common.CloneProto(a.HeartbeatTimeout),
		Priority:               common.CloneProto(a.Priority),
		RetryPolicy:            common.CloneProto(a.RetryPolicy),
		StartDelay:             common.CloneProto(a.StartDelay),
	}

	if err := activityoptions.MergeActivityOptions(ao, common.CloneProto(req.GetActivityOptions()), updateFields); err != nil {
		return err
	}

	if util.FieldMaskHasSubPath(updateFields, "retryPolicy") {
		if err := retrypolicy.Validate(ao.GetRetryPolicy()); err != nil {
			return err
		}
	}

	// Re-normalize timeouts after the update so that relationships like
	// start_to_close <= schedule_to_close and heartbeat <= start_to_close are preserved.
	// This mirrors adjustActivityOptions for workflow-embedded activities.
	if err := validateAndNormalizeTimeouts(req.GetActivityId(), a.GetActivityType().GetName(), durationpb.New(0), ao); err != nil {
		return err
	}

	// Write the merged and normalized options back to the Activity state fields.
	a.TaskQueue = ao.TaskQueue
	a.ScheduleToCloseTimeout = ao.ScheduleToCloseTimeout
	a.ScheduleToStartTimeout = ao.ScheduleToStartTimeout
	a.StartToCloseTimeout = ao.StartToCloseTimeout
	a.HeartbeatTimeout = ao.HeartbeatTimeout
	a.Priority = ao.Priority
	a.RetryPolicy = ao.RetryPolicy
	a.StartDelay = ao.StartDelay

	return nil
}

func (a *Activity) handleCancellationRequested(ctx chasm.MutableContext, request *activitypb.RequestCancelActivityExecutionRequest) (
	*activitypb.RequestCancelActivityExecutionResponse, error,
) {
	req := request.GetFrontendRequest()
	newReqID := req.GetRequestId()
	existingReqID := a.GetCancelState().GetRequestId()

	// Deduplicate first because a retry may arrive after the activity transitions to Canceled.
	if newReqID != "" && existingReqID == newReqID {
		return &activitypb.RequestCancelActivityExecutionResponse{}, nil
	}

	if a.isTerminal() {
		return nil, serviceerror.NewFailedPreconditionf("activity is in terminal state %v", a.GetStatus())
	}

	// Reject a second cancellation request with a different request ID.
	if a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED {
		return nil, serviceerror.NewFailedPrecondition(
			fmt.Sprintf("cancellation already requested with request ID %s", existingReqID))
	}

	hasAttemptInProgress := a.hasAttemptInProgress()
	originalStatus := a.GetStatus()

	// Always transition to CancelRequested
	// TODO: this is questionable, since CancelRequested is otherwise a state that implies an attempt
	// is in progress.
	if err := TransitionCancelRequested.Apply(a, ctx, req); err != nil {
		return nil, err
	}

	// Transition to Canceled if no attempt in progress; otherwise wait for worker response.
	if !hasAttemptInProgress {
		metricsHandler := a.enrichedMetricsHandler(ctx, metrics.HistoryRespondActivityTaskCanceledScope)
		err := TransitionCanceled.Apply(a, ctx, cancelEvent{
			metricsHandler: metricsHandler,
			fromStatus:     originalStatus,
			details: &commonpb.Payloads{
				Payloads: []*commonpb.Payload{
					payload.EncodeString(req.GetReason()),
				},
			},
		})
		if err != nil {
			return nil, err
		}
	}

	return &activitypb.RequestCancelActivityExecutionResponse{}, nil
}

func (a *Activity) handlePauseRequested(ctx chasm.MutableContext, req *activitypb.PauseActivityExecutionRequest) (
	*activitypb.PauseActivityExecutionResponse, error,
) {
	// Deduplicate a replay of a request that already paused this activity, even if the
	// activity has since been unpaused. Without this check, a delayed replay of an old
	// Pause request would silently re-pause an activity that a later Unpause resumed.
	newReqID := req.GetFrontendRequest().GetRequestId()
	if newReqID != "" && a.LastPauseState.GetRequestId() == newReqID {
		return &activitypb.PauseActivityExecutionResponse{}, nil
	}

	canPause := TransitionPaused.Possible(a)
	canRequestPause := TransitionPauseRequested.Possible(a)
	if !canPause && !canRequestPause {
		return nil, serviceerror.NewFailedPreconditionf("activity is in non-pausable state %v", a.GetStatus())
	}

	metricsHandler := a.enrichedMetricsHandler(ctx, metrics.ActivityPausedScope)

	event := pauseEvent{req: req.GetFrontendRequest(), metricsHandler: metricsHandler}
	if canPause {
		if err := TransitionPaused.Apply(a, ctx, event); err != nil {
			return nil, err
		}
	} else {
		if err := TransitionPauseRequested.Apply(a, ctx, event); err != nil {
			return nil, err
		}
	}
	return &activitypb.PauseActivityExecutionResponse{}, nil
}

func (a *Activity) handleUnpauseRequested(ctx chasm.MutableContext, req *activitypb.UnpauseActivityExecutionRequest) (
	_ *activitypb.UnpauseActivityExecutionResponse, retErr error,
) {
	frontendReq := req.GetFrontendRequest()
	requestID := frontendReq.GetRequestId()
	if requestID != "" && requestID == a.GetLastUnpauseRequestId() {
		return &activitypb.UnpauseActivityExecutionResponse{}, nil
	}
	if requestID != "" {
		defer func() {
			if retErr == nil {
				a.LastUnpauseRequestId = requestID
			}
		}()
	}

	if a.isTerminal() {
		return nil, serviceerror.NewFailedPreconditionf("activity is in terminal state %v", a.GetStatus())
	}
	metricsHandler := a.enrichedMetricsHandler(ctx, metrics.ActivityUnpausedScope)

	event := unpauseEvent{req: frontendReq, metricsHandler: metricsHandler}
	switch {
	case TransitionUnpaused.Possible(a):
		if err := TransitionUnpaused.Apply(a, ctx, event); err != nil {
			return nil, err
		}
	case TransitionUnpausedWhilePauseRequested.Possible(a):
		if err := TransitionUnpausedWhilePauseRequested.Apply(a, ctx, event); err != nil {
			return nil, err
		}
	default:
		return nil, serviceerror.NewFailedPreconditionf("activity is in non-unpausable state %v", a.GetStatus())
	}
	a.emitOnUnpausedMetrics(metricsHandler)
	return &activitypb.UnpauseActivityExecutionResponse{}, nil
}

// isPaused reports whether the activity is currently paused (waiting) or has a pending pause request
// (worker still running).
func (a *Activity) isPaused() bool {
	switch a.GetStatus() {
	case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED,
		activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED:
		return true
	case activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED:
		return a.GetResetShouldPause()
	default:
		return false
	}
}

// unpauseDispatchTime computes when an unpaused attempt should be dispatched
func (a *Activity) unpauseDispatchTime(ctx chasm.MutableContext, event unpauseEvent) time.Time {
	unpauseTime := ctx.Now(a)
	if jitter := event.req.GetJitter().AsDuration(); jitter > 0 {
		unpauseTime = unpauseTime.Add(time.Duration(rand.Int63n(int64(jitter)))) //nolint:gosec
	}
	dispatchTime := a.dispatchTimeRespectingStartDelay(unpauseTime)
	retryDispatchTime := dispatchTimeForRetry(a.LastAttempt.Get(ctx))
	if retryDispatchTime != nil && retryDispatchTime.AsTime().After(dispatchTime) {
		return retryDispatchTime.AsTime()
	}
	return dispatchTime
}

func (a *Activity) recordPauseState(
	ctx chasm.MutableContext,
	event pauseEvent,
) {
	a.LastPauseState = &activitypb.ActivityPauseState{
		PauseTime: timestamppb.New(ctx.Now(a)),
		Identity:  event.req.GetIdentity(),
		Reason:    event.req.GetReason(),
		RequestId: event.req.GetRequestId(),
	}
	a.emitOnPausedMetrics(event.metricsHandler)
}

func (a *Activity) clearHeartbeatDetails(ctx chasm.MutableContext) {
	if hb, ok := a.LastHeartbeat.TryGet(ctx); ok {
		hb.Details = nil
		hb.RecordedTime = nil
	}
}

// handleReset handles the activity execution reset.
//
// For SCHEDULED and PAUSED activities (no worker running): re-dispatches at attempt 1. Any pending
// retry backoff is discarded (reset clears CurrentRetryInterval), but a pending start_delay is
// honored so the re-dispatched attempt 1 does not fire before its original requested start time. A
// PAUSED activity is unpaused first — unless keepPaused is set, in which case the counter is reset
// to 1 but the activity stays PAUSED until a later unpause.
//
// For STARTED activities: transitions to RESET_REQUESTED. The worker is notified via
// ActivityReset=true on its next heartbeat response and continues to use its existing task token.
// If the attempt fails, the activity transitions back to SCHEDULED at attempt 1 via
// TransitionResetAttemptFailedToScheduled.
//
// RestoreOriginalOptions follows the same split: applied immediately for a non-running activity,
// and deferred for a running one, so the in-flight attempt is not disturbed — every restored option
// takes effect on the new attempt 1.
//
// For CANCEL_REQUESTED activities: rejected with FailedPrecondition; cancel takes precedence.
func (a *Activity) handleReset(
	ctx chasm.MutableContext,
	req *activitypb.ResetActivityExecutionRequest,
) (_ *activitypb.ResetActivityExecutionResponse, retErr error) {
	frontendReq := req.GetFrontendRequest()
	requestID := frontendReq.GetRequestId()
	if requestID != "" && requestID == a.GetLastResetRequestId() {
		return &activitypb.ResetActivityExecutionResponse{}, nil
	}
	if requestID != "" {
		defer func() {
			if retErr == nil {
				a.LastResetRequestId = requestID
			}
		}()
	}

	if frontendReq.GetRestoreOriginalOptions() {
		if err := validateOriginalOptionsRestorable(a.GetOriginalOptions()); err != nil {
			return nil, err
		}
	}

	metricsHandler := a.enrichedMetricsHandler(ctx, metrics.ActivityResetScope)

	switch a.Status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED:
		return nil, serviceerror.NewFailedPrecondition("cannot reset an activity with a pending cancellation")
	case activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED:
		// A reset is already pending on this running attempt. For now we reject a second reset
		// request clearly.
		// TODO (dan): define desired behavior and implement
		return nil, serviceerror.NewFailedPrecondition("cannot reset an activity with a pending reset")
	case activitypb.ACTIVITY_EXECUTION_STATUS_STARTED, activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED:
		return a.deferResetWhileRunning(ctx, frontendReq, metricsHandler)
	case activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED:
		// No worker is running; restore takes effect immediately.
		if frontendReq.GetRestoreOriginalOptions() {
			a.restoreOriginalOptions(ctx)
		}
		if frontendReq.GetKeepPaused() {
			return a.resetKeepPaused(ctx, frontendReq, metricsHandler)
		}
		// No keepPaused: perform an immediate reset. restoreOriginalOptions (if requested) already
		// ran above, so skip it in resetImmediately to avoid restoring twice.
		return a.resetImmediately(ctx, frontendReq, metricsHandler, false)
	case activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED:
		return a.resetImmediately(ctx, frontendReq, metricsHandler, frontendReq.GetRestoreOriginalOptions())
	default:
		// Terminal or unspecified state.
		return nil, serviceerror.NewFailedPrecondition("activity execution is not running")
	}
}

// deferResetWhileRunning defers reset mutations (option restore, heartbeat details clear,
// attempt-count rewind) while a worker is still executing (STARTED or PAUSE_REQUESTED), so that the
// in-flight attempt is undisturbed. The deferred reset applies when the worker yields.
func (a *Activity) deferResetWhileRunning(
	ctx chasm.MutableContext,
	frontendReq *workflowservice.ResetActivityExecutionRequest,
	metricsHandler metrics.Handler,
) (*activitypb.ResetActivityExecutionResponse, error) {
	keepPaused := frontendReq.GetKeepPaused()
	pauseRequested := a.Status == activitypb.ACTIVITY_EXECUTION_STATUS_PAUSE_REQUESTED

	if pauseRequested && !keepPaused {
		// Unpause; the deferred reset will apply on the next retry via STARTED->SCHEDULED.
		if err := TransitionUnpausedWhilePauseRequested.Apply(a, ctx, unpauseEvent{
			req:            &workflowservice.UnpauseActivityExecutionRequest{},
			metricsHandler: metricsHandler,
		}); err != nil {
			return nil, err
		}
	}
	if frontendReq.GetRestoreOriginalOptions() {
		a.ResetRestoreOptions = true
	}
	if frontendReq.GetResetHeartbeat() {
		a.ResetShouldClearHeartbeat = true
	}
	// keepPaused on a paused (PAUSE_REQUESTED) activity preserves the pause: when the worker
	// yields the activity lands back in PAUSED rather than SCHEDULED.
	a.ResetShouldPause = keepPaused && pauseRequested
	if err := TransitionResetRequested.Apply(a, ctx, nil); err != nil {
		return nil, err
	}
	a.emitOnResetMetrics(metricsHandler)
	return &activitypb.ResetActivityExecutionResponse{}, nil
}

func (a *Activity) resetKeepPaused(
	ctx chasm.MutableContext,
	frontendReq *workflowservice.ResetActivityExecutionRequest,
	metricsHandler metrics.Handler,
) (*activitypb.ResetActivityExecutionResponse, error) {
	attempt := a.LastAttempt.Get(ctx)
	attempt.Count = 1
	attempt.Stamp++
	attempt.CurrentRetryInterval = nil
	attempt.CurrentRetryIntervalSource = activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_UNSPECIFIED
	attempt.DispatchTime = nil
	if frontendReq.GetResetHeartbeat() {
		a.clearHeartbeatDetails(ctx)
	}
	a.emitOnResetMetrics(metricsHandler)
	return &activitypb.ResetActivityExecutionResponse{}, nil
}

func (a *Activity) resetImmediately(
	ctx chasm.MutableContext,
	frontendReq *workflowservice.ResetActivityExecutionRequest,
	metricsHandler metrics.Handler,
	restore bool,
) (*activitypb.ResetActivityExecutionResponse, error) {
	if restore {
		a.restoreOriginalOptions(ctx)
	}
	resetTime := ctx.Now(a)
	if jitter := frontendReq.GetJitter().AsDuration(); jitter > 0 {
		resetTime = resetTime.Add(time.Duration(rand.Int63n(int64(jitter)))) //nolint:gosec
	}
	if err := TransitionReset.Apply(a, ctx, resetEvent{
		req:            frontendReq,
		resetTime:      resetTime,
		metricsHandler: metricsHandler,
	}); err != nil {
		return nil, err
	}
	return &activitypb.ResetActivityExecutionResponse{}, nil
}

// applyDeferredOptionRestore applies a Reset(RestoreOriginalOptions) that was deferred because a
// worker was running an attempt at reset time (see handleReset).
func (a *Activity) applyDeferredOptionRestore(ctx chasm.MutableContext) {
	if !a.ResetRestoreOptions {
		return
	}
	a.ResetRestoreOptions = false
	a.restoreOriginalOptions(ctx)
}

// applyDeferredHeartbeatClear applies a Reset(ResetHeartbeat) that was deferred because a worker was
// running an attempt at reset time (see handleReset).
func (a *Activity) applyDeferredHeartbeatClear(ctx chasm.MutableContext) {
	if !a.ResetShouldClearHeartbeat {
		return
	}
	a.ResetShouldClearHeartbeat = false
	a.clearHeartbeatDetails(ctx)
}

// restoreOriginalOptions resets the activity's options to the values it was originally scheduled
// with and reissues the ScheduleToClose timer at the resulting deadline. start_delay is restored
// only if the activity has never started.
func (a *Activity) restoreOriginalOptions(ctx chasm.MutableContext) {
	og := a.GetOriginalOptions()
	a.TaskQueue = common.CloneProto(og.GetTaskQueue())
	a.ScheduleToCloseTimeout = common.CloneProto(og.GetScheduleToCloseTimeout())
	a.ScheduleToStartTimeout = common.CloneProto(og.GetScheduleToStartTimeout())
	a.StartToCloseTimeout = common.CloneProto(og.GetStartToCloseTimeout())
	a.HeartbeatTimeout = common.CloneProto(og.GetHeartbeatTimeout())
	a.RetryPolicy = common.CloneProto(og.GetRetryPolicy())
	a.Priority = common.CloneProto(og.GetPriority())
	if a.GetFirstAttemptStartedTime() == nil {
		a.StartDelay = common.CloneProto(og.GetStartDelay())
	}
	a.reissueScheduleToClose(ctx)
}
