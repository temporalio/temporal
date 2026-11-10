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
	"go.temporal.io/server/common/primitives/timestamp"
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

	if a.isTerminal() {
		return nil, a.errClosed()
	}

	switch a.Status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_RESET_REQUESTED,
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

	if err := a.applyActivityOptionsUpdate(ctx, frontendReq, updateFields); err != nil {
		return nil, err
	}

	attempt := a.LastAttempt.Get(ctx)

	// Recreate the ScheduleToClose task at the (possibly updated) deadline.
	a.reissueScheduleToClose(ctx)

	attempt.Stamp++

	if err := a.reissueRunningAttemptTimers(ctx, attempt); err != nil {
		return nil, err
	}

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
				AsyncHandoffOptions:    a.GetAsyncHandoffOptions(),
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
//   - the update explicitly changes the governing retry policy, or restores the original options;
//   - the pending interval is still policy-derived (CurrentRetryIntervalSource is RETRY_POLICY).
//     A worker-provided NextRetryDelay override is preserved regardless of its value. An
//     UNSPECIFIED source (attempt state persisted before this field existed) is treated the same
//     as an override, since we can no longer tell whether it was policy-derived or a worker
//     override, and preserving it is the safer default.
func (a *Activity) shouldRecalculateCurrentRetryInterval(
	attempt *activitypb.ActivityAttemptState,
	restoreOriginal bool,
	updateFields map[string]struct{},
	handoffPolicyUpdated bool,
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
		policyUpdated := util.FieldMaskHasPathOrSubPath(updateFields, "retryPolicy")
		if attempt.GetPendingRetryType() == activitypb.ACTIVITY_RETRY_TYPE_HANDOFF {
			policyUpdated = handoffPolicyUpdated
		}

		if !policyUpdated {
			return false
		}
	}

	return attempt.GetCurrentRetryIntervalSource() == activitypb.ACTIVITY_RETRY_INTERVAL_SOURCE_RETRY_POLICY
}

// mergeActivityOptions returns merged and normalized options without changing activity state.
// The structure mirrors the field-mask logic in service/history/api/updateactivityoptions/api.go
func (a *Activity) mergeActivityOptions(
	ctx chasm.Context,
	req *workflowservice.UpdateActivityExecutionOptionsRequest,
) (*apiactivitypb.ActivityOptions, error) {
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
		AsyncHandoffOptions:    common.CloneProto(a.GetAsyncHandoffOptions()),
		StartDelay:             common.CloneProto(a.StartDelay),
	}

	if err := activityoptions.MergeActivityOptions(ao, common.CloneProto(req.GetActivityOptions()), updateFields); err != nil {
		return nil, err
	}

	if util.FieldMaskHasSubPath(updateFields, "retryPolicy") {
		if err := retrypolicy.Validate(ao.GetRetryPolicy()); err != nil {
			return nil, err
		}
	}

	// Normalize merged timeouts based on whether handoff options are present.
	if ao.GetAsyncHandoffOptions() != nil {
		incoming := req.GetActivityOptions().GetAsyncHandoffOptions()
		_, replaceHandoff := updateFields["asyncHandoffOptions"]
		_, replaceHandoffPolicy := updateFields["asyncHandoffOptions.retryPolicy"]

		// Apply defaults when adding handoff options or replacing a whole options/policy message.
		// Leaf edits to existing options preserve unselected policy values.
		needsRetryDefaults := replaceHandoffPolicy ||
			(replaceHandoff && incoming != nil) ||
			(a.GetAsyncHandoffOptions() == nil && incoming != nil && util.FieldMaskHasSubPath(updateFields, "asyncHandoffOptions"))
		var retryDefaults *retrypolicy.DefaultRetrySettings
		if needsRetryDefaults {
			defaults := activityContextFromChasm(ctx).config.DefaultActivityRetryPolicy(ctx.NamespaceEntry().Name().String())
			retryDefaults = &defaults
		}

		if err := validateAndNormalizeHandoffOptions(ao, retryDefaults); err != nil {
			return nil, err
		}
	} else if err := validateAndNormalizeTimeouts(req.GetActivityId(), a.GetActivityType().GetName(), durationpb.New(0), ao); err != nil {
		return nil, err
	}

	return ao, nil
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
		return nil, a.errClosed()
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
		metricsHandler := a.completionMetricsHandler(ctx, metrics.HistoryRespondActivityTaskCanceledScope)
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
	} else {
		actCtx := activityContextFromChasm(ctx)
		if actCtx.config.EnableCancelActivityWorkerCommand(ctx.NamespaceEntry().Name().String()) {
			a.addWorkerCommandDispatchTask(ctx, &activitypb.WorkerCommandDispatchTask{
				Command: &activitypb.WorkerCommandDispatchTask_Cancel{},
			})
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

	if a.isTerminal() {
		return nil, a.errClosed()
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
		return nil, a.errClosed()
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

	if a.isTerminal() {
		return nil, a.errClosed()
	}

	if frontendReq.GetRestoreOriginalOptions() {
		if err := validateOriginalOptionsRestorable(a.GetOriginalOptions()); err != nil {
			return nil, err
		}

		if a.GetAsyncHandoffOptions() == nil && a.GetOriginalOptions().GetAsyncHandoffOptions() != nil &&
			!activityContextFromChasm(ctx).config.StandaloneAsyncHandoffEnabled(ctx.NamespaceEntry().Name().String()) {
			return nil, serviceerror.NewInvalidArgument("async handoff is not enabled for this namespace")
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
	a.clearHandoffState()
	a.resetHandoffAttempt(ctx)

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
	a.applyActivityOptions(a.originalActivityOptions())
	a.reissueScheduleToClose(ctx)
}

// pendingRetryType distinguishes an old main retry from an initial queue wait or a live delivery.
func (a *Activity) pendingRetryType(attempt *activitypb.ActivityAttemptState) activitypb.ActivityRetryType {
	pending := (a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED || a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED) &&
		attempt.GetCurrentRetryInterval() != nil
	if !pending {
		return activitypb.ACTIVITY_RETRY_TYPE_UNSPECIFIED
	}

	if attempt.GetPendingRetryType() == activitypb.ACTIVITY_RETRY_TYPE_UNSPECIFIED {
		return activitypb.ACTIVITY_RETRY_TYPE_ACTIVITY
	}

	return attempt.GetPendingRetryType()
}

func (a *Activity) originalActivityOptions() *apiactivitypb.ActivityOptions {
	options := common.CloneProto(a.GetOriginalOptions())

	// Restoring start delay after pickup would shift the schedule-to-close deadline.
	if a.GetFirstAttemptStartedTime() != nil {
		options.StartDelay = common.CloneProto(a.GetStartDelay())
	}

	return options
}

func (a *Activity) applyActivityOptions(options *apiactivitypb.ActivityOptions) {
	a.TaskQueue = options.TaskQueue
	a.ScheduleToCloseTimeout = options.ScheduleToCloseTimeout
	a.ScheduleToStartTimeout = options.ScheduleToStartTimeout
	a.StartToCloseTimeout = options.StartToCloseTimeout
	a.HeartbeatTimeout = options.HeartbeatTimeout
	a.Priority = options.Priority
	a.RetryPolicy = options.RetryPolicy
	a.AsyncHandoffOptions = options.AsyncHandoffOptions
	a.StartDelay = options.StartDelay
}

// applyActivityOptionsUpdate validates merged options and live handoff/retry changes before applying them.
func (a *Activity) applyActivityOptionsUpdate(
	ctx chasm.MutableContext,
	req *workflowservice.UpdateActivityExecutionOptionsRequest,
	updateFields map[string]struct{},
) error {
	hadHandoffOptions := a.GetAsyncHandoffOptions() != nil
	incoming := req.GetActivityOptions().GetAsyncHandoffOptions()
	_, replaceHandoff := updateFields["asyncHandoffOptions"]

	// Replacing existing handoff options resets an omitted retry policy to namespace defaults,
	// so any pending policy-derived retry backoff is recalculated. Adding or re-adding options
	// without a supplied retry policy preserves the pending retry backoff.
	handoffPolicyUpdated := util.FieldMaskHasPathOrSubPath(updateFields, "asyncHandoffOptions.retryPolicy") ||
		(replaceHandoff && incoming != nil && (hadHandoffOptions || incoming.GetRetryPolicy() != nil))

	var options *apiactivitypb.ActivityOptions
	if req.GetRestoreOriginal() {
		options = a.originalActivityOptions()
	} else {
		var err error
		options, err = a.mergeActivityOptions(ctx, req)
		if err != nil {
			return err
		}
	}

	// Check new opt-in admission before changing execution state.
	willHaveHandoffOptions := options.GetAsyncHandoffOptions() != nil
	if !hadHandoffOptions && willHaveHandoffOptions && !activityContextFromChasm(ctx).config.StandaloneAsyncHandoffEnabled(ctx.NamespaceEntry().Name().String()) {
		return serviceerror.NewInvalidArgument("async handoff is not enabled for this namespace")
	}

	if err := a.applyRetryAndHandoffStateUpdate(ctx, options, req.GetRestoreOriginal(), updateFields, handoffPolicyUpdated); err != nil {
		return err
	}

	a.applyActivityOptions(options)
	return nil
}

// applyRetryAndHandoffStateUpdate validates and calculates retry/handoff changes using the current
// state and resulting options. It applies those changes only after all checks pass.
func (a *Activity) applyRetryAndHandoffStateUpdate(
	ctx chasm.MutableContext,
	options *apiactivitypb.ActivityOptions,
	restoreOriginal bool,
	updateFields map[string]struct{},
	handoffPolicyUpdated bool,
) error {
	attempt := a.LastAttempt.Get(ctx)
	hadHandoffOptions := a.GetAsyncHandoffOptions() != nil
	willHaveHandoffOptions := options.GetAsyncHandoffOptions() != nil

	pendingType := a.pendingRetryType(attempt)
	handoffAttempt, startToCloseStartedAt, err := a.handoffStateForOptionsUpdate(ctx, willHaveHandoffOptions)
	if err != nil {
		return err
	}

	// Recalculate a pending policy-derived delay only when its governing policy was updated.
	pendingHandoffPolicyAbsent := pendingType == activitypb.ACTIVITY_RETRY_TYPE_HANDOFF && !willHaveHandoffOptions
	var updatedRetryInterval *durationpb.Duration
	if !pendingHandoffPolicyAbsent && a.shouldRecalculateCurrentRetryInterval(attempt, restoreOriginal, updateFields, handoffPolicyUpdated) {
		policy, count := options.GetRetryPolicy(), attempt.GetCount()
		if pendingType == activitypb.ACTIVITY_RETRY_TYPE_HANDOFF {
			policy, count = options.GetAsyncHandoffOptions().GetRetryPolicy(), handoffAttempt
		}

		if count <= 0 {
			return serviceerror.NewInternal("pending activity retry counter is not initialized")
		}

		updatedRetryInterval = durationpb.New(backoff.CalculateExponentialRetryInterval(policy, max(count-1, 1)))
	}

	// All checks have passed; apply the handoff state and pending retry changes.
	if hadHandoffOptions != willHaveHandoffOptions {
		a.clearStartToCloseState(ctx)
	}

	if willHaveHandoffOptions && attempt.AsyncHandoffState == nil {
		attempt.AsyncHandoffState = &activitypb.ActivityAttemptState_AsyncHandoffState{}
	}

	if state := attempt.GetAsyncHandoffState(); state != nil {
		state.Count = handoffAttempt
		state.StartToCloseStartedTime = nil
		if !startToCloseStartedAt.IsZero() {
			state.StartToCloseStartedTime = timestamppb.New(startToCloseStartedAt)
		}
	}

	attempt.PendingRetryType = pendingType
	if updatedRetryInterval != nil {
		attempt.CurrentRetryInterval = updatedRetryInterval

		// If the new backoff already elapsed, dispatch now and start schedule-to-start from now.
		retryDispatchTime := timestamp.TimeValue(dispatchTimeForRetry(attempt))
		attempt.DispatchTime = timestamppb.New(util.MaxTime(retryDispatchTime, ctx.Now(a)))
	}

	return nil
}

// handoffStateForOptionsUpdate returns the resulting handoff count and start-to-close start time.
// It only calculates return values; it does not update the activity's fields.
// The count is 0 when options are absent, 1 when added, or retained otherwise.
// The start time is preserved, set to the current task's pickup time on live opt-in, or zero when
// no clock should run. Inconsistent current state returns an error.
func (a *Activity) handoffStateForOptionsUpdate(ctx chasm.Context, willHaveHandoffOptions bool) (int32, time.Time, error) {
	attempt := a.LastAttempt.Get(ctx)
	hadHandoffOptions := a.GetAsyncHandoffOptions() != nil
	pendingType := a.pendingRetryType(attempt)
	if !hadHandoffOptions && !willHaveHandoffOptions && pendingType != activitypb.ACTIVITY_RETRY_TYPE_HANDOFF {
		return 0, time.Time{}, nil
	}

	state := attempt.GetAsyncHandoffState()
	deliveryActive := a.hasAttemptInProgress()
	paused := a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_PAUSED
	taskStartedAt := timestamp.TimeValue(attempt.GetStartedTime())
	startToCloseStartedAt := timestamp.TimeValue(state.GetStartToCloseStartedTime())

	if a.GetAwaitingCompletion() && (!deliveryActive || timestamp.TimeValue(a.GetAsyncHandoffTime()).IsZero()) {
		return 0, time.Time{}, serviceerror.NewInternal("activity completion phase has no active delivery or handoff time")
	}

	if deliveryActive && taskStartedAt.IsZero() {
		return 0, time.Time{}, serviceerror.NewInternal("active activity task has invalid pickup state")
	}

	if hadHandoffOptions && (state.GetCount() <= 0 || deliveryActive && startToCloseStartedAt.IsZero()) {
		return 0, time.Time{}, serviceerror.NewInternal("activity handoff state is not initialized")
	}

	// Removing options must not bypass the state checks above.
	if !willHaveHandoffOptions {
		return 0, time.Time{}, nil
	}

	if hadHandoffOptions {
		if paused {
			return state.GetCount(), time.Time{}, nil
		}

		return state.GetCount(), startToCloseStartedAt, nil
	}

	if deliveryActive {
		return 1, taskStartedAt, nil
	}

	return 1, time.Time{}, nil
}
