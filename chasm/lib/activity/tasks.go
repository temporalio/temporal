package activity

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/util"
	"go.uber.org/fx"
)

// DispatchTaskHook wraps an activity dispatch on the active cluster after CHASM validation and before it is sent to Matching.
type DispatchTaskHook func(
	context.Context,
	string,
	*activitypb.ActivityDispatchTask,
	func(context.Context) error,
) error

type activityDispatchTaskHandlerOptions struct {
	fx.In

	MatchingClient   resource.MatchingClient
	DispatchTaskHook DispatchTaskHook `optional:"true"`
}

type activityDispatchTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*activitypb.ActivityDispatchTask]
	opts activityDispatchTaskHandlerOptions
}

func newActivityDispatchTaskHandler(opts activityDispatchTaskHandlerOptions) *activityDispatchTaskHandler {
	return &activityDispatchTaskHandler{
		opts: opts,
	}
}

func (h *activityDispatchTaskHandler) Validate(
	ctx chasm.Context,
	activity *Activity,
	_ chasm.TaskInvocation,
	task *activitypb.ActivityDispatchTask,
) (bool, error) {
	return (TransitionStarted.Possible(activity) &&
		task.Stamp == activity.LastAttempt.Get(ctx).GetStamp()), nil
}

func (h *activityDispatchTaskHandler) Execute(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	_ chasm.TaskAttributes,
	task *activitypb.ActivityDispatchTask,
) error {
	request, err := h.createMatchingRequest(ctx, activityRef)
	if err != nil {
		return err
	}

	if h.opts.DispatchTaskHook != nil {
		return h.opts.DispatchTaskHook(
			ctx,
			activityRef.NamespaceID,
			task,
			func(ctx context.Context) error {
				return h.sendToMatching(ctx, request)
			},
		)
	}

	return h.sendToMatching(ctx, request)
}

// Discard spills the task to matching instead of silently discarding it on standby clusters when the activity
// dispatch task has been pending past the discard delay.
func (h *activityDispatchTaskHandler) Discard(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *activitypb.ActivityDispatchTask,
) error {
	return h.dispatchActivity(ctx, activityRef)
}

func (h *activityDispatchTaskHandler) dispatchActivity(
	ctx context.Context,
	activityRef chasm.ComponentRef,
) error {
	request, err := h.createMatchingRequest(ctx, activityRef)
	if err != nil {
		return err
	}

	return h.sendToMatching(ctx, request)
}

func (h *activityDispatchTaskHandler) createMatchingRequest(
	ctx context.Context,
	activityRef chasm.ComponentRef,
) (*matchingservice.AddActivityTaskRequest, error) {
	return chasm.ReadComponent(
		ctx,
		activityRef,
		(*Activity).createAddActivityTaskRequest,
		activityRef.NamespaceID,
	)
}

func (h *activityDispatchTaskHandler) sendToMatching(
	ctx context.Context,
	request *matchingservice.AddActivityTaskRequest,
) error {
	_, err := h.opts.MatchingClient.AddActivityTask(ctx, request)

	return err
}

type scheduleToStartTimeoutTaskHandler struct {
	chasm.PureTaskHandlerBase
}

func newScheduleToStartTimeoutTaskHandler() *scheduleToStartTimeoutTaskHandler {
	return &scheduleToStartTimeoutTaskHandler{}
}

func (h *scheduleToStartTimeoutTaskHandler) Validate(
	ctx chasm.Context,
	activity *Activity,
	_ chasm.TaskInvocation,
	task *activitypb.ScheduleToStartTimeoutTask,
) (bool, error) {
	return (activity.Status == activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED &&
		task.Stamp == activity.LastAttempt.Get(ctx).GetStamp()), nil
}

func (h *scheduleToStartTimeoutTaskHandler) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.ScheduleToStartTimeoutTask,
) error {
	metricsHandler := activity.enrichedMetricsHandler(ctx, metrics.TimerActiveTaskActivityTimeoutScope)

	event := timeoutEvent{
		timeoutType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		retryState:     enumspb.RETRY_STATE_TIMEOUT,
		metricsHandler: metricsHandler,
	}

	return TransitionTimedOut.Apply(activity, ctx, event)
}

type scheduleToCloseTimeoutTaskHandler struct{ chasm.PureTaskHandlerBase }

func newScheduleToCloseTimeoutTaskHandler() *scheduleToCloseTimeoutTaskHandler {
	return &scheduleToCloseTimeoutTaskHandler{}
}

func (h *scheduleToCloseTimeoutTaskHandler) Validate(
	_ chasm.Context,
	activity *Activity,
	_ chasm.TaskInvocation,
	task *activitypb.ScheduleToCloseTimeoutTask,
) (bool, error) {
	if !TransitionTimedOut.Possible(activity) {
		return false, nil
	}
	// If schedule-to-close was disabled via an options update, discard this task.
	if activity.GetScheduleToCloseTimeout().AsDuration() <= 0 {
		return false, nil
	}
	// Stamp check: discard tasks from before the most recent ScheduleToCloseTimeoutTask was
	// scheduled (e.g. after a schedule-to-close extension or a disable+re-enable cycle).
	if task.GetStamp() != activity.GetScheduleToCloseStamp() {
		return false, nil
	}
	return true, nil
}

func (h *scheduleToCloseTimeoutTaskHandler) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.ScheduleToCloseTimeoutTask,
) error {
	metricsHandler := activity.enrichedMetricsHandler(ctx, metrics.TimerActiveTaskActivityTimeoutScope)
	retryState := enumspb.RETRY_STATE_TIMEOUT
	if activity.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED {
		retryState = enumspb.RETRY_STATE_CANCEL_REQUESTED
	}

	event := timeoutEvent{
		timeoutType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		retryState:     retryState,
		metricsHandler: metricsHandler,
	}

	return TransitionTimedOut.Apply(activity, ctx, event)
}

type startToCloseTimeoutTaskHandler struct{ chasm.PureTaskHandlerBase }

func newStartToCloseTimeoutTaskHandler() *startToCloseTimeoutTaskHandler {
	return &startToCloseTimeoutTaskHandler{}
}

func (h *startToCloseTimeoutTaskHandler) Validate(
	ctx chasm.Context,
	activity *Activity,
	_ chasm.TaskInvocation,
	task *activitypb.StartToCloseTimeoutTask,
) (bool, error) {
	valid := activity.hasAttemptInProgress() &&
		task.Stamp == activity.LastAttempt.Get(ctx).GetStamp()
	return valid, nil
}

// Execute executes a StartToCloseTimeoutTask. It fails the attempt, leading to retry or activity
// failure.
func (h *startToCloseTimeoutTaskHandler) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.StartToCloseTimeoutTask,
) error {
	retryState, err := activity.tryReschedule(
		ctx,
		activity.timeoutRetryable(enumspb.TIMEOUT_TYPE_START_TO_CLOSE),
		0,
		createStartToCloseTimeoutFailure(),
	)
	if err != nil {
		return err
	}

	metricsHandler := activity.enrichedMetricsHandler(ctx, metrics.TimerActiveTaskActivityTimeoutScope)

	if retryState == enumspb.RETRY_STATE_IN_PROGRESS {
		activity.emitOnAttemptTimedOutMetrics(metricsHandler, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)

		return nil
	}

	return TransitionTimedOut.Apply(activity, ctx, timeoutEvent{
		timeoutType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		retryState:     retryState,
		metricsHandler: metricsHandler,
	})
}

// HeartbeatTimeoutTask is a pure task that enforces heartbeat timeouts.
type heartbeatTimeoutTaskHandler struct{ chasm.PureTaskHandlerBase }

func newHeartbeatTimeoutTaskHandler() *heartbeatTimeoutTaskHandler {
	return &heartbeatTimeoutTaskHandler{}
}

// Validate validates a HeartbeatTimeoutTask.
func (h *heartbeatTimeoutTaskHandler) Validate(
	ctx chasm.Context,
	activity *Activity,
	taskAttrs chasm.TaskInvocation,
	task *activitypb.HeartbeatTimeoutTask,
) (bool, error) {
	// Let T = user-configured heartbeat timeout and let hb_i be the time of the ith user-submitted
	// heartbeat request. (hb_0 = 0 since we always start a timer task when an attempt starts).

	// There are two concurrent sequences of events:
	// 1. A worker is sending heartbeats at times hb_i.
	// 2. This task is being executed at (shortly after) times hb_i + T.

	// On the i-th execution of this function, we look back into the past and determine whether the
	// last heartbeat was received after hb_i. If so, we reject this timeout task. Otherwise, the
	// Execute function runs and we fail the attempt.
	if !activity.hasAttemptInProgress() {
		return false, nil
	}
	// Task attempt must still match current attempt.
	attempt := activity.LastAttempt.Get(ctx)
	if attempt.GetStamp() != task.Stamp {
		return false, nil
	}

	// Must not have been a heartbeat since this task was created
	hbTimeout := activity.GetHeartbeatTimeout().AsDuration() // T
	attemptStartTime := attempt.GetStartedTime().AsTime()
	lastHb, _ := activity.LastHeartbeat.TryGet(ctx) // could be nil, or from a previous attempt
	// No hbs in attempt so far is equivalent to hb having been sent at attempt start time.
	lastHbTime := util.MaxTime(lastHb.GetRecordedTime().AsTime(), attemptStartTime)
	thisTaskHbTime := taskAttrs.ScheduledTime.Add(-hbTimeout) // hb_i
	if lastHbTime.After(thisTaskHbTime) {
		// another heartbeat has invalidated this task's heartbeat
		return false, nil
	}
	return true, nil
}

// Execute executes a HeartbeatTimeoutTask. It fails the attempt, leading to retry or activity
// failure.
func (h *heartbeatTimeoutTaskHandler) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.HeartbeatTimeoutTask,
) error {
	retryState, err := activity.tryReschedule(
		ctx,
		activity.timeoutRetryable(enumspb.TIMEOUT_TYPE_HEARTBEAT),
		0,
		createHeartbeatTimeoutFailure(),
	)
	if err != nil {
		return err
	}

	metricsHandler := activity.enrichedMetricsHandler(ctx, metrics.TimerActiveTaskActivityTimeoutScope)

	if retryState == enumspb.RETRY_STATE_IN_PROGRESS {
		activity.emitOnAttemptTimedOutMetrics(metricsHandler, enumspb.TIMEOUT_TYPE_HEARTBEAT)
		return nil
	}

	return TransitionTimedOut.Apply(activity, ctx, timeoutEvent{
		timeoutType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		retryState:     retryState,
		metricsHandler: metricsHandler,
	})
}
