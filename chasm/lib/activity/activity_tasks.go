package activity

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/util"
	"go.uber.org/fx"
)

type activityDispatchTaskHandlerOptions struct {
	fx.In

	MatchingClient resource.MatchingClient
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
	_ chasm.TaskAttributes,
	task *activitypb.ActivityDispatchTask,
) (bool, error) {
	// TODO(saa-preview): make sure we handle resets when we support them, as they will reset the attempt count
	return (TransitionStarted.Possible(activity) &&
		task.Stamp == activity.LastAttempt.Get(ctx).GetStamp()), nil
}

func (h *activityDispatchTaskHandler) Execute(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *activitypb.ActivityDispatchTask,
) error {
	return h.pushToMatching(ctx, activityRef)
}

// Discard spills the task to matching instead of silently discarding it on standby clusters when the activity
// dispatch task has been pending past the discard delay.
func (h *activityDispatchTaskHandler) Discard(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	_ chasm.TaskAttributes,
	_ *activitypb.ActivityDispatchTask,
) error {
	return h.pushToMatching(ctx, activityRef)
}

func (h *activityDispatchTaskHandler) pushToMatching(
	ctx context.Context,
	activityRef chasm.ComponentRef,
) error {
	request, err := chasm.ReadComponent(
		ctx,
		activityRef,
		(*Activity).createAddActivityTaskRequest,
		activityRef.NamespaceID,
	)
	if err != nil {
		return err
	}

	_, err = h.opts.MatchingClient.AddActivityTask(ctx, request)

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
	_ chasm.TaskAttributes,
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
	metricsHandler, err := activity.enrichMetricsHandler(ctx, metrics.TimerActiveTaskActivityTimeoutScope)
	if err != nil {
		return err
	}

	event := timeoutEvent{
		timeoutType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START,
		metricsHandler: metricsHandler,
		fromStatus:     activity.GetStatus(),
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
	attrs chasm.TaskAttributes,
	_ *activitypb.ScheduleToCloseTimeoutTask,
) (bool, error) {
	if !TransitionTimedOut.Possible(activity) {
		return false, nil
	}
	// Discard stale tasks created before a schedule-to-close extension.
	// When the deadline is extended, a new task is added at the updated deadline; any earlier
	// task (from the old shorter deadline) must not time out the activity prematurely.
	if timeout := activity.GetScheduleToCloseTimeout().AsDuration(); timeout > 0 {
		currentDeadline := activity.GetScheduleTime().AsTime().Add(timeout)
		if attrs.ScheduledTime.Before(currentDeadline) {
			return false, nil
		}
	}
	return true, nil
}

func (h *scheduleToCloseTimeoutTaskHandler) Execute(
	ctx chasm.MutableContext,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.ScheduleToCloseTimeoutTask,
) error {
	metricsHandler, err := activity.enrichMetricsHandler(ctx, metrics.TimerActiveTaskActivityTimeoutScope)
	if err != nil {
		return err
	}
	event := timeoutEvent{
		timeoutType:    enumspb.TIMEOUT_TYPE_SCHEDULE_TO_CLOSE,
		metricsHandler: metricsHandler,
		fromStatus:     activity.GetStatus(),
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
	_ chasm.TaskAttributes,
	task *activitypb.StartToCloseTimeoutTask,
) (bool, error) {
	valid := (activity.Status == activitypb.ACTIVITY_EXECUTION_STATUS_STARTED &&
		task.Stamp == activity.LastAttempt.Get(ctx).GetStamp())
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
	rescheduled, err := activity.tryReschedule(ctx, 0, createStartToCloseTimeoutFailure())
	if err != nil {
		return err
	}

	metricsHandler, err := activity.enrichMetricsHandler(ctx, metrics.TimerActiveTaskActivityTimeoutScope)
	if err != nil {
		return err
	}

	if rescheduled {
		activity.emitOnAttemptTimedOutMetrics(ctx, metricsHandler, enumspb.TIMEOUT_TYPE_START_TO_CLOSE)

		return nil
	}

	return TransitionTimedOut.Apply(activity, ctx, timeoutEvent{
		timeoutType:    enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		metricsHandler: metricsHandler,
		fromStatus:     activity.GetStatus(),
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
	taskAttrs chasm.TaskAttributes,
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
	if activity.Status != activitypb.ACTIVITY_EXECUTION_STATUS_STARTED &&
		activity.Status != activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED {
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
	rescheduled, err := activity.tryReschedule(ctx, 0, createHeartbeatTimeoutFailure())
	if err != nil {
		return err
	}

	metricsHandler, err := activity.enrichMetricsHandler(ctx, metrics.TimerActiveTaskActivityTimeoutScope)
	if err != nil {
		return err
	}

	if rescheduled {
		activity.emitOnAttemptTimedOutMetrics(ctx, metricsHandler, enumspb.TIMEOUT_TYPE_HEARTBEAT)
		return nil
	}

	return TransitionTimedOut.Apply(activity, ctx, timeoutEvent{
		timeoutType:    enumspb.TIMEOUT_TYPE_HEARTBEAT,
		metricsHandler: metricsHandler,
		fromStatus:     activity.GetStatus(),
	})
}
