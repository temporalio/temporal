package activity

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	workerservicepb "go.temporal.io/api/nexusservices/workerservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/configs"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"
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
	_ chasm.TaskAttributes,
	_ *activitypb.ScheduleToCloseTimeoutTask,
) (bool, error) {
	return TransitionTimedOut.Possible(activity), nil
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
	valid := ((activity.Status == activitypb.ACTIVITY_EXECUTION_STATUS_STARTED ||
		activity.Status == activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED) &&
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

// cancelCommandDispatchTaskHandler dispatches a cancel command to the worker via the Nexus
// worker commands control queue. This is a best-effort mechanism — the activity will eventually
// time out if the worker doesn't respond.
type cancelCommandDispatchTaskHandler struct {
	chasm.SideEffectTaskHandlerBase[*activitypb.CancelCommandDispatchTask]
	opts cancelCommandDispatchTaskHandlerOptions
}

type cancelCommandDispatchTaskHandlerOptions struct {
	fx.In

	MatchingClient resource.MatchingClient
	Config         *configs.Config
	MetricsHandler metrics.Handler
	Logger         log.Logger
}

func newCancelCommandDispatchTaskHandler(opts cancelCommandDispatchTaskHandlerOptions) *cancelCommandDispatchTaskHandler {
	return &cancelCommandDispatchTaskHandler{opts: opts}
}

func (h *cancelCommandDispatchTaskHandler) Validate(
	_ chasm.Context,
	activity *Activity,
	_ chasm.TaskAttributes,
	_ *activitypb.CancelCommandDispatchTask,
) (bool, error) {
	// Valid if the activity is in a state where it has been requested to cancel or terminated
	// (meaning it was running on a worker when the cancel/terminate was issued).
	return activity.Status == activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED ||
		activity.Status == activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED, nil
}

const (
	cancelCommandDispatchTimeout = time.Second * 10 * debug.TimeoutMultiplier

	workerCommandsServiceName   = "temporal.api.nexusservices.workerservice.v1.WorkerService"
	workerCommandsOperationName = "ExecuteCommands"
)

func (h *cancelCommandDispatchTaskHandler) Execute(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	taskAttrs chasm.TaskAttributes,
	_ *activitypb.CancelCommandDispatchTask,
) error {
	if !h.opts.Config.EnableCancelActivityWorkerCommand(activityRef.NamespaceID) {
		return nil
	}

	// Read the activity to build the task token for the cancel command.
	taskToken, err := chasm.ReadComponent(
		ctx,
		activityRef,
		(*Activity).buildCancelCommandTaskToken,
		activityRef,
	)
	if err != nil {
		return err
	}

	command := &workerpb.WorkerCommand{
		Type: &workerpb.WorkerCommand_CancelActivity{
			CancelActivity: &workerpb.CancelActivityCommand{
				TaskToken: taskToken,
			},
		},
	}

	return h.dispatchToWorker(ctx, activityRef.NamespaceID, taskAttrs.Destination, []*workerpb.WorkerCommand{command})
}

func (h *cancelCommandDispatchTaskHandler) dispatchToWorker(
	ctx context.Context,
	namespaceID string,
	controlQueue string,
	commands []*workerpb.WorkerCommand,
) error {
	ctx, cancel := context.WithTimeout(ctx, cancelCommandDispatchTimeout)
	defer cancel()

	request := &workerservicepb.ExecuteCommandsRequest{
		Commands: commands,
	}
	requestData, err := proto.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to encode worker commands request: %w", err)
	}
	requestPayload := &commonpb.Payload{
		Metadata: map[string][]byte{
			"encoding": []byte("binary/protobuf"),
		},
		Data: requestData,
	}

	nexusRequest := &nexuspb.Request{
		Header: map[string]string{},
		Variant: &nexuspb.Request_StartOperation{
			StartOperation: &nexuspb.StartOperationRequest{
				Service:   workerCommandsServiceName,
				Operation: workerCommandsOperationName,
				Payload:   requestPayload,
			},
		},
	}

	resp, err := h.opts.MatchingClient.DispatchNexusTask(ctx, &matchingservice.DispatchNexusTaskRequest{
		NamespaceId: namespaceID,
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: controlQueue,
			Kind: enumspb.TASK_QUEUE_KIND_WORKER_COMMANDS,
		},
		Request: nexusRequest,
	})
	if err != nil {
		h.opts.Logger.Warn("Failed to dispatch cancel command",
			tag.NewStringTag("control_queue", controlQueue),
			tag.Error(err))
		metrics.WorkerCommandsSent.With(h.opts.MetricsHandler).Record(1, metrics.OutcomeTag("rpc_error"))
		return err
	}

	nexusErr := commonnexus.DispatchResponseToError(resp)
	if nexusErr == nil {
		metrics.WorkerCommandsSent.With(h.opts.MetricsHandler).Record(1, metrics.OutcomeTag("success"))
		return nil
	}

	return h.handleDispatchError(nexusErr, controlQueue)
}

func (h *cancelCommandDispatchTaskHandler) handleDispatchError(nexusErr error, controlQueue string) error {
	var handlerErr *nexus.HandlerError
	if errors.As(nexusErr, &handlerErr) {
		// Handler-level error (transport, timeout, internal).
		if handlerErr.Type == nexus.HandlerErrorTypeUpstreamTimeout {
			h.opts.Logger.Warn("No worker polling control queue",
				tag.NewStringTag("control_queue", controlQueue))
			metrics.WorkerCommandsSent.With(h.opts.MetricsHandler).Record(1, metrics.OutcomeTag("no_poller"))
			return nexusErr
		}

		if !handlerErr.Retryable() {
			h.opts.Logger.Error("Cancel command non-retryable handler error",
				tag.NewStringTag("control_queue", controlQueue),
				tag.Error(nexusErr))
			metrics.WorkerCommandsSent.With(h.opts.MetricsHandler).Record(1, metrics.OutcomeTag("non_retryable_error"))
			return nil
		}

		h.opts.Logger.Warn("Cancel command transport failure",
			tag.NewStringTag("control_queue", controlQueue),
			tag.Error(nexusErr))
		metrics.WorkerCommandsSent.With(h.opts.MetricsHandler).Record(1, metrics.OutcomeTag("transport_error"))
		return nexusErr
	}

	// Worker-returned failure (ApplicationError, CanceledError, etc.). The worker received
	// and processed the request but returned an error. Permanent — the worker contract
	// requires success for all defined commands, so this indicates a bug or version
	// incompatibility. Retrying won't help.
	h.opts.Logger.Error("Worker returned failure for cancel command",
		tag.NewStringTag("control_queue", controlQueue),
		tag.Error(nexusErr))
	metrics.WorkerCommandsSent.With(h.opts.MetricsHandler).Record(1, metrics.OutcomeTag("worker_error"))
	return nil
}
