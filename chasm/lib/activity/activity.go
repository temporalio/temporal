package activity

import (
	"errors"
	"fmt"
	"slices"
	"time"

	apiactivitypb "go.temporal.io/api/activity/v1" //nolint:importas
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payload"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/tqid"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// WorkflowTypeTag is a required workflow tag for standalone activities to ensure consistent
	// metric labeling between workflows and activities.
	WorkflowTypeTag = "__temporal_standalone_activity__"
)

var (
	TypeSearchAttribute   = chasm.NewSearchAttributeKeyword("ActivityType", chasm.SearchAttributeFieldKeyword01)
	StatusSearchAttribute = chasm.NewSearchAttributeKeyword("ExecutionStatus", chasm.SearchAttributeFieldLowCardinalityKeyword01)
)

var _ chasm.VisibilitySearchAttributesProvider = (*Activity)(nil)

type ActivityStore interface {
	// RecordCompleted applies the provided function to record activity completion
	RecordCompleted(ctx chasm.MutableContext, applyFn func(ctx chasm.MutableContext) error) error
}

// Activity component represents an activity execution persistence object and can be either standalone activity or one
// embedded within a workflow.
type Activity struct {
	chasm.UnimplementedComponent

	*activitypb.ActivityState

	Visibility    chasm.Field[*chasm.Visibility]
	LastAttempt   chasm.Field[*activitypb.ActivityAttemptState]
	LastHeartbeat chasm.Field[*activitypb.ActivityHeartbeatState]
	// Standalone only
	RequestData chasm.Field[*activitypb.ActivityRequestData]
	Outcome     chasm.Field[*activitypb.ActivityOutcome]
	// Pointer to an implementation of the "store". For a workflow activity this would be a parent
	// pointer back to the workflow. For a standalone activity this is nil (Activity itself
	// implements the ActivityStore interface).
	// TODO(saa-preview): figure out better naming.
	Store chasm.ParentPtr[ActivityStore]
}

// WithToken wraps a request with its deserialized task token.
type WithToken[R any] struct {
	Token   *tokenspb.Task
	Request R
}

// MetricsHandlerBuilderParams contains parameters for building/enriching  a metrics handler for activity operations
type MetricsHandlerBuilderParams struct {
	Handler                     metrics.Handler
	NamespaceName               string
	BreakdownMetricsByTaskQueue dynamicconfig.TypedPropertyFnWithTaskQueueFilter[bool]
}

// RespondCompletedEvent wraps the RespondActivityTaskCompletedRequest with context-specific data.
type RespondCompletedEvent struct {
	Request                     *historyservice.RespondActivityTaskCompletedRequest
	Token                       *tokenspb.Task
	MetricsHandlerBuilderParams MetricsHandlerBuilderParams
}

// RespondFailedEvent wraps the RespondActivityTaskFailedRequest with context-specific data.
type RespondFailedEvent struct {
	Request                     *historyservice.RespondActivityTaskFailedRequest
	Token                       *tokenspb.Task
	MetricsHandlerBuilderParams MetricsHandlerBuilderParams
}

// RespondCancelledEvent wraps the RespondActivityTaskCanceledRequest with context-specific data.
type RespondCancelledEvent struct {
	Request                     *historyservice.RespondActivityTaskCanceledRequest
	Token                       *tokenspb.Task
	MetricsHandlerBuilderParams MetricsHandlerBuilderParams
}

// requestCancelEvent wraps the RequestCancelActivityExecutionRequest with context-specific data.
type requestCancelEvent struct {
	request                     *activitypb.RequestCancelActivityExecutionRequest
	MetricsHandlerBuilderParams MetricsHandlerBuilderParams
}

// terminateEvent wraps the TerminateActivityExecutionRequest with context-specific data.
type terminateEvent struct {
	request                     *activitypb.TerminateActivityExecutionRequest
	MetricsHandlerBuilderParams MetricsHandlerBuilderParams
}

// LifecycleState implements the chasm.Component interface.
func (a *Activity) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch a.Status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED:
		return chasm.LifecycleStateCompleted
	case activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

// Terminate implements the chasm.RootComponent interface.
func (a *Activity) Terminate(
	_ chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	// TODO: Implement terminate logic.
	return chasm.TerminateComponentResponse{}, nil
}

// NewStandaloneActivity creates a new activity component and adds associated tasks to start execution.
func NewStandaloneActivity(
	ctx chasm.MutableContext,
	request *workflowservice.StartActivityExecutionRequest,
) (*Activity, error) {
	visibility := chasm.NewVisibilityWithData(
		ctx,
		request.GetSearchAttributes().GetIndexedFields(),
		nil,
	)

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:           request.ActivityType,
			TaskQueue:              request.GetTaskQueue(),
			ScheduleToCloseTimeout: request.GetScheduleToCloseTimeout(),
			ScheduleToStartTimeout: request.GetScheduleToStartTimeout(),
			StartToCloseTimeout:    request.GetStartToCloseTimeout(),
			HeartbeatTimeout:       request.GetHeartbeatTimeout(),
			RetryPolicy:            request.GetRetryPolicy(),
			Priority:               request.Priority,
		},
		LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{}),
		RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
			Input:        request.Input,
			Header:       request.Header,
			UserMetadata: request.UserMetadata,
		}),
		Outcome:    chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
		Visibility: chasm.NewComponentField(ctx, visibility),
	}

	activity.ScheduleTime = timestamppb.New(ctx.Now(activity))

	return activity, nil
}

func NewEmbeddedActivity(
	ctx chasm.MutableContext,
	state *activitypb.ActivityState,
	parent ActivityStore,
) {
}

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

// HandleStarted updates the activity on recording activity task started and populates the response.
func (a *Activity) HandleStarted(ctx chasm.MutableContext, request *historyservice.RecordActivityTaskStartedRequest) (
	*historyservice.RecordActivityTaskStartedResponse, error,
) {
	lastAttempt := a.LastAttempt.Get(ctx)
	// If already started, return existing response if request ID matches to make retry idempotent, else error.
	if a.StateMachineState() == activitypb.ACTIVITY_EXECUTION_STATUS_STARTED && request.GetRequestId() == lastAttempt.GetStartRequestId() {
		return a.GenerateRecordActivityTaskStartedResponse(ctx, request.GetPollRequest().GetNamespace())
	}
	if lastAttempt.GetStamp() != request.GetStamp() {
		return nil, serviceerrors.NewObsoleteMatchingTask("activity attempt stamp mismatch")
	}
	if err := TransitionStarted.Apply(a, ctx, request); err != nil {
		if errors.Is(err, chasm.ErrInvalidTransition) {
			return nil, serviceerrors.NewObsoleteMatchingTask(err.Error())
		}
		return nil, err
	}
	return a.GenerateRecordActivityTaskStartedResponse(ctx, request.GetPollRequest().GetNamespace())
}

// GenerateRecordActivityTaskStartedResponse generates the response for HandleStarted.
func (a *Activity) GenerateRecordActivityTaskStartedResponse(
	ctx chasm.Context,
	namespace string,
) (*historyservice.RecordActivityTaskStartedResponse, error) {
	key := ctx.ExecutionKey()
	lastHeartbeat, _ := a.LastHeartbeat.TryGet(ctx)
	requestData := a.RequestData.Get(ctx)
	attempt := a.LastAttempt.Get(ctx)

	return &historyservice.RecordActivityTaskStartedResponse{
		StartedTime:                 attempt.GetStartedTime(),
		Attempt:                     attempt.GetCount(),
		Priority:                    a.GetPriority(),
		RetryPolicy:                 a.GetRetryPolicy(),
		ActivityRunId:               key.RunID,
		WorkflowNamespace:           namespace,
		HeartbeatDetails:            lastHeartbeat.GetDetails(),
		CurrentAttemptScheduledTime: a.attemptScheduleTime(attempt),
		ScheduledEvent: &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
			EventTime: a.GetScheduleTime(),
			Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
				ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					ActivityId:             key.BusinessID,
					ActivityType:           a.GetActivityType(),
					Input:                  requestData.GetInput(),
					Header:                 requestData.GetHeader(),
					TaskQueue:              a.GetTaskQueue(),
					ScheduleToCloseTimeout: a.GetScheduleToCloseTimeout(),
					ScheduleToStartTimeout: a.GetScheduleToStartTimeout(),
					StartToCloseTimeout:    a.GetStartToCloseTimeout(),
					HeartbeatTimeout:       a.GetHeartbeatTimeout(),
				},
			},
		},
	}, nil
}

// attemptScheduleTime returns when the given attempt was scheduled to run:
// the activity's original schedule time for the first attempt, or
// calculated from attemptScheduleTimeForRetry on retries.
func (a *Activity) attemptScheduleTime(attempt *activitypb.ActivityAttemptState) *timestamppb.Timestamp {
	if attempt.GetCount() == 1 {
		return a.GetScheduleTime()
	}
	return attemptScheduleTimeForRetry(attempt)
}

// attemptScheduleTimeForRetry computes the time a retried attempt is scheduled to start,
// as complete_time + retry_interval. Returns nil if either field is missing or zero.
func attemptScheduleTimeForRetry(attempt *activitypb.ActivityAttemptState) *timestamppb.Timestamp {
	retryInterval := attempt.GetCurrentRetryInterval()
	completeTime := attempt.GetCompleteTime()
	if retryInterval != nil && retryInterval.AsDuration() > 0 && completeTime != nil {
		return timestamppb.New(completeTime.AsTime().Add(retryInterval.AsDuration()))
	}
	return nil
}

// RecordCompleted applies the provided function to record activity completion.
func (a *Activity) RecordCompleted(ctx chasm.MutableContext, applyFn func(ctx chasm.MutableContext) error) error {
	return applyFn(ctx)
}

// HandleCompleted updates the activity on activity completion.
func (a *Activity) HandleCompleted(
	ctx chasm.MutableContext,
	event RespondCompletedEvent,
) (*historyservice.RespondActivityTaskCompletedResponse, error) {
	if err := a.validateActivityTaskToken(ctx, event.Token, event.Request.GetNamespaceId()); err != nil {
		return nil, err
	}

	metricsHandler := enrichMetricsHandler(
		a,
		event.MetricsHandlerBuilderParams.Handler,
		event.MetricsHandlerBuilderParams.NamespaceName,
		metrics.HistoryRespondActivityTaskCompletedScope,
		event.MetricsHandlerBuilderParams.BreakdownMetricsByTaskQueue)

	if err := TransitionCompleted.Apply(a, ctx, completeEvent{
		req:            event.Request,
		metricsHandler: metricsHandler,
	}); err != nil {
		return nil, err
	}

	return &historyservice.RespondActivityTaskCompletedResponse{}, nil
}

// HandleFailed updates the activity on activity failure. if the activity is retryable, it will be rescheduled
// for retry instead.
func (a *Activity) HandleFailed(
	ctx chasm.MutableContext,
	event RespondFailedEvent,
) (*historyservice.RespondActivityTaskFailedResponse, error) {
	if err := a.validateActivityTaskToken(ctx, event.Token, event.Request.GetNamespaceId()); err != nil {
		return nil, err
	}

	metricsHandler := enrichMetricsHandler(
		a,
		event.MetricsHandlerBuilderParams.Handler,
		event.MetricsHandlerBuilderParams.NamespaceName,
		metrics.HistoryRespondActivityTaskFailedScope,
		event.MetricsHandlerBuilderParams.BreakdownMetricsByTaskQueue)

	failure := event.Request.GetFailedRequest().GetFailure()

	appFailure := failure.GetApplicationFailureInfo()
	isRetryable := appFailure != nil &&
		!appFailure.GetNonRetryable() &&
		!slices.Contains(a.GetRetryPolicy().GetNonRetryableErrorTypes(), appFailure.GetType())

	if isRetryable {
		rescheduled, err := a.tryReschedule(ctx, appFailure.GetNextRetryDelay().AsDuration(), failure)
		if err != nil {
			return nil, err
		}
		if rescheduled {
			a.emitOnAttemptFailedMetrics(ctx, metricsHandler)

			return &historyservice.RespondActivityTaskFailedResponse{}, nil
		}
	}

	if err := TransitionFailed.Apply(a, ctx, failedEvent{
		req:            event.Request,
		metricsHandler: metricsHandler,
	}); err != nil {
		return nil, err
	}

	return &historyservice.RespondActivityTaskFailedResponse{}, nil
}

// HandleCanceled updates the activity on activity canceled.
func (a *Activity) HandleCanceled(
	ctx chasm.MutableContext,
	event RespondCancelledEvent,
) (*historyservice.RespondActivityTaskCanceledResponse, error) {
	if err := a.validateActivityTaskToken(ctx, event.Token, event.Request.GetNamespaceId()); err != nil {
		return nil, err
	}

	metricsHandler := enrichMetricsHandler(
		a,
		event.MetricsHandlerBuilderParams.Handler,
		event.MetricsHandlerBuilderParams.NamespaceName,
		metrics.HistoryRespondActivityTaskCanceledScope,
		event.MetricsHandlerBuilderParams.BreakdownMetricsByTaskQueue)

	if err := TransitionCanceled.Apply(a, ctx, cancelEvent{
		details:    event.Request.GetCancelRequest().GetDetails(),
		handler:    metricsHandler,
		fromStatus: a.GetStatus(),
	}); err != nil {
		return nil, err
	}

	return &historyservice.RespondActivityTaskCanceledResponse{}, nil
}

func (a *Activity) handleTerminated(ctx chasm.MutableContext, req terminateEvent) (
	*activitypb.TerminateActivityExecutionResponse, error,
) {
	frontendReq := req.request.GetFrontendRequest()

	// If already in terminated state, fail if request ID is different, else no-op
	if a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED {
		newReqID := frontendReq.GetRequestId()
		existingReqID := a.GetTerminateState().GetRequestId()

		if existingReqID != newReqID {
			return nil, serviceerror.NewFailedPrecondition(
				fmt.Sprintf("already terminated with request ID %s", existingReqID))
		}

		return &activitypb.TerminateActivityExecutionResponse{}, nil
	}

	if err := TransitionTerminated.Apply(a, ctx, req); err != nil {
		return nil, err
	}

	return &activitypb.TerminateActivityExecutionResponse{}, nil
}

// getOrCreateLastHeartbeat retrieves the last heartbeat state, initializing it if not present. The heartbeat is lazily created
// to avoid unnecessary writes when heartbeats are not used.
func (a *Activity) getOrCreateLastHeartbeat(ctx chasm.MutableContext) *activitypb.ActivityHeartbeatState {
	heartbeat, ok := a.LastHeartbeat.TryGet(ctx)
	if !ok {
		heartbeat = &activitypb.ActivityHeartbeatState{}
		a.LastHeartbeat = chasm.NewDataField(ctx, heartbeat)
	}
	return heartbeat
}

func (a *Activity) handleCancellationRequested(ctx chasm.MutableContext, event requestCancelEvent) (
	*activitypb.RequestCancelActivityExecutionResponse, error,
) {
	req := event.request.GetFrontendRequest()
	newReqID := req.GetRequestId()
	existingReqID := a.GetCancelState().GetRequestId()

	// If already in cancel requested state, fail if request ID is different, else no-op
	if a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED {
		if existingReqID != newReqID {
			return nil, serviceerror.NewFailedPrecondition(
				fmt.Sprintf("cancellation already requested with request ID %s", existingReqID))
		}

		return &activitypb.RequestCancelActivityExecutionResponse{}, nil
	}

	// If in scheduled state, cancel immediately right after marking cancel requested
	isCancelImmediately := a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED

	if err := TransitionCancelRequested.Apply(a, ctx, req); err != nil {
		return nil, err
	}

	if isCancelImmediately {
		details := &commonpb.Payloads{
			Payloads: []*commonpb.Payload{
				payload.EncodeString(req.GetReason()),
			},
		}

		metricsHandler := enrichMetricsHandler(
			a,
			event.MetricsHandlerBuilderParams.Handler,
			event.MetricsHandlerBuilderParams.NamespaceName,
			metrics.HistoryRespondActivityTaskCanceledScope,
			event.MetricsHandlerBuilderParams.BreakdownMetricsByTaskQueue)

		err := TransitionCanceled.Apply(a, ctx, cancelEvent{
			details:    details,
			handler:    metricsHandler,
			fromStatus: activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED, // if we're here the original status was scheduled
		})
		if err != nil {
			return nil, err
		}
	}

	return &activitypb.RequestCancelActivityExecutionResponse{}, nil
}

// recordScheduleToStartOrCloseTimeoutFailure records schedule-to-start or schedule-to-close timeouts. Such timeouts are not retried so we
// set the outcome failure directly and leave the attempt failure as is.
func (a *Activity) recordScheduleToStartOrCloseTimeoutFailure(ctx chasm.MutableContext, timeoutType enumspb.TimeoutType) error {
	outcome := a.Outcome.Get(ctx)

	failure := &failurepb.Failure{
		Message: fmt.Sprintf(common.FailureReasonActivityTimeout, timeoutType.String()),
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: timeoutType,
			},
		},
	}

	outcome.Variant = &activitypb.ActivityOutcome_Failed_{
		Failed: &activitypb.ActivityOutcome_Failed{
			Failure: failure,
		},
	}

	return nil
}

// recordFailedAttempt records any failures resulting from a tried attempt, including worker application failures and
// start-to-close timeouts. Since the calls come from retried attempts we update the attempt failure info but leave
// the outcome failure empty to avoid duplication.
func (a *Activity) recordFailedAttempt(
	ctx chasm.MutableContext,
	retryInterval time.Duration,
	failure *failurepb.Failure,
	currentTime time.Time,
	noRetriesLeft bool,
) error {
	attempt := a.LastAttempt.Get(ctx)

	attempt.LastFailureDetails = &activitypb.ActivityAttemptState_LastFailureDetails{
		Failure: failure,
		Time:    timestamppb.New(currentTime),
	}
	attempt.CompleteTime = timestamppb.New(currentTime)

	if noRetriesLeft {
		attempt.CurrentRetryInterval = nil
	} else {
		attempt.CurrentRetryInterval = durationpb.New(retryInterval)
	}
	return nil
}

// tryReschedule attempts to reschedule the activity for retry. Returns true if rescheduled, false
// if retry is not possible.
func (a *Activity) tryReschedule(
	ctx chasm.MutableContext,
	overridingRetryInterval time.Duration,
	failure *failurepb.Failure,
) (bool, error) {
	shouldRetry, retryInterval := a.shouldRetry(ctx, overridingRetryInterval)
	if !shouldRetry {
		return false, nil
	}
	return true, TransitionRescheduled.Apply(a, ctx, rescheduleEvent{
		retryInterval: retryInterval,
		failure:       failure,
	})
}

func (a *Activity) shouldRetry(ctx chasm.Context, overridingRetryInterval time.Duration) (bool, time.Duration) {
	if !TransitionRescheduled.Possible(a) {
		return false, 0
	}
	attempt := a.LastAttempt.Get(ctx)
	retryPolicy := a.RetryPolicy

	enoughAttempts := retryPolicy.GetMaximumAttempts() == 0 || attempt.GetCount() < retryPolicy.GetMaximumAttempts()
	enoughTime, retryInterval := a.hasEnoughTimeForRetry(ctx, overridingRetryInterval)
	return enoughAttempts && enoughTime, retryInterval
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

	deadline := a.ScheduleTime.AsTime().Add(scheduleToClose)
	return ctx.Now(a).Add(retryInterval).Before(deadline), retryInterval
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

// RecordHeartbeat records a heartbeat for the activity.
func (a *Activity) RecordHeartbeat(
	ctx chasm.MutableContext,
	input WithToken[*historyservice.RecordActivityTaskHeartbeatRequest],
) (*historyservice.RecordActivityTaskHeartbeatResponse, error) {
	err := a.validateActivityTaskToken(ctx, input.Token, input.Request.GetNamespaceId())
	if err != nil {
		return nil, err
	}
	a.LastHeartbeat = chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{
		RecordedTime: timestamppb.New(ctx.Now(a)),
		Details:      input.Request.GetHeartbeatRequest().GetDetails(),
	})
	if heartbeatTimeout := a.GetHeartbeatTimeout().AsDuration(); heartbeatTimeout > 0 {
		ctx.AddTask(
			a,
			chasm.TaskAttributes{
				ScheduledTime: ctx.Now(a).Add(heartbeatTimeout),
			},
			&activitypb.HeartbeatTimeoutTask{
				Stamp: a.LastAttempt.Get(ctx).GetStamp(),
			},
		)
	}
	return &historyservice.RecordActivityTaskHeartbeatResponse{
		CancelRequested: a.Status == activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED,
		// TODO(saa-preview): ActivityPaused, ActivityReset
	}, nil
}

// InternalStatusToAPIStatus converts internal activity execution status to API status.
func InternalStatusToAPIStatus(status activitypb.ActivityExecutionStatus) enumspb.ActivityExecutionStatus {
	switch status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED,
		activitypb.ACTIVITY_EXECUTION_STATUS_STARTED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED
	case activitypb.ACTIVITY_EXECUTION_STATUS_FAILED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_FAILED
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED
	case activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED
	case activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
		return enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
	case activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED:
		return enumspb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED
	default:
		panic(fmt.Sprintf("unknown activity execution status: %v", status)) //nolint:forbidigo
	}
}

func internalStatusToRunState(status activitypb.ActivityExecutionStatus) enumspb.PendingActivityState {
	switch status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED:
		return enumspb.PENDING_ACTIVITY_STATE_SCHEDULED
	case activitypb.ACTIVITY_EXECUTION_STATUS_STARTED:
		return enumspb.PENDING_ACTIVITY_STATE_STARTED
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED:
		return enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT,
		activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED:
		return enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	default:
		panic(fmt.Sprintf("unknown activity execution status: %v", status)) //nolint:forbidigo
	}
}

func (a *Activity) buildActivityExecutionInfo(ctx chasm.Context) *apiactivitypb.ActivityExecutionInfo {
	// TODO(saa-preview): support pause states
	status := InternalStatusToAPIStatus(a.GetStatus())
	runState := internalStatusToRunState(a.GetStatus())

	requestData := a.RequestData.Get(ctx)
	attempt := a.LastAttempt.Get(ctx)
	heartbeat, _ := a.LastHeartbeat.TryGet(ctx)
	key := ctx.ExecutionKey()

	var closeTime *timestamppb.Timestamp
	var executionDuration *durationpb.Duration
	if a.LifecycleState(ctx) != chasm.LifecycleStateRunning {
		executionDuration = durationpb.New(ctx.ExecutionCloseTime().Sub(a.GetScheduleTime().AsTime()))
		closeTime = timestamppb.New(ctx.ExecutionCloseTime())
	}

	var expirationTime *timestamppb.Timestamp
	if timeout := a.GetScheduleToCloseTimeout().AsDuration(); timeout > 0 {
		expirationTime = timestamppb.New(a.GetScheduleTime().AsTime().Add(timeout))
	}

	sa := &commonpb.SearchAttributes{
		IndexedFields: a.Visibility.Get(ctx).CustomSearchAttributes(ctx),
	}

	info := &apiactivitypb.ActivityExecutionInfo{
		ActivityId:              key.BusinessID,
		ActivityType:            a.GetActivityType(),
		Attempt:                 attempt.GetCount(),
		CanceledReason:          a.CancelState.GetReason(),
		CloseTime:               closeTime,
		CurrentRetryInterval:    attempt.GetCurrentRetryInterval(),
		ExecutionDuration:       executionDuration,
		ExpirationTime:          expirationTime,
		Header:                  requestData.GetHeader(),
		HeartbeatDetails:        heartbeat.GetDetails(),
		HeartbeatTimeout:        a.GetHeartbeatTimeout(),
		LastAttemptCompleteTime: attempt.GetCompleteTime(),
		LastFailure:             attempt.GetLastFailureDetails().GetFailure(),
		LastHeartbeatTime:       heartbeat.GetRecordedTime(),
		LastStartedTime:         attempt.GetStartedTime(),
		LastWorkerIdentity:      attempt.GetLastWorkerIdentity(),
		NextAttemptScheduleTime: attemptScheduleTimeForRetry(attempt),
		Priority:                a.GetPriority(),
		RetryPolicy:             a.GetRetryPolicy(),
		RunId:                   key.RunID,
		RunState:                runState,
		ScheduleTime:            a.GetScheduleTime(),
		ScheduleToCloseTimeout:  a.GetScheduleToCloseTimeout(),
		ScheduleToStartTimeout:  a.GetScheduleToStartTimeout(),
		StartToCloseTimeout:     a.GetStartToCloseTimeout(),
		StateTransitionCount:    ctx.StateTransitionCount(),
		// TODO(saa-preview): StateSizeBytes?
		SearchAttributes: sa,
		Status:           status,
		TaskQueue:        a.GetTaskQueue().GetName(),
		UserMetadata:     requestData.GetUserMetadata(),
	}

	return info
}

func (a *Activity) buildDescribeActivityExecutionResponse(
	ctx chasm.Context,
	req *activitypb.DescribeActivityExecutionRequest,
) (*activitypb.DescribeActivityExecutionResponse, error) {
	request := req.GetFrontendRequest()

	token, err := ctx.Ref(a)
	if err != nil {
		return nil, err
	}

	info := a.buildActivityExecutionInfo(ctx)

	var input *commonpb.Payloads
	if request.GetIncludeInput() {
		input = a.RequestData.Get(ctx).GetInput()
	}

	response := &workflowservice.DescribeActivityExecutionResponse{
		Info:          info,
		RunId:         ctx.ExecutionKey().RunID,
		Input:         input,
		LongPollToken: token,
	}

	if request.GetIncludeOutcome() {
		response.Outcome = a.outcome(ctx)
	}

	return &activitypb.DescribeActivityExecutionResponse{
		FrontendResponse: response,
	}, nil
}

func (a *Activity) buildPollActivityExecutionResponse(
	ctx chasm.Context,
) *activitypb.PollActivityExecutionResponse {
	return &activitypb.PollActivityExecutionResponse{
		FrontendResponse: &workflowservice.PollActivityExecutionResponse{
			RunId:   ctx.ExecutionKey().RunID,
			Outcome: a.outcome(ctx),
		},
	}
}

// outcome retrieves the activity outcome (result or failure) if the activity has completed.
// Returns nil if the activity has not completed.
func (a *Activity) outcome(ctx chasm.Context) *apiactivitypb.ActivityExecutionOutcome {
	if !a.LifecycleState(ctx).IsClosed() {
		return nil
	}
	activityOutcome := a.Outcome.Get(ctx)
	if successful := activityOutcome.GetSuccessful(); successful != nil {
		return &apiactivitypb.ActivityExecutionOutcome{
			Value: &apiactivitypb.ActivityExecutionOutcome_Result{Result: successful.GetOutput()},
		}
	}
	if failure := activityOutcome.GetFailed().GetFailure(); failure != nil {
		return &apiactivitypb.ActivityExecutionOutcome{
			Value: &apiactivitypb.ActivityExecutionOutcome_Failure{Failure: failure},
		}
	}
	if details := a.LastAttempt.Get(ctx).GetLastFailureDetails(); details != nil {
		return &apiactivitypb.ActivityExecutionOutcome{
			Value: &apiactivitypb.ActivityExecutionOutcome_Failure{Failure: details.GetFailure()},
		}
	}
	return nil
}

// StoreOrSelf returns the store for the activity. If the store is not set as a field (e.g.
// standalone activities), it returns the activity itself.
func (a *Activity) StoreOrSelf(ctx chasm.Context) ActivityStore {
	store, ok := a.Store.TryGet(ctx)
	if ok {
		return store
	}
	return a
}

// validateActivityTaskToken validates a task token against the current activity state.
func (a *Activity) validateActivityTaskToken(
	ctx chasm.Context,
	token *tokenspb.Task,
	requestNamespaceID string,
) error {
	if a.Status != activitypb.ACTIVITY_EXECUTION_STATUS_STARTED &&
		a.Status != activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED {
		return serviceerror.NewNotFound("activity task not found")
	}
	if token.Attempt != a.LastAttempt.Get(ctx).GetCount() {
		return serviceerror.NewNotFound("activity task not found")
	}

	ref, err := chasm.DeserializeComponentRef(token.GetComponentRef())
	if err != nil {
		return serviceerror.NewInvalidArgument("malformed token")
	}

	// Validate that the request namespace matches the token's namespace.
	// This prevents cross-namespace token reuse attacks where an attacker could use a valid token from namespace B to
	// complete an activity in namespace A.
	if requestNamespaceID != ref.NamespaceID {
		return serviceerror.NewInvalidArgument("token does not match namespace")
	}

	return nil
}

func enrichMetricsHandler(
	a *Activity,
	handler metrics.Handler,
	namespaceName string,
	operationTag string,
	breakdownMetricsByTaskQueue dynamicconfig.TypedPropertyFnWithTaskQueueFilter[bool],
) metrics.Handler {
	taskQueueFamily := a.GetTaskQueue().GetName()
	return metrics.GetPerTaskQueueFamilyScope(
		handler,
		namespaceName,
		tqid.UnsafeTaskQueueFamily(namespaceName, taskQueueFamily),
		breakdownMetricsByTaskQueue(namespaceName, taskQueueFamily, enumspb.TASK_QUEUE_TYPE_ACTIVITY),
		metrics.OperationTag(operationTag),
		metrics.ActivityTypeTag(a.GetActivityType().GetName()),
		metrics.VersioningBehaviorTag(enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED),
		metrics.WorkflowTypeTag(WorkflowTypeTag),
	)
}

func (a *Activity) emitOnAttemptTimedOutMetrics(ctx chasm.Context, handler metrics.Handler, timeoutType enumspb.TimeoutType) {
	attempt := a.LastAttempt.Get(ctx)
	startedTime := attempt.GetStartedTime().AsTime()

	latency := time.Since(startedTime)
	metrics.ActivityStartToCloseLatency.With(handler).Record(latency)

	timeoutTag := metrics.StringTag("timeout_type", timeoutType.String())
	metrics.ActivityTaskTimeout.With(handler).Record(1, timeoutTag)
}

func (a *Activity) emitOnAttemptFailedMetrics(ctx chasm.Context, handler metrics.Handler) {
	attempt := a.LastAttempt.Get(ctx)
	startedTime := attempt.GetStartedTime().AsTime()

	latency := time.Since(startedTime)
	metrics.ActivityStartToCloseLatency.With(handler).Record(latency)

	metrics.ActivityTaskFail.With(handler).Record(1)
}

func (a *Activity) emitOnCompletedMetrics(ctx chasm.Context, handler metrics.Handler) {
	attempt := a.LastAttempt.Get(ctx)
	startedTime := attempt.GetStartedTime().AsTime()

	startToCloseLatency := time.Since(startedTime)
	metrics.ActivityStartToCloseLatency.With(handler).Record(startToCloseLatency)

	scheduleToCloseLatency := time.Since(a.GetScheduleTime().AsTime())
	metrics.ActivityScheduleToCloseLatency.With(handler).Record(scheduleToCloseLatency)

	metrics.ActivitySuccess.With(handler).Record(1)
}

func (a *Activity) emitOnFailedMetrics(ctx chasm.Context, handler metrics.Handler) {
	attempt := a.LastAttempt.Get(ctx)
	startedTime := attempt.GetStartedTime().AsTime()

	startToCloseLatency := time.Since(startedTime)
	metrics.ActivityStartToCloseLatency.With(handler).Record(startToCloseLatency)

	scheduleToCloseLatency := time.Since(a.GetScheduleTime().AsTime())
	metrics.ActivityScheduleToCloseLatency.With(handler).Record(scheduleToCloseLatency)

	metrics.ActivityTaskFail.With(handler).Record(1)
	metrics.ActivityFail.With(handler).Record(1)
}

func (a *Activity) emitOnCanceledMetrics(
	ctx chasm.Context,
	handler metrics.Handler,
	fromStatus activitypb.ActivityExecutionStatus,
) {
	// Only record start-to-close latency if a current attempt was running. If it in scheduled status, it means the current attempt never started.
	if fromStatus != activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED {
		startedTime := a.LastAttempt.Get(ctx).GetStartedTime().AsTime()
		startToCloseLatency := time.Since(startedTime)
		metrics.ActivityStartToCloseLatency.With(handler).Record(startToCloseLatency)
	}

	scheduleToCloseLatency := time.Since(a.GetScheduleTime().AsTime())
	metrics.ActivityScheduleToCloseLatency.With(handler).Record(scheduleToCloseLatency)

	metrics.ActivityCancel.With(handler).Record(1)
}

func (a *Activity) emitOnTimedOutMetrics(
	ctx chasm.Context,
	handler metrics.Handler,
	timeoutType enumspb.TimeoutType,
	fromStatus activitypb.ActivityExecutionStatus,
) {
	// Only record start-to-close latency if a current attempt was running. If it in scheduled status, it means the current attempt never started.
	if fromStatus != activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED {
		startedTime := a.LastAttempt.Get(ctx).GetStartedTime().AsTime()
		startToCloseLatency := time.Since(startedTime)
		metrics.ActivityStartToCloseLatency.With(handler).Record(startToCloseLatency)
	}

	scheduleToCloseLatency := time.Since(a.GetScheduleTime().AsTime())
	metrics.ActivityScheduleToCloseLatency.With(handler).Record(scheduleToCloseLatency)

	timeoutTag := metrics.StringTag("timeout_type", timeoutType.String())
	metrics.ActivityTaskTimeout.With(handler).Record(1, timeoutTag)
	metrics.ActivityTimeout.With(handler).Record(1, timeoutTag)
}

// SearchAttributes implements chasm.VisibilitySearchAttributesProvider interface.
// Returns the current search attribute values for this activity execution.
func (a *Activity) SearchAttributes(_ chasm.Context) []chasm.SearchAttributeKeyValue {
	return []chasm.SearchAttributeKeyValue{
		TypeSearchAttribute.Value(a.GetActivityType().GetName()),
		StatusSearchAttribute.Value(InternalStatusToAPIStatus(a.GetStatus()).String()),
		chasm.SearchAttributeTaskQueue.Value(a.GetTaskQueue().GetName()),
	}
}
