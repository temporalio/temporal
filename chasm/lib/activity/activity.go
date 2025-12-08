package activity

import (
	"fmt"
	"slices"
	"time"

	"go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/payload"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ActivityStore interface {
	// PopulateRecordStartedResponse populates the response for RecordActivityTaskStarted
	PopulateRecordStartedResponse(ctx chasm.Context, key chasm.ExecutionKey, response *historyservice.RecordActivityTaskStartedResponse) error

	// RecordCompleted applies the provided function to record activity completion
	RecordCompleted(ctx chasm.MutableContext, applyFn func(ctx chasm.MutableContext) error) error
}

// Activity component represents an activity execution persistence object and can be either standalone activity or one
// embedded within a workflow.
// TODO implement VisibilitySearchAttributesProvider to support timeout status
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
	// TODO: revisit a standalone activity pointing to itself once we handle storing it more efficiently.
	// TODO: figure out better naming.
	Store chasm.Field[ActivityStore]
}

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
	}, nil
}

// HandleStarted updates the activity on recording activity task started and populates the response.
func (a *Activity) HandleStarted(ctx chasm.MutableContext, request *historyservice.RecordActivityTaskStartedRequest) (
	*historyservice.RecordActivityTaskStartedResponse, error,
) {
	if err := TransitionStarted.Apply(a, ctx, nil); err != nil {
		return nil, err
	}

	attempt := a.LastAttempt.Get(ctx)
	attempt.StartedTime = timestamppb.New(ctx.Now(a))
	attempt.LastWorkerIdentity = request.GetPollRequest().GetIdentity()

	if versionDirective := request.GetVersionDirective().GetDeploymentVersion(); versionDirective != nil {
		attempt.LastDeploymentVersion = &deploymentpb.WorkerDeploymentVersion{
			BuildId:        versionDirective.GetBuildId(),
			DeploymentName: versionDirective.GetDeploymentName(),
		}
	}
	response := &historyservice.RecordActivityTaskStartedResponse{}
	err := a.StoreOrSelf(ctx).PopulateRecordStartedResponse(ctx, ctx.ExecutionKey(), response)
	return response, err
}

func (a *Activity) PopulateRecordStartedResponse(ctx chasm.Context, key chasm.ExecutionKey, response *historyservice.RecordActivityTaskStartedResponse) error {
	lastHeartbeat, _ := a.LastHeartbeat.TryGet(ctx)
	if lastHeartbeat != nil {
		response.HeartbeatDetails = lastHeartbeat.GetDetails()
	}
	requestData := a.RequestData.Get(ctx)
	attempt := a.LastAttempt.Get(ctx)
	response.StartedTime = attempt.StartedTime
	response.Attempt = attempt.GetCount()
	response.Priority = a.GetPriority()
	response.RetryPolicy = a.GetRetryPolicy()
	response.ScheduledEvent = &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
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
	}
	return nil
}

func (a *Activity) RecordCompleted(ctx chasm.MutableContext, applyFn func(ctx chasm.MutableContext) error) error {
	return applyFn(ctx)
}

// HandleCompleted updates the activity on activity completion.
func (a *Activity) HandleCompleted(ctx chasm.MutableContext, request *historyservice.RespondActivityTaskCompletedRequest) (
	*historyservice.RespondActivityTaskCompletedResponse, error,
) {
	if err := TransitionCompleted.Apply(a, ctx, request); err != nil {
		return nil, err
	}

	return &historyservice.RespondActivityTaskCompletedResponse{}, nil
}

// HandleFailed updates the activity on activity failure. if the activity is retryable, it will be rescheduled
// for retry instead.
func (a *Activity) HandleFailed(ctx chasm.MutableContext, req *historyservice.RespondActivityTaskFailedRequest) (
	*historyservice.RespondActivityTaskFailedResponse, error,
) {
	failure := req.GetFailedRequest().GetFailure()

	shouldRetry, retryInterval, err := a.shouldRetryOnFailure(ctx, failure)
	if err != nil {
		return nil, err
	}

	if shouldRetry {
		if err := TransitionRescheduled.Apply(a, ctx, rescheduleEvent{
			retryInterval: retryInterval,
			failure:       failure,
		}); err != nil {
			return nil, err
		}

		return &historyservice.RespondActivityTaskFailedResponse{}, nil
	}

	// No more retries, transition to failed state
	if err := TransitionFailed.Apply(a, ctx, req); err != nil {
		return nil, err
	}

	return &historyservice.RespondActivityTaskFailedResponse{}, nil
}

// HandleCanceled updates the activity on activity canceled.
func (a *Activity) HandleCanceled(ctx chasm.MutableContext, request *historyservice.RespondActivityTaskCanceledRequest) (
	*historyservice.RespondActivityTaskCanceledResponse, error,
) {
	if err := TransitionCanceled.Apply(a, ctx, request.GetCancelRequest().GetDetails()); err != nil {
		return nil, err
	}

	return &historyservice.RespondActivityTaskCanceledResponse{}, nil
}

func (a *Activity) handleTerminated(ctx chasm.MutableContext, req *activitypb.TerminateActivityExecutionRequest) (
	*activitypb.TerminateActivityExecutionResponse, error,
) {
	if err := TransitionTerminated.Apply(a, ctx, req); err != nil {
		return nil, err
	}

	return &activitypb.TerminateActivityExecutionResponse{}, nil
}

// getLastHeartbeat retrieves the last heartbeat state, initializing it if not present. The heartbeat is lazily created
// to avoid unnecessary writes when heartbeats are not used.
func (a *Activity) getLastHeartbeat(ctx chasm.MutableContext) *activitypb.ActivityHeartbeatState {
	heartbeat, ok := a.LastHeartbeat.TryGet(ctx)
	if !ok {
		heartbeat = &activitypb.ActivityHeartbeatState{}
		a.LastHeartbeat = chasm.NewDataField(ctx, heartbeat)
	}
	return heartbeat
}

func (a *Activity) handleCancellationRequested(ctx chasm.MutableContext, req *activitypb.RequestCancelActivityExecutionRequest) (
	*activitypb.RequestCancelActivityExecutionResponse, error,
) {
	newReqID := req.GetFrontendRequest().GetRequestId()
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

	if err := TransitionCancelRequested.Apply(a, ctx, req.GetFrontendRequest()); err != nil {
		return nil, err
	}

	if isCancelImmediately {
		details := &commonpb.Payloads{
			Payloads: []*commonpb.Payload{
				payload.EncodeString(req.GetFrontendRequest().GetReason()),
			},
		}

		err := TransitionCanceled.Apply(a, ctx, details)
		if err != nil {
			return nil, err
		}
	}

	return &activitypb.RequestCancelActivityExecutionResponse{}, nil
}

func (a *Activity) shouldRetryOnFailure(ctx chasm.Context, failure *failurepb.Failure) (bool, time.Duration, error) {
	var isRetryable bool

	if failure.GetApplicationFailureInfo() != nil {
		appFailure := failure.GetApplicationFailureInfo()
		isRetryable = !appFailure.GetNonRetryable() && !slices.Contains(
			a.GetRetryPolicy().GetNonRetryableErrorTypes(),
			appFailure.GetType(),
		)
	}

	if !isRetryable {
		return false, 0, nil
	}

	overridingRetryInterval := failure.GetApplicationFailureInfo().GetNextRetryDelay().AsDuration()

	return a.shouldRetry(ctx, overridingRetryInterval)
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
	noRetriesLeft bool,
) error {
	outcome := a.Outcome.Get(ctx)
	attempt := a.LastAttempt.Get(ctx)
	currentTime := timestamppb.New(ctx.Now(a))

	attempt.LastFailureDetails = &activitypb.ActivityAttemptState_LastFailureDetails{
		Failure: failure,
		Time:    currentTime,
	}
	attempt.CompleteTime = currentTime

	// If the activity has exhausted retries, mark the outcome failure as well but don't store duplicate failure info.
	// Also reset the retry interval as there won't be any more retries.
	if noRetriesLeft {
		outcome.Variant = &activitypb.ActivityOutcome_Failed_{}
		attempt.CurrentRetryInterval = nil
	} else {
		attempt.CurrentRetryInterval = durationpb.New(retryInterval)
	}
	return nil
}

func (a *Activity) shouldRetry(ctx chasm.Context, overridingRetryInterval time.Duration) (bool, time.Duration, error) {
	if !TransitionRescheduled.Possible(a) {
		return false, 0, nil
	}
	attempt := a.LastAttempt.Get(ctx)
	retryPolicy := a.RetryPolicy

	enoughAttempts := retryPolicy.GetMaximumAttempts() == 0 || attempt.GetCount() < retryPolicy.GetMaximumAttempts()
	enoughTime, retryInterval, err := a.hasEnoughTimeForRetry(ctx, overridingRetryInterval)
	if err != nil {
		return false, 0, err
	}
	return enoughAttempts && enoughTime, retryInterval, nil
}

// hasEnoughTimeForRetry checks if there is enough time left in the schedule-to-close timeout. If sufficient time
// remains, it will also return a valid retry interval
func (a *Activity) hasEnoughTimeForRetry(ctx chasm.Context, overridingRetryInterval time.Duration) (bool, time.Duration, error) {
	attempt := a.LastAttempt.Get(ctx)

	// Use overriding retry interval if provided, else calculate based on retry policy
	retryInterval := overridingRetryInterval
	if retryInterval <= 0 {
		retryInterval = backoff.CalculateExponentialRetryInterval(a.RetryPolicy, attempt.Count)
	}

	scheduleToClose := a.GetScheduleToCloseTimeout().AsDuration()
	if scheduleToClose == 0 {
		return true, retryInterval, nil
	}

	deadline := a.ScheduleTime.AsTime().Add(scheduleToClose)
	return ctx.Now(a).Add(retryInterval).Before(deadline), retryInterval, nil
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

func (a *Activity) RecordHeartbeat(ctx chasm.MutableContext, details *commonpb.Payloads) (chasm.NoValue, error) {
	a.LastHeartbeat = chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{
		RecordedTime: timestamppb.New(ctx.Now(a)),
		Details:      details,
	})
	return nil, nil
}

func (a *Activity) buildActivityExecutionInfo(ctx chasm.Context) (*activity.ActivityExecutionInfo, error) {
	// TODO(dan): support pause states
	var status enumspb.ActivityExecutionStatus
	var runState enumspb.PendingActivityState
	switch a.GetStatus() {
	case activitypb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED:
		status = enumspb.ACTIVITY_EXECUTION_STATUS_UNSPECIFIED
		runState = enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case activitypb.ACTIVITY_EXECUTION_STATUS_SCHEDULED:
		status = enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING
		runState = enumspb.PENDING_ACTIVITY_STATE_SCHEDULED
	case activitypb.ACTIVITY_EXECUTION_STATUS_STARTED:
		status = enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING
		runState = enumspb.PENDING_ACTIVITY_STATE_STARTED
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCEL_REQUESTED:
		status = enumspb.ACTIVITY_EXECUTION_STATUS_RUNNING
		runState = enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED:
		status = enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED
		runState = enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case activitypb.ACTIVITY_EXECUTION_STATUS_FAILED:
		status = enumspb.ACTIVITY_EXECUTION_STATUS_FAILED
		runState = enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED:
		status = enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED
		runState = enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED:
		status = enumspb.ACTIVITY_EXECUTION_STATUS_TERMINATED
		runState = enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	case activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
		status = enumspb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT
		runState = enumspb.PENDING_ACTIVITY_STATE_UNSPECIFIED
	default:
		return nil, serviceerror.NewInternalf("unknown activity execution status: %s", a.GetStatus())
	}

	requestData := a.RequestData.Get(ctx)
	attempt := a.LastAttempt.Get(ctx)
	heartbeat, _ := a.LastHeartbeat.TryGet(ctx)
	key := ctx.ExecutionKey()

	info := &activity.ActivityExecutionInfo{
		ActivityId:              key.BusinessID,
		ActivityType:            a.GetActivityType(),
		Attempt:                 attempt.GetCount(),
		CanceledReason:          a.CancelState.GetReason(),
		Header:                  requestData.GetHeader(),
		HeartbeatDetails:        heartbeat.GetDetails(),
		LastAttemptCompleteTime: attempt.GetCompleteTime(),
		LastFailure:             attempt.GetLastFailureDetails().GetFailure(),
		LastHeartbeatTime:       heartbeat.GetRecordedTime(),
		LastStartedTime:         attempt.GetStartedTime(),
		LastWorkerIdentity:      attempt.GetLastWorkerIdentity(),
		Priority:                a.GetPriority(),
		RunId:                   key.RunID,
		RunState:                runState,
		ScheduleTime:            a.GetScheduleTime(),
		Status:                  status,
		// TODO(dan): populate remaining fields
	}

	return info, nil
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

	info, err := a.buildActivityExecutionInfo(ctx)
	if err != nil {
		return nil, err
	}

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

func (a *Activity) buildGetActivityExecutionOutcomeResponse(
	ctx chasm.Context,
) (*activitypb.GetActivityExecutionOutcomeResponse, error) {
	return &activitypb.GetActivityExecutionOutcomeResponse{
		FrontendResponse: &workflowservice.GetActivityExecutionOutcomeResponse{
			RunId:   ctx.ExecutionKey().RunID,
			Outcome: a.outcome(ctx),
		},
	}, nil
}

// outcome retrieves the activity outcome (result or failure) if the activity has completed.
// Returns nil if the activity has not completed.
func (a *Activity) outcome(ctx chasm.Context) *activity.ActivityExecutionOutcome {
	activityOutcome := a.Outcome.Get(ctx)
	// Check for successful outcome
	if successful := activityOutcome.GetSuccessful(); successful != nil {
		return &activity.ActivityExecutionOutcome{
			Value: &activity.ActivityExecutionOutcome_Result{Result: successful.GetOutput()},
		}
	}
	// Check for failure in outcome
	if failure := activityOutcome.GetFailed().GetFailure(); failure != nil {
		return &activity.ActivityExecutionOutcome{
			Value: &activity.ActivityExecutionOutcome_Failure{Failure: failure},
		}
	}
	// Check for failure in last attempt details
	shouldHaveFailure := (a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_FAILED ||
		a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT ||
		a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED ||
		a.GetStatus() == activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED)
	if shouldHaveFailure {
		if details := a.LastAttempt.Get(ctx).GetLastFailureDetails(); details != nil {
			return &activity.ActivityExecutionOutcome{
				Value: &activity.ActivityExecutionOutcome_Failure{Failure: details.GetFailure()},
			}
		}
	}
	return nil
}

// StoreOrSelf returns the store for the activity. If the store is not set as a field (e.g.
// standalone activities), it returns the activity itself.
func (a *Activity) StoreOrSelf(ctx chasm.MutableContext) ActivityStore {
	store, ok := a.Store.TryGet(ctx)
	if ok {
		return store
	}
	return a
}
