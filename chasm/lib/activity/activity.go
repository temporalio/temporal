package activity

import (
	"errors"
	"fmt"
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
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ActivityStore interface {
	// PopulateRecordActivityTaskStartedResponse populates the response for RecordActivityTaskStarted
	PopulateRecordActivityTaskStartedResponse(ctx chasm.Context, key chasm.EntityKey, response *historyservice.RecordActivityTaskStartedResponse) error

	// RecordCompletion applies the provided function to record activity completion
	RecordCompletion(ctx chasm.MutableContext, applyFn func(ctx chasm.MutableContext) error) error
}

// Activity component represents an activity execution persistence object and can be either standalone activity or one
// embedded within a workflow.
// TODO implement VisibilitySearchAttributesProvider to support timeout status
type Activity struct {
	chasm.UnimplementedComponent

	*activitypb.ActivityState

	Visibility    chasm.Field[*chasm.Visibility]
	Attempt       chasm.Field[*activitypb.ActivityAttemptState]
	LastHeartbeat chasm.Field[*activitypb.ActivityHeartbeatState]
	Outcome       chasm.Field[*activitypb.ActivityOutcome]
	// Standalone only
	RequestData chasm.Field[*activitypb.ActivityRequestData]
	// Pointer to an implementation of the "store". for a workflow activity this would be a parent pointer back to
	// the workflow. For a standalone activity this would be nil.
	// TODO: revisit a standalone activity pointing to itself once we handle storing it more efficiently.
	// TODO: figure out better naming.
	Store chasm.Field[ActivityStore]
}

// RecordActivityTaskStartedParams holds parameters for RecordActivityTaskStarted
type RecordActivityTaskStartedParams struct {
	VersionDirective *taskqueuespb.TaskVersionDirective
	WorkerIdentity   string
}

// LifecycleState TODO: we need to add more lifecycle states to better categorize some activity states, particulary for terminated/canceled.
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
	visibility, err := chasm.NewVisibilityWithData(ctx, request.GetSearchAttributes().GetIndexedFields(), request.GetMemo().GetFields())
	if err != nil {
		return nil, err
	}

	// TODO flatten this when API is updated
	options := request.GetOptions()

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:           request.ActivityType,
			TaskQueue:              options.GetTaskQueue(),
			ScheduleToCloseTimeout: options.GetScheduleToCloseTimeout(),
			ScheduleToStartTimeout: options.GetScheduleToStartTimeout(),
			StartToCloseTimeout:    options.GetStartToCloseTimeout(),
			HeartbeatTimeout:       options.GetHeartbeatTimeout(),
			RetryPolicy:            options.GetRetryPolicy(),
			Priority:               request.Priority,
		},
		Attempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{}),
		RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
			Input:        request.Input,
			Header:       request.Header,
			UserMetadata: request.UserMetadata,
		}),
		Outcome:    chasm.NewDataField(ctx, &activitypb.ActivityOutcome{}),
		Visibility: chasm.NewComponentField(ctx, visibility),
	}

	activity.ScheduledTime = timestamppb.New(ctx.Now(activity))

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

// RecordActivityTaskStarted updates the activity on recording activity task started and populates the response.
func (a *Activity) RecordActivityTaskStarted(ctx chasm.MutableContext, params RecordActivityTaskStartedParams) (*historyservice.RecordActivityTaskStartedResponse, error) {
	if err := TransitionStarted.Apply(a, ctx, nil); err != nil {
		return nil, err
	}

	attempt, err := a.Attempt.Get(ctx)
	if err != nil {
		return nil, err
	}

	attempt.LastStartedTime = timestamppb.New(ctx.Now(a))
	attempt.LastWorkerIdentity = params.WorkerIdentity

	if versionDirective := params.VersionDirective.GetDeploymentVersion(); versionDirective != nil {
		attempt.LastDeploymentVersion = &deploymentpb.WorkerDeploymentVersion{
			BuildId:        versionDirective.GetBuildId(),
			DeploymentName: versionDirective.GetDeploymentName(),
		}
	}

	store, err := a.Store.Get(ctx)
	if err != nil {
		return nil, err
	}

	response := &historyservice.RecordActivityTaskStartedResponse{}
	if store == nil {
		if err := a.PopulateRecordActivityTaskStartedResponse(ctx, ctx.ExecutionKey(), response); err != nil {
			return nil, err
		}
	} else {
		if err := store.PopulateRecordActivityTaskStartedResponse(ctx, ctx.ExecutionKey(), response); err != nil {
			return nil, err
		}
	}

	return response, nil
}

func (a *Activity) PopulateRecordActivityTaskStartedResponse(ctx chasm.Context, key chasm.EntityKey, response *historyservice.RecordActivityTaskStartedResponse) error {
	attempt, err := a.Attempt.Get(ctx)
	if err != nil {
		return err
	}

	lastHeartbeat, err := a.LastHeartbeat.Get(ctx)
	if err != nil {
		return err
	}

	requestData, err := a.RequestData.Get(ctx)
	if err != nil {
		return err
	}

	response.StartedTime = attempt.LastStartedTime
	response.Attempt = attempt.GetCount()
	if lastHeartbeat != nil {
		response.HeartbeatDetails = lastHeartbeat.GetDetails()
	}
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

func (a *Activity) RecordCompletion(ctx chasm.MutableContext, applyFn func(ctx chasm.MutableContext) error) error {
	return applyFn(ctx)
}

// recordFromScheduledTimeOut records schedule-to-start or schedule-to-close timeouts. Such timeouts are not retried so we
// set the outcome failure directly and leave the attempt failure as is.
func (a *Activity) recordFromScheduledTimeOut(ctx chasm.MutableContext, timeoutType enumspb.TimeoutType) error {
	outcome, err := a.Outcome.Get(ctx)
	if err != nil {
		return err
	}

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

// recordStartToCloseTimedOut records start-to-close timeouts. These come from retried attempts so we update the attempt
// failure info but leave the outcome failure empty to avoid duplication
func (a *Activity) recordStartToCloseTimedOut(ctx chasm.MutableContext, retryInterval time.Duration, noRetriesLeft bool) error {
	outcome, err := a.Outcome.Get(ctx)
	if err != nil {
		return err
	}

	timeoutType := enumspb.TIMEOUT_TYPE_START_TO_CLOSE

	failure := &failurepb.Failure{
		Message: fmt.Sprintf(common.FailureReasonActivityTimeout, timeoutType.String()),
		FailureInfo: &failurepb.Failure_TimeoutFailureInfo{
			TimeoutFailureInfo: &failurepb.TimeoutFailureInfo{
				TimeoutType: timeoutType,
			},
		},
	}

	attempt, err := a.Attempt.Get(ctx)
	if err != nil {
		return err
	}

	currentTime := timestamppb.New(ctx.Now(a))

	attempt.LastFailureDetails = &activitypb.ActivityAttemptState_LastFailureDetails{
		Failure: failure,
		Time:    currentTime,
	}
	attempt.LastAttemptCompleteTime = currentTime

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

func (a *Activity) hasEnoughTimeForRetry(ctx chasm.Context) (bool, error) {
	attempt, err := a.Attempt.Get(ctx)
	if err != nil {
		return false, err
	}

	retryInterval := backoff.CalculateExponentialRetryInterval(a.RetryPolicy, attempt.Count)

	deadline := a.ScheduledTime.AsTime().Add(a.GetScheduleToCloseTimeout().AsDuration())

	return ctx.Now(a).Add(retryInterval).Before(deadline), nil
}

func (a *Activity) RecordHeartbeat(ctx chasm.MutableContext, details *commonpb.Payloads) (chasm.NoValue, error) {
	a.LastHeartbeat = chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{
		RecordedTime: timestamppb.New(ctx.Now(a)),
		Details:      details,
	})
	return nil, nil
}

func (a *Activity) buildActivityExecutionInfo(ctx chasm.Context) (*activity.ActivityExecutionInfo, error) {
	if a.ActivityState == nil {
		return nil, errors.New("activity state is nil")
	}

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

	requestData, err := a.RequestData.Get(ctx)
	if err != nil {
		return nil, err
	}

	key := ctx.ExecutionKey()

	info := &activity.ActivityExecutionInfo{
		ActivityId:    key.BusinessID,
		RunId:         key.EntityID,
		ActivityType:  a.GetActivityType(),
		Status:        status,
		RunState:      runState,
		ScheduledTime: a.GetScheduledTime(),
		Priority:      a.GetPriority(),
		Header:        requestData.GetHeader(),
		// TODO(dan): populate remaining fields
	}

	return info, nil
}

func (a *Activity) buildPollActivityExecutionResponse(
	ctx chasm.Context,
	req *activitypb.PollActivityExecutionRequest,
) (*activitypb.PollActivityExecutionResponse, error) {
	request := req.GetFrontendRequest()

	// TODO(dan): pass ref into this function?
	ref, err := ctx.Ref(a)
	if err != nil {
		return nil, err
	}
	token, err := chasm.EncodeStateToken(ref)
	if err != nil {
		return nil, err
	}

	var info *activity.ActivityExecutionInfo
	if request.GetIncludeInfo() {
		info, err = a.buildActivityExecutionInfo(ctx)
		if err != nil {
			return nil, err
		}
	}

	var input *commonpb.Payloads
	if request.GetIncludeInput() {
		activityRequest, err := a.RequestData.Get(ctx)
		if err != nil {
			return nil, err
		}
		input = activityRequest.GetInput()
	}

	response := &workflowservice.PollActivityExecutionResponse{
		Info:                     info,
		RunId:                    ctx.ExecutionKey().EntityID,
		Input:                    input,
		StateChangeLongPollToken: token,
	}

	if request.GetIncludeOutcome() {
		activityOutcome, err := a.Outcome.Get(ctx)
		if err != nil {
			return nil, err
		}
		switch v := activityOutcome.GetVariant().(type) {
		case *activitypb.ActivityOutcome_Failed_:
			response.Outcome = &workflowservice.PollActivityExecutionResponse_Failure{
				Failure: v.Failed.GetFailure(),
			}
		case *activitypb.ActivityOutcome_Successful_:
			response.Outcome = &workflowservice.PollActivityExecutionResponse_Result{
				Result: v.Successful.GetOutput(),
			}
		}
	}

	return &activitypb.PollActivityExecutionResponse{
		FrontendResponse: response,
	}, nil
}
