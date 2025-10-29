package activity

import (
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ActivityStore interface {
	PopulateRecordActivityTaskStartedResponse(ctx chasm.Context, key chasm.EntityKey, response *historyservice.RecordActivityTaskStartedResponse) error
	RecordCompletion(ctx chasm.MutableContext) error
}

// Activity component represents an activity execution persistence object and can be either standalone activity or one
// embedded within a workflow.
type Activity struct {
	chasm.UnimplementedComponent

	*activitypb.ActivityState

	// Standalone only
	Visibility    chasm.Field[*chasm.Visibility]
	Attempt       chasm.Field[*activitypb.ActivityAttemptState]
	LastHeartbeat chasm.Field[*activitypb.ActivityHeartbeatState]
	// Standalone only
	RequestData chasm.Field[*activitypb.ActivityRequestData]

	// Pointer to an implementation of the "store". for a workflow activity this would be a parent pointer back to
	// the workflow. For a standalone activity this would be nil.
	// TODO: revisit a standalone activity pointing to itself once we handle storing it more efficiently.
	// TODO: figure out better naming.
	Store chasm.Field[ActivityStore]
}

// RecordActivityTaskStartedParams holds parameters for RecordActivityTaskStarted method when recording activity
// task started.
type RecordActivityTaskStartedParams struct {
	EntityKey        chasm.EntityKey
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
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

// NewStandaloneActivity creates a new activity component and launches associated tasks to start execution.
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
		Attempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
			Count: 1,
		}),
		RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
			Input:        request.Input,
			Header:       request.Header,
			UserMetadata: request.UserMetadata,
		}),
		Visibility: chasm.NewComponentField(ctx, visibility),
	}

	return activity, nil
}

func NewEmbeddedActivity(
	ctx chasm.MutableContext,
	state *activitypb.ActivityState,
	parent ActivityStore,
) {
}

func (a *Activity) createAddActivityTaskRequest(ctx chasm.Context, activityRef chasm.ComponentRef) (*matchingservice.AddActivityTaskRequest, error) {
	// Get latest component ref and unmarshal into proto ref
	componentBytes, err := ctx.Ref(a)
	if err != nil {
		return nil, err
	}

	protoRef := &persistencespb.ChasmComponentRef{}
	if err := proto.Unmarshal(componentBytes, protoRef); err != nil {
		return nil, err
	}

	// Note: No need to set the vector clock here, as the components track version conflicts for read/write
	return &matchingservice.AddActivityTaskRequest{
		NamespaceId:            activityRef.NamespaceID,
		TaskQueue:              a.GetTaskQueue(),
		ScheduleToStartTimeout: a.GetScheduleToStartTimeout(),
		Priority:               a.GetPriority(),
		ComponentRef:           protoRef,
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
		if err := a.PopulateRecordActivityTaskStartedResponse(ctx, params.EntityKey, response); err != nil {
			return nil, err
		}
	} else {
		if err := store.PopulateRecordActivityTaskStartedResponse(ctx, params.EntityKey, response); err != nil {
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

func (a *Activity) RecordCompletion(_ chasm.MutableContext) error {
	return serviceerror.NewUnimplemented("RecordCompletion is not implemented")
}

func (a *Activity) RecordHeartbeat(ctx chasm.MutableContext, details *commonpb.Payloads) (chasm.NoValue, error) {
	a.LastHeartbeat = chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{
		RecordedTime: timestamppb.New(ctx.Now(a)),
		Details:      details,
	})
	return nil, nil
}
