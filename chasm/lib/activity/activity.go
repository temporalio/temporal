package activity

import (
	"context"

	"github.com/pkg/errors"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
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

	// Pointer to an implementation of the "store" (for a workflow activity this would be a parent pointer back to
	// the workflow).
	// TODO: figure out better naming.
	Store chasm.Field[ActivityStore]
}

// LifecycleState TODO: we need to add more lifecycle states to better categorize some activity states, particulary for terminated/canceled.
func (a Activity) LifecycleState(_ chasm.Context) chasm.LifecycleState {
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

	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:    request.ActivityType,
			ActivityOptions: request.Options,
			Priority:        request.Priority,
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

	// Standalone activities point to themselves as the store.
	activity.Store = chasm.Field[ActivityStore](chasm.ComponentPointerTo(ctx, activity))

	return activity, nil
}

func NewEmbeddedActivity(
	ctx chasm.MutableContext,
	state *activitypb.ActivityState,
	parent ActivityStore,
) {
}

// HandleRecordActivityTaskStarted processes the start of an activity task execution. It transitions the activity to
// started state, updates the component fields, and returns the updated activity component.
func HandleRecordActivityTaskStarted(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	versionDirective *taskqueuespb.TaskVersionDirective,
	workerIdentity string,
) (*historyservice.RecordActivityTaskStartedResponse, error) {
	response, _, err := chasm.UpdateComponent(
		ctx,
		activityRef,
		func(a *Activity, ctx chasm.MutableContext, _ any) (*historyservice.RecordActivityTaskStartedResponse, error) {
			if err := TransitionStarted.Apply(a, ctx, nil); err != nil {
				return nil, err
			}

			attempt, err := a.Attempt.Get(ctx)
			if err != nil {
				return nil, err
			}

			attempt.LastStartedTime = timestamppb.New(ctx.Now(a))
			attempt.LastWorkerIdentity = workerIdentity

			if versionDirective := versionDirective.GetDeploymentVersion(); versionDirective != nil {
				attempt.LastDeploymentVersion = &deploymentpb.WorkerDeploymentVersion{
					BuildId:        versionDirective.GetBuildId(),
					DeploymentName: versionDirective.GetDeploymentName(),
				}
			}

			store, err := a.Store.Get(ctx)
			if err != nil {
				return nil, err
			}

			if store == nil {
				return nil, errors.New("activity store is nil")
			}

			response := &historyservice.RecordActivityTaskStartedResponse{}
			if err := store.PopulateRecordActivityTaskStartedResponse(ctx, activityRef.EntityKey, response); err != nil {
				return nil, err
			}

			return response, nil
		},
		nil,
	)
	if err != nil {
		return nil, err
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

	options := a.GetActivityOptions()

	response.StartedTime = attempt.LastStartedTime
	response.Attempt = attempt.GetCount()
	if lastHeartbeat != nil {
		response.HeartbeatDetails = lastHeartbeat.GetDetails()
	}
	response.Priority = a.GetPriority()
	response.RetryPolicy = options.GetRetryPolicy()
	response.ScheduledEvent = &historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
		Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
			ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
				ActivityId:             key.BusinessID,
				ActivityType:           a.GetActivityType(),
				Input:                  requestData.GetInput(),
				Header:                 requestData.GetHeader(),
				TaskQueue:              options.GetTaskQueue(),
				ScheduleToCloseTimeout: options.GetScheduleToCloseTimeout(),
				ScheduleToStartTimeout: options.GetScheduleToStartTimeout(),
				StartToCloseTimeout:    options.GetStartToCloseTimeout(),
				HeartbeatTimeout:       options.GetHeartbeatTimeout(),
			},
		},
	}

	return nil
}

func (a Activity) RecordCompletion(ctx chasm.MutableContext) error {
	return serviceerror.NewUnimplemented("RecordCompletion is not implemented")
}

func (a *Activity) RecordHeartbeat(ctx chasm.MutableContext, details *commonpb.Payloads) (chasm.NoValue, error) {
	a.LastHeartbeat = chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{
		RecordedTime: timestamppb.New(ctx.Now(a)),
		Details:      details,
	})
	return nil, nil
}
