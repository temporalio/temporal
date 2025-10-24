package activity

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ActivityStore interface {
	PopulateRecordActivityTaskStartedResponse(ctx chasm.Context, res *historyservice.RecordActivityTaskStartedResponse) error
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

	ctx.AddTask(
		activity,
		chasm.TaskAttributes{},
		&activitypb.ActivityStartExecuteTask{
			Attempt: 1,
		})

	return activity, nil
}

func NewEmbeddedActivity(
	ctx chasm.MutableContext,
	state *activitypb.ActivityState,
	parent ActivityStore,
) {
}

// GetActivityState reads an activity component's persisted ActivityState by its component reference.
func GetActivityState(ctx context.Context, ref chasm.ComponentRef) (*activitypb.ActivityState, error) {
	return getActivityField(ctx, ref, func(a *Activity, _ chasm.Context) (*activitypb.ActivityState, error) {
		return a.ActivityState, nil
	})
}

// GetActivityRequestData reads an activity component's RequestData field by its component reference.
func GetActivityRequestData(ctx context.Context, ref chasm.ComponentRef) (*activitypb.ActivityRequestData, error) {
	return getActivityField(ctx, ref, func(a *Activity, ctx chasm.Context) (*activitypb.ActivityRequestData, error) {
		return a.RequestData.Get(ctx)
	})
}

// GetActivityAttempt reads an activity component's Attempt field by its component reference.
func GetActivityAttempt(ctx context.Context, ref chasm.ComponentRef) (*activitypb.ActivityAttemptState, error) {
	return getActivityField(ctx, ref, func(a *Activity, ctx chasm.Context) (*activitypb.ActivityAttemptState, error) {
		return a.Attempt.Get(ctx)
	})
}

// GetActivityLastHeartbeat reads an activity component's LastHeartbeat field by its component reference.
func GetActivityLastHeartbeat(ctx context.Context, ref chasm.ComponentRef) (*activitypb.ActivityHeartbeatState, error) {
	return getActivityField(ctx, ref, func(a *Activity, ctx chasm.Context) (*activitypb.ActivityHeartbeatState, error) {
		return a.LastHeartbeat.Get(ctx)
	})
}

func getActivityField[T any](
	ctx context.Context,
	ref chasm.ComponentRef,
	getter func(*Activity, chasm.Context) (T, error),
) (T, error) {
	return chasm.ReadComponent(
		ctx,
		ref,
		func(a *Activity, ctx chasm.Context, _ any) (T, error) {
			return getter(a, ctx)
		},
		nil,
	)
}

// HandleRecordActivityTaskStarted processes the start of an activity task execution. It transitions the activity to
// started state, updates the component fields, and returns the updated activity component.
func HandleRecordActivityTaskStarted(
	ctx context.Context,
	activityRef chasm.ComponentRef,
	versionDirective *taskqueuespb.TaskVersionDirective,
	workerIdentity string,
) (*Activity, error) {
	activity, _, err := chasm.UpdateComponent(
		ctx,
		activityRef,
		func(a *Activity, ctx chasm.MutableContext, _ any) (*Activity, error) {
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

			return a, nil
		},
		nil,
	)
	if err != nil {
		return nil, err
	}

	return activity, nil
}

func (a *Activity) PopulateRecordActivityTaskStartedResponse(ctx chasm.Context, res *historyservice.RecordActivityTaskStartedResponse) error {
	store, err := a.Store.Get(ctx)
	if err != nil {
		return err
	}
	if err := store.PopulateRecordActivityTaskStartedResponse(ctx, res); err != nil {
		return err
	}
	// ...
	return nil
}

func (a *Activity) RecordHeartbeat(ctx chasm.MutableContext, details *commonpb.Payloads) (chasm.NoValue, error) {
	a.LastHeartbeat = chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{
		RecordedTime: timestamppb.New(ctx.Now(a)),
		Details:      details,
	})
	return nil, nil
}
