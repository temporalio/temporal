package activity

import (
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ActivityStore interface {
	PopulateRecordActivityTaskStartedResponse(ctx chasm.Context, res *historyservice.RecordActivityTaskStartedResponse) error
	RecordCompletion(ctx chasm.MutableContext) error
}

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

func (a Activity) LifecycleState(context chasm.Context) chasm.LifecycleState {
	switch a.Status {
	case activitypb.ACTIVITY_EXECUTION_STATUS_COMPLETED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TERMINATED,
		activitypb.ACTIVITY_EXECUTION_STATUS_CANCELED:
		return chasm.LifecycleStateCompleted
	case activitypb.ACTIVITY_EXECUTION_STATUS_FAILED,
		activitypb.ACTIVITY_EXECUTION_STATUS_TIMED_OUT:
		return chasm.LifecycleStateFailed
	default:
		return chasm.LifecycleStateRunning
	}
}

func NewStandaloneActivity(
	ctx chasm.MutableContext,
	request *workflowservice.StartActivityExecutionRequest,
) (*Activity, error) {
	visibility, err := chasm.NewVisibilityWithData(ctx, request.GetSearchAttributes().GetIndexedFields(), request.GetMemo().GetFields())
	if err != nil {
		return nil, err
	}

	return &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType:    request.ActivityType,
			ActivityOptions: request.Options,
			Priority:        request.Priority,
		},
		Attempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
			Count:           1,
			LastStartedTime: timestamppb.Now(),
		}),
		RequestData: chasm.NewDataField(ctx, &activitypb.ActivityRequestData{
			Input:        request.Input,
			Header:       request.Header,
			UserMetadata: request.UserMetadata,
		}),
		Visibility: chasm.NewComponentField(ctx, visibility),
	}, nil
}

func NewEmbeddedActivity(
	ctx chasm.MutableContext,
	state *activitypb.ActivityState,
	parent ActivityStore,
) {
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

func (a *Activity) RecordHeartbeat(ctx chasm.MutableContext, details *commonpb.Payloads) (*struct{}, error) {
	a.LastHeartbeat = chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{
		RecordedTime: timestamppb.New(ctx.Now(a)),
		Details:      details,
	})
	return nil, nil
}
