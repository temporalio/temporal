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
	RequestData chasm.Field[*activitypb.ActivityHeartbeatState]

	// Pointer to an implementation of the "store" (for a workflow activity this would be a parent pointer back to
	// the workflow).
	// TODO: figure out better naming.
	Store chasm.Field[ActivityStore]
}

// LifecycleState implements chasm.Component.
func (activity *Activity) LifecycleState(chasm.Context) chasm.LifecycleState {
	// TODO
	return chasm.LifecycleStateRunning
}

func NewStandaloneActivity(
	ctx chasm.MutableContext,
	request *workflowservice.StartActivityExecutionRequest,
) (*Activity, error) {
	return &Activity{
		//
		// chasm.NewVisibilityWithData(ctx)
	}, nil
}

func NewEmbeddedActivity(
	ctx chasm.MutableContext,
	state *activitypb.ActivityState,
	parent ActivityStore,
) {
}

func (activity *Activity) PopulateRecordActivityTaskStartedResponse(ctx chasm.Context, res *historyservice.RecordActivityTaskStartedResponse) error {
	store, err := activity.Store.Get(ctx)
	if err != nil {
		return err
	}
	if err := store.PopulateRecordActivityTaskStartedResponse(ctx, res); err != nil {
		return err
	}
	// ...
	return nil
}

func (activity *Activity) RecordHeartbeat(ctx chasm.MutableContext, details *commonpb.Payloads) (*struct{}, error) {
	activity.LastHeartbeat = chasm.NewDataField(ctx, &activitypb.ActivityHeartbeatState{
		RecordedTime: timestamppb.New(ctx.Now(activity)),
		Details:      details,
	})
	return nil, nil
}
