package collection

import (
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	activitypb "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	collectionpb "go.temporal.io/server/chasm/lib/collection/gen/collectionpb/v1"
	"go.temporal.io/server/common"
)

var _ activity.ActivityStore = (*Collection)(nil)

const (
	libraryName   = "collection"
	componentName = "collection"
)

var (
	Archetype   = chasm.FullyQualifiedName(libraryName, componentName)
	ArchetypeID = chasm.GenerateTypeID(Archetype)
)

// Collection is the CHASM archetype backing the user-facing Map. For now it is an empty,
// runnable root component: it can be started, described, closed, and deleted, but holds no
// items, operations, or config. Those are added in subsequent commits.
type Collection struct {
	chasm.UnimplementedComponent

	*collectionpb.CollectionState

	// Activities holds the embedded record-arm operations, keyed by item id (1-1 with items for now).
	Activities chasm.Map[string, *activity.Activity]
}

func NewCollection(_ chasm.MutableContext) (*Collection, error) {
	return &Collection{
		CollectionState: &collectionpb.CollectionState{},
	}, nil
}

func (c *Collection) LifecycleState(_ chasm.Context) chasm.LifecycleState {
	switch {
	case c.Canceled:
		return chasm.LifecycleStateFailed
	case c.Closed:
		return chasm.LifecycleStateCompleted
	default:
		return chasm.LifecycleStateRunning
	}
}

func (c *Collection) Terminate(
	_ chasm.MutableContext,
	_ chasm.TerminateComponentRequest,
) (chasm.TerminateComponentResponse, error) {
	c.Closed = true
	return chasm.TerminateComponentResponse{}, nil
}

func (c *Collection) ContextMetadata(_ chasm.Context) map[string]string {
	return nil
}

func (c *Collection) Describe(
	ctx chasm.Context,
	_ *collectionpb.DescribeCollectionExecutionRequest,
) (*collectionpb.DescribeCollectionExecutionResponse, error) {
	items := make([]*collectionpb.ItemDescription, 0, len(c.Activities))
	for itemID, field := range c.Activities {
		items = append(items, &collectionpb.ItemDescription{
			ItemId: itemID,
			Status: field.Get(ctx).StateMachineState().String(),
		})
	}
	return &collectionpb.DescribeCollectionExecutionResponse{
		State: common.CloneProto(c.CollectionState),
		Items: items,
	}, nil
}

// RecordCompleted implements activity.ActivityStore. Embedded activities delegate their terminal
// transitions here; for now the Collection simply applies the activity's own state update.
func (c *Collection) RecordCompleted(ctx chasm.MutableContext, applyFn func(ctx chasm.MutableContext) error) error {
	return applyFn(ctx)
}

// AddItems admits new items, each dispatching an embedded activity (the record arm). Items are
// deduplicated by item id.
func (c *Collection) AddItems(
	ctx chasm.MutableContext,
	req *collectionpb.AddCollectionItemsRequest,
) (*collectionpb.AddCollectionItemsResponse, error) {
	if c.Activities == nil {
		c.Activities = make(chasm.Map[string, *activity.Activity])
	}
	for _, item := range req.GetItems() {
		if _, ok := c.Activities[item.GetItemId()]; ok {
			continue
		}
		op := item.GetActivity()
		if op == nil {
			return nil, serviceerror.NewInvalidArgumentf("item %q has no activity operation", item.GetItemId())
		}
		act, err := activity.NewEmbeddedActivity(
			ctx,
			&activitypb.ActivityState{
				ActivityType:        op.GetActivityType(),
				TaskQueue:           op.GetTaskQueue(),
				StartToCloseTimeout: op.GetStartToCloseTimeout(),
			},
			&activitypb.ActivityRequestData{
				Input: op.GetInput(),
			},
		)
		if err != nil {
			return nil, err
		}
		if err := activity.TransitionScheduled.Apply(act, ctx, nil); err != nil {
			return nil, err
		}
		c.Activities[item.GetItemId()] = chasm.NewComponentField(ctx, act)
	}
	return &collectionpb.AddCollectionItemsResponse{}, nil
}

func (c *Collection) Close(
	_ chasm.MutableContext,
	_ chasm.NoValue,
) (*collectionpb.CloseCollectionExecutionResponse, error) {
	c.Closed = true
	return &collectionpb.CloseCollectionExecutionResponse{}, nil
}
