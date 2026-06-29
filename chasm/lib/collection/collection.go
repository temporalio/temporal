package collection

import (
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity"
	activitypb "go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	collectionpb "go.temporal.io/server/chasm/lib/collection/gen/collectionpb/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common"
	commonnexus "go.temporal.io/server/common/nexus"
)

var (
	_ activity.ActivityStore        = (*Collection)(nil)
	_ nexusoperation.OperationStore = (*Collection)(nil)
)

// collectionNexusWorkflowTypeName tags metrics emitted from Collection-embedded Nexus operations.
const collectionNexusWorkflowTypeName = "__temporal_collection_nexus_operation__"

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
	// NexusOperations holds the embedded execution-arm operations, keyed by item id.
	NexusOperations chasm.Map[string, *nexusoperation.Operation]
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
	items := make([]*collectionpb.ItemDescription, 0, len(c.Activities)+len(c.NexusOperations))
	for itemID, field := range c.Activities {
		items = append(items, &collectionpb.ItemDescription{
			ItemId: itemID,
			Status: field.Get(ctx).StateMachineState().String(),
		})
	}
	for itemID, field := range c.NexusOperations {
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

// OperationStore: an embedded Nexus operation delegates its transitions and invocation input to the
// Collection. For now the Collection applies the operation's own default transitions (the same the
// operation would apply if it ran standalone) and sources input from the operation's request data.

func (c *Collection) NexusOperationInvocationData(ctx chasm.Context, op *nexusoperation.Operation) (nexusoperation.InvocationData, error) {
	rd := op.RequestData.Get(ctx)
	return nexusoperation.InvocationData{Input: rd.GetInput(), Header: rd.GetNexusHeader()}, nil
}

func (c *Collection) OnNexusOperationStarted(ctx chasm.MutableContext, op *nexusoperation.Operation, token string, startTime *time.Time, links []*commonpb.Link) error {
	op.Links = append(op.Links, links...)
	return nexusoperation.TransitionStarted.Apply(op, ctx, nexusoperation.EventStarted{OperationToken: token, StartTime: startTime})
}

func (c *Collection) OnNexusOperationCompleted(ctx chasm.MutableContext, op *nexusoperation.Operation, result *commonpb.Payload, links []*commonpb.Link) error {
	op.Links = append(op.Links, links...)
	return nexusoperation.TransitionSucceeded.Apply(op, ctx, nexusoperation.EventSucceeded{Result: result})
}

func (c *Collection) OnNexusOperationFailed(ctx chasm.MutableContext, op *nexusoperation.Operation, cause *failurepb.Failure) error {
	return nexusoperation.TransitionFailed.Apply(op, ctx, nexusoperation.EventFailed{Failure: cause})
}

func (c *Collection) OnNexusOperationCanceled(ctx chasm.MutableContext, op *nexusoperation.Operation, cause *failurepb.Failure) error {
	return nexusoperation.TransitionCanceled.Apply(op, ctx, nexusoperation.EventCanceled{Failure: cause})
}

func (c *Collection) OnNexusOperationTimedOut(ctx chasm.MutableContext, op *nexusoperation.Operation, cause *failurepb.Failure, fromAttempt bool) error {
	return nexusoperation.TransitionTimedOut.Apply(op, ctx, nexusoperation.EventTimedOut{Failure: cause, FromAttempt: fromAttempt})
}

func (c *Collection) OnNexusOperationCancellationCompleted(_ chasm.MutableContext, _ *nexusoperation.Operation) error {
	return nil
}

func (c *Collection) OnNexusOperationCancellationFailed(_ chasm.MutableContext, _ *nexusoperation.Operation, _ *failurepb.Failure) error {
	return nil
}

func (c *Collection) WorkflowTypeName() string {
	return collectionNexusWorkflowTypeName
}

// AddItems admits new items, each dispatching an embedded operation. Items are deduplicated by item
// id and map 1-1 with operations.
func (c *Collection) AddItems(
	ctx chasm.MutableContext,
	req *collectionpb.AddCollectionItemsRequest,
) (*collectionpb.AddCollectionItemsResponse, error) {
	for _, item := range req.GetItems() {
		itemID := item.GetItemId()
		if c.hasItem(itemID) {
			continue
		}
		switch {
		case item.GetActivity() != nil:
			if err := c.addActivityItem(ctx, itemID, item.GetActivity()); err != nil {
				return nil, err
			}
		case item.GetNexus() != nil:
			if err := c.addNexusItem(ctx, itemID, item.GetNexus()); err != nil {
				return nil, err
			}
		default:
			return nil, serviceerror.NewInvalidArgumentf("item %q has no operation", itemID)
		}
	}
	return &collectionpb.AddCollectionItemsResponse{}, nil
}

func (c *Collection) hasItem(itemID string) bool {
	if _, ok := c.Activities[itemID]; ok {
		return true
	}
	_, ok := c.NexusOperations[itemID]
	return ok
}

func (c *Collection) addActivityItem(ctx chasm.MutableContext, itemID string, op *collectionpb.ActivityOperation) error {
	act, err := activity.NewEmbeddedActivity(
		ctx,
		&activitypb.ActivityState{
			ActivityType:        op.GetActivityType(),
			TaskQueue:           op.GetTaskQueue(),
			StartToCloseTimeout: op.GetStartToCloseTimeout(),
		},
		&activitypb.ActivityRequestData{Input: op.GetInput()},
	)
	if err != nil {
		return err
	}
	if err := activity.TransitionScheduled.Apply(act, ctx, nil); err != nil {
		return err
	}
	if c.Activities == nil {
		c.Activities = make(chasm.Map[string, *activity.Activity])
	}
	c.Activities[itemID] = chasm.NewComponentField(ctx, act)
	return nil
}

func (c *Collection) addNexusItem(ctx chasm.MutableContext, itemID string, op *collectionpb.NexusOperation) error {
	endpoint := op.GetEndpoint()
	if endpoint == "" {
		endpoint = commonnexus.SystemEndpoint
	}
	nop, err := nexusoperation.NewEmbeddedOperation(
		ctx,
		&nexusoperationpb.OperationState{
			Endpoint:               endpoint,
			Service:                op.GetService(),
			Operation:              op.GetOperation(),
			ScheduleToCloseTimeout: op.GetScheduleToCloseTimeout(),
			RequestId:              uuid.NewString(),
		},
		&nexusoperationpb.OperationRequestData{Input: op.GetInput()},
	)
	if err != nil {
		return err
	}
	if err := nexusoperation.TransitionScheduled.Apply(nop, ctx, nexusoperation.EventScheduled{}); err != nil {
		return err
	}
	if c.NexusOperations == nil {
		c.NexusOperations = make(chasm.Map[string, *nexusoperation.Operation])
	}
	c.NexusOperations[itemID] = chasm.NewComponentField(ctx, nop)
	return nil
}

func (c *Collection) Close(
	_ chasm.MutableContext,
	_ chasm.NoValue,
) (*collectionpb.CloseCollectionExecutionResponse, error) {
	c.Closed = true
	return &collectionpb.CloseCollectionExecutionResponse{}, nil
}
