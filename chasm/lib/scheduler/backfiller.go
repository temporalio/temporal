package scheduler

import (
	"time"

	"github.com/pborman/uuid"
	schedulepb "go.temporal.io/api/schedule/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The Backfiller component is responsible for buffering manually
// requested actions. Each backfill request has its own Backfiller node.
type Backfiller struct {
	chasm.UnimplementedComponent

	*schedulespb.BackfillerInternal

	Scheduler chasm.Field[*Scheduler]
	Invoker   chasm.Field[*Invoker]
}

// The type of Backfill represented by an invididual Backfiller.
type BackfillRequestType int

const (
	RequestTypeTrigger BackfillRequestType = iota
	RequestTypeBackfill
)

// NewRangeBackfiller returns an intialized Backfiller component, which should
// be parented under a Scheduler root node.
func NewRangeBackfiller(
	ctx chasm.MutableContext,
	scheduler *Scheduler,
	invoker *Invoker,
	request *schedulepb.BackfillRequest,
) *Backfiller {
	backfiller := newBackfiller(ctx, scheduler, invoker)
	backfiller.BackfillerInternal.Request = &schedulespb.BackfillerInternal_BackfillRequest{
		BackfillRequest: request,
	}
	return backfiller
}

func newBackfiller(
	ctx chasm.MutableContext,
	scheduler *Scheduler,
	invoker *Invoker,
) *Backfiller {
	id := uuid.New()
	backfiller := &Backfiller{
		BackfillerInternal: &schedulespb.BackfillerInternal{
			BackfillId:        id,
			LastProcessedTime: timestamppb.New(ctx.Now(scheduler)),
		},
		Scheduler: chasm.ComponentPointerTo(ctx, scheduler),
		Invoker:   chasm.ComponentPointerTo(ctx, invoker),
	}

	// Add the backfiller to the scheduler tree, and add a task to kick off backfill
	// processing.
	scheduler.Backfillers[id] = chasm.NewComponentField(ctx, backfiller)
	ctx.AddTask(backfiller, chasm.TaskAttributes{}, &schedulespb.BackfillerTask{})

	return backfiller
}

// NewImmediateBackfiller returns an intialized Backfiller component, which should
// be parented under a Scheduler root node.
func NewImmediateBackfiller(
	ctx chasm.MutableContext,
	scheduler *Scheduler,
	invoker *Invoker,
	request *schedulepb.TriggerImmediatelyRequest,
) *Backfiller {
	backfiller := newBackfiller(ctx, scheduler, invoker)
	backfiller.BackfillerInternal.Request = &schedulespb.BackfillerInternal_TriggerRequest{
		TriggerRequest: request,
	}
	return backfiller
}

func (b *Backfiller) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	// TODO - given that the component is deleted from the Scheduler's Backfillers
	// map, and has no children, do we need to do anything here?
	return chasm.LifecycleStateRunning
}

func (b *Backfiller) RequestType() BackfillRequestType {
	if b.GetTriggerRequest() != nil {
		return RequestTypeTrigger
	}

	return RequestTypeBackfill
}

type backfillProgressResult struct {
	// BufferedStarts that should be enqueued to the Invoker.
	BufferedStarts []*schedulespb.BufferedStart

	// High water mark for when state was last updated.
	LastProcessedTime time.Time

	// When true, the backfill has completed and the node can be deleted.
	Complete bool
}
