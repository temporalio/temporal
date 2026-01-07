package scheduler

import (
	"time"

	"github.com/google/uuid"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The Backfiller component is responsible for buffering manually
// requested actions. Each backfill request has its own Backfiller node.
type Backfiller struct {
	chasm.UnimplementedComponent

	*schedulerpb.BackfillerState

	Scheduler chasm.ParentPtr[*Scheduler]
}

type BackfillRequestType int

const (
	RequestTypeTrigger BackfillRequestType = iota
	RequestTypeBackfill
)

// addBackfiller returns an initialized backfiller, adding it to the scheduler's
// Backfillers.
func addBackfiller(
	ctx chasm.MutableContext,
	scheduler *Scheduler,
) *Backfiller {
	id := uuid.NewString()
	backfiller := &Backfiller{
		BackfillerState: &schedulerpb.BackfillerState{
			BackfillId:        id,
			LastProcessedTime: timestamppb.New(ctx.Now(scheduler)),
		},
	}

	// Immediately schedule the first backfiller task.
	ctx.AddTask(backfiller, chasm.TaskAttributes{}, &schedulerpb.BackfillerTask{})

	if scheduler.Backfillers == nil {
		scheduler.Backfillers = make(chasm.Map[string, *Backfiller])
	}
	scheduler.Backfillers[id] = chasm.NewComponentField(ctx, backfiller)

	return backfiller
}

func (b *Backfiller) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
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
