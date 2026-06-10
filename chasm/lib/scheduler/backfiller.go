package scheduler

import (
	"fmt"
	"time"

	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	schedulescommon "go.temporal.io/server/common/schedules"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The Backfiller component is responsible for buffering manually
// requested actions. Each backfill request has its own Backfiller node.
type Backfiller struct {
	chasm.UnimplementedComponent

	*schedulerpb.BackfillerState

	Scheduler chasm.ParentPtr[*Scheduler]

	EventLog chasm.Field[*EventLog]
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
	id := schedulescommon.GenerateBackfillerID()
	backfiller := newBackfillerWithState(ctx, &schedulerpb.BackfillerState{
		BackfillId:        id,
		LastProcessedTime: timestamppb.New(ctx.Now(scheduler)),
	})

	if scheduler.Backfillers == nil {
		scheduler.Backfillers = make(chasm.Map[string, *Backfiller])
	}
	scheduler.Backfillers[id] = chasm.NewComponentField(ctx, backfiller)
	scheduler.EventLog.Get(ctx).LogEvent(ctx, fmt.Sprintf("added backfiller: %s", id))

	return backfiller
}

func newBackfillerWithState(ctx chasm.MutableContext, state *schedulerpb.BackfillerState) *Backfiller {
	backfiller := &Backfiller{
		BackfillerState: state,
		EventLog:        chasm.NewComponentField(ctx, NewEventLog(ctx)),
	}
	backfiller.scheduleTask(ctx, chasm.TaskScheduledTimeImmediate)
	return backfiller
}

// scheduleTask schedules a BackfillerTask at the given time.
func (b *Backfiller) scheduleTask(ctx chasm.MutableContext, scheduledTime time.Time) {
	b.EventLog.Get(ctx).LogEvent(ctx,
		fmt.Sprintf("scheduled backfillerTask for %s", scheduledTime.Format(time.RFC3339)))
	ctx.AddTask(b, chasm.TaskAttributes{
		ScheduledTime: scheduledTime,
	}, &schedulerpb.BackfillerTask{})
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
