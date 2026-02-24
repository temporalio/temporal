package scheduler

import (
	"time"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/service/worker/scheduler"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// The Generator component is responsible for buffering actions according
// to the schedule's specification. Manually requested actions (from an immediate
// request or backfill) are separately handled in the Backfiller component.
type Generator struct {
	chasm.UnimplementedComponent

	*schedulerpb.GeneratorState

	Scheduler chasm.ParentPtr[*Scheduler]
}

// NewGenerator returns an intialized Generator component, which should
// be parented under a Scheduler root node.
func NewGenerator(ctx chasm.MutableContext) *Generator {
	generator := &Generator{
		GeneratorState: &schedulerpb.GeneratorState{
			LastProcessedTime: nil,
		},
	}

	// Kick off initial generator run as an immediate task.
	generator.Generate(ctx)

	return generator
}

// Generate immediately kicks off a new GeneratorTask. Used after updating the
// schedule specification.
func (g *Generator) Generate(ctx chasm.MutableContext) {
	g.scheduleTask(ctx, chasm.TaskScheduledTimeImmediate)
}

// scheduleTask schedules a GeneratorTask at the given time.
func (g *Generator) scheduleTask(ctx chasm.MutableContext, scheduledTime time.Time) {
	ctx.AddTask(g, chasm.TaskAttributes{
		ScheduledTime: scheduledTime,
	}, &schedulerpb.GeneratorTask{})
}

func (g *Generator) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}

// UpdateFutureActionTimes computes and stores the next scheduled action times.
func (g *Generator) UpdateFutureActionTimes(
	ctx chasm.Context,
	specBuilder *scheduler.SpecBuilder,
) {
	sched := g.Scheduler.Get(ctx)
	spec, err := sched.getCompiledSpec(specBuilder)
	if err != nil {
		return
	}

	count := recentActionCount
	if sched.Schedule.State.LimitedActions {
		count = min(int(sched.Schedule.State.RemainingActions), recentActionCount)
	}

	futureTimes := make([]*timestamppb.Timestamp, 0, count)
	// Start from max(now, updateTime) to ensure we skip times before the last update.
	t := ctx.Now(g)
	if updateTime := sched.Info.GetUpdateTime().AsTime(); updateTime.After(t) {
		t = updateTime
	}
	for len(futureTimes) < count {
		t = spec.GetNextTime(sched.jitterSeed(), t).Next
		if t.IsZero() {
			break
		}
		futureTimes = append(futureTimes, timestamppb.New(t))
	}

	g.FutureActionTimes = futureTimes
}
