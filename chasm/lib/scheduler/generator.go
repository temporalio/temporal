package scheduler

import (
	"time"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
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
	ctx.AddTask(g, chasm.TaskAttributes{
		ScheduledTime: chasm.TaskScheduledTimeImmediate,
	}, &schedulerpb.GeneratorTask{
		TaskVersion: g.TaskVersion,
	})
}

// scheduleTask schedules a GeneratorTask at the given time.
func (g *Generator) scheduleTask(ctx chasm.MutableContext, scheduledTime time.Time) {
	ctx.AddTask(g, chasm.TaskAttributes{
		ScheduledTime: scheduledTime,
	}, &schedulerpb.GeneratorTask{
		TaskVersion: g.TaskVersion,
	})
}

// incrementTaskVersion advances the component's task version after execution.
// Called after executing an immediate task to invalidate any stale duplicate tasks.
func (g *Generator) incrementTaskVersion() {
	g.TaskVersion++
}

func (g *Generator) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}
