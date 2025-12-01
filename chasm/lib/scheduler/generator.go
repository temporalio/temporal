package scheduler

import (
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

	// Kick off initial generator run.
	ctx.AddTask(generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})

	return generator
}

// Generate immediately kicks off a new GeneratorTask. Used after updating the
// schedule specification.
func (g *Generator) Generate(ctx chasm.MutableContext) {
	ctx.AddTask(g, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
}

func (g *Generator) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}
