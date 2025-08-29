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

	Scheduler chasm.Field[*Scheduler]
}

// NewGenerator returns an intialized Generator component, which should
// be parented under a Scheduler root node.
func NewGenerator(ctx chasm.MutableContext, scheduler *Scheduler, invoker *Invoker) *Generator {
	return &Generator{
		GeneratorState: &schedulerpb.GeneratorState{
			LastProcessedTime: nil,
		},
		Scheduler: chasm.ComponentPointerTo(ctx, scheduler),
	}
}

func (g *Generator) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}
