package scheduler

import (
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
)

// The Generator component is responsible for buffering actions according
// to the schedule's specification. Manually requested actions (from an immediate
// request or backfill) are separately handled in the Backfiller component.
type Generator struct {
	chasm.UnimplementedComponent

	*schedulespb.GeneratorInternal

	Scheduler chasm.Field[*Scheduler]
	Invoker   chasm.Field[*Invoker]
}

// NewGenerator returns an intialized Generator component, which should
// be parented under a Scheduler root node.
func NewGenerator(ctx chasm.MutableContext, scheduler *Scheduler, invoker *Invoker) *Generator {
	return &Generator{
		GeneratorInternal: &schedulespb.GeneratorInternal{
			LastProcessedTime: nil,
		},
		Scheduler: chasm.ComponentPointerTo(ctx, scheduler),
		Invoker:   chasm.ComponentPointerTo(ctx, invoker),
	}
}

func (g *Generator) LifecycleState(ctx chasm.Context) chasm.LifecycleState {
	return chasm.LifecycleStateRunning
}
