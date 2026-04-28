package taskqueue

import (
	"go.temporal.io/server/tests/model"
	"go.temporal.io/server/tests/testcore/umpire"
)

// Deps wires the task queue component to a test cluster. The embedded
// model.Deps carries cluster-wide dependencies.
type Deps struct {
	model.Deps
	Store *umpire.EntityStore[*Entity]
}

// New builds the umpire.Component for task queue verification + implicit
// observation.
func New(deps Deps) *umpire.Component {
	driver := newDriver(deps)
	observer := newObserver(deps)
	return &umpire.Component{
		Name:            "task-queue",
		Behavior:        driver,
		Observer:        observer.Strategy(),
		ObservedMethods: observer.ObservedMethods(),
		Rules:           &rules,
		RPCs:            rpcs,
	}
}
