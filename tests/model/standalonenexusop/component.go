package standalonenexusop

import (
	"go.temporal.io/server/tests/model"
	"go.temporal.io/server/tests/testcore/umpire"
)

// Deps wires the standalone Nexus operation component to a test cluster.
// The embedded model.Deps carries cluster-wide dependencies; the fields
// below are component-specific.
type Deps struct {
	model.Deps
	Store         *umpire.EntityStore[*Entity]
	EndpointName  string
	TaskQueue     string
	MaxOperations int
}

// New builds the umpire.Component for the standalone Nexus operation
// lifecycle.
func New(deps Deps) *umpire.Component {
	driver := newDriver(deps)
	observer := newObserver(deps)
	return &umpire.Component{
		Name:            "standalone-nexus-op",
		Behavior:        driver,
		Observer:        observer.Strategy(),
		ObservedMethods: observer.ObservedMethods(),
		Rules:           &rules,
		RPCs:            rpcs,
	}
}
