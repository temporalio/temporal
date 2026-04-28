package nexusendpoint

import (
	"go.temporal.io/server/tests/model"
	"go.temporal.io/server/tests/testcore/umpire"
)

// Deps wires the nexus endpoint component to a test cluster. The embedded
// model.Deps carries cluster-wide dependencies (Umpire, Context, gRPC
// clients, default Namespace, Prefix).
type Deps struct {
	model.Deps
	Store        *umpire.EntityStore[*Entity]
	MaxEndpoints int
}

// New builds the umpire.Component for the nexus endpoint lifecycle.
func New(deps Deps) *umpire.Component {
	driver := newDriver(deps)
	observer := newObserver(deps)
	return &umpire.Component{
		Name:            "nexus-endpoint",
		Behavior:        driver,
		Observer:        observer.Strategy(),
		ObservedMethods: observer.ObservedMethods(),
		Rules:           &rules,
		RPCs: []umpire.RPCSpec{
			{Method: methodCreate},
			{Method: methodDelete},
		},
	}
}
