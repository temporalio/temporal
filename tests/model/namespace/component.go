package namespace

import (
	"go.temporal.io/server/tests/model"
	"go.temporal.io/server/tests/testcore/umpire"
)

// Deps wires the namespace component to a test cluster. The embedded
// model.Deps carries cluster-wide dependencies (Umpire, Context, gRPC
// clients, default Namespace, Prefix); the per-component fields below are
// specific to namespace.
type Deps struct {
	model.Deps
	Store         *umpire.EntityStore[*Entity]
	MaxNamespaces int
}

// New builds the umpire.Component for the namespace lifecycle.
func New(deps Deps) *umpire.Component {
	driver := newDriver(deps)
	observer := newObserver(deps)
	return &umpire.Component{
		Name:            "namespace",
		Behavior:        driver,
		Observer:        observer.Strategy(),
		ObservedMethods: observer.ObservedMethods(),
		Rules:           &rules,
		// Placeholder catalog entry; field-level specs can be filled in when
		// the model exercises mutations or invalid-input cases for this RPC.
		RPCs: []umpire.RPCSpec{
			{Method: methodRegister},
		},
	}
}
