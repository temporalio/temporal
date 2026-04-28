package world

// Package world holds the cross-cutting registry of every domain entity the
// property test exercises. Each per-entity package owns its own type; this
// struct wires those types together so the test constructs one World and
// shares it across model components and observers.

import (
	"go.temporal.io/server/tests/model/namespace"
	"go.temporal.io/server/tests/model/nexusendpoint"
	"go.temporal.io/server/tests/model/standalonenexusop"
	"go.temporal.io/server/tests/model/taskqueue"
	"go.temporal.io/server/tests/testcore/umpire"
)

// World is the typed entity-store registry shared by every component.
type World struct {
	Namespaces *umpire.EntityStore[*namespace.Entity]
	TaskQueues *umpire.EntityStore[*taskqueue.Entity]
	Endpoints  *umpire.EntityStore[*nexusendpoint.Entity]
	Operations *umpire.EntityStore[*standalonenexusop.Entity]
}

// NewWorld constructs a World with empty stores for every supported entity
// type.
func New() *World {
	return &World{
		Namespaces: umpire.NewEntityStore(func(n *namespace.Entity) string {
			return n.Name
		}),
		TaskQueues: umpire.NewEntityStore(func(q *taskqueue.Entity) string {
			// (namespace, name) is the unique key — same name across
			// namespaces is allowed.
			return q.Namespace + "/" + q.Name
		}),
		Endpoints: umpire.NewEntityStore(func(e *nexusendpoint.Entity) string {
			return e.Name
		}),
		Operations: umpire.NewEntityStore(func(o *standalonenexusop.Entity) string {
			// Operation IDs are scoped per namespace.
			return o.Namespace + "/" + o.OperationID
		}),
	}
}

// Reset clears every entity store in place. The World's store pointers
// stay valid, so interceptors and observers that captured them at startup
// continue to work across iterations.
func (w *World) Reset() {
	w.Namespaces.Reset()
	w.TaskQueues.Reset()
	w.Endpoints.Reset()
	w.Operations.Reset()
}
