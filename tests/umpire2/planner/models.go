package planner

import (
	"fmt"

	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/model"
)

// Models is the authoring surface: a catalog of every entity model the planner can
// plan over, keyed by entity type. A developer names targets fully-qualified by
// entity ("Workflow", "completed") instead of first fetching a Lifecycle by
// hand — which also disambiguates state names shared by multiple entities.
//
// Planning is structural (over the model graph, not any live instance), so one
// representative Lifecycle per entity type is all the catalog needs.
type Models struct {
	byType map[string]*umpire.Lifecycle
}

// NewModels returns an empty catalog; prefer DefaultModels for the standard set.
func NewModels() *Models {
	return &Models{byType: map[string]*umpire.Lifecycle{}}
}

// DefaultModels is the catalog of the default entity models. It derives from the
// same model.DefaultEntities() the monitor registers — the single source of truth —
// so the two sides can never drift. Only entities with a Lifecycle (the ones the
// planner can route over) are included; non-modelled entities (e.g. TaskQueue) are
// skipped. This is the entry point a test starts from.
func DefaultModels() *Models {
	m := NewModels()
	for _, e := range model.DefaultEntities() {
		if lc, ok := e.New().(umpire.Lifecycled); ok {
			m.Add(lc)
		}
	}
	return m
}

// Add registers a Lifecycled entity under its own Type(), so the catalog keys are
// the canonical entity type names.
func (m *Models) Add(e umpire.Lifecycled) *Models {
	m.byType[string(e.Type())] = e.Lifecycle()
	return m
}

// Lifecycle returns the model for an entity type, or false if it is not registered.
func (m *Models) Lifecycle(entityType string) (*umpire.Lifecycle, bool) {
	lc, ok := m.byType[entityType]
	return lc, ok
}

// PlanTo plans a route to a fully-qualified target state (entityType, target).
func (m *Models) PlanTo(entityType, target string, mode RouteMode, c Constraints, opts ...Option) (*Plan, error) {
	lc, ok := m.byType[entityType]
	if !ok {
		return nil, fmt.Errorf("planner: unknown entity model %q", entityType)
	}
	return PlanTo(lc, target, mode, c, opts...)
}

// Explore roams a constrained sub-graph of the named entity's model.
func (m *Models) Explore(entityType string, c Constraints, opts ...Option) (*Plan, error) {
	lc, ok := m.byType[entityType]
	if !ok {
		return nil, fmt.Errorf("planner: unknown entity model %q", entityType)
	}
	return Explore(lc, c, opts...), nil
}
