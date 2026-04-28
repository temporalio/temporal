package umpire

import "sort"

// EntityStore is a typed registry of domain entities, keyed by string. It is
// designed to back per-entity-type collections in a property-test World:
//
//	type World struct {
//	    Namespaces *umpire.EntityStore[*Namespace]
//	    Operations *umpire.EntityStore[*NexusOperation]
//	}
//
// All iteration helpers return entries in a deterministic order (sorted by
// the entity's key) so action draws stay reproducible across rapid runs
// with the same seed.
type EntityStore[E any] struct {
	keyFn func(E) string
	items map[string]E
}

// NewEntityStore creates an EntityStore that derives each entity's key via
// keyFn.
func NewEntityStore[E any](keyFn func(E) string) *EntityStore[E] {
	return &EntityStore[E]{keyFn: keyFn, items: make(map[string]E)}
}

// Add inserts or replaces the entity at its key.
func (s *EntityStore[E]) Add(e E) {
	s.items[s.keyFn(e)] = e
}

// Reset clears every entry. The store pointer stays the same, so other
// callers that captured a *EntityStore reference (drivers, observers) keep
// working — they just see an empty store.
func (s *EntityStore[E]) Reset() {
	s.items = make(map[string]E)
}

// Get returns the entity at key, if present.
func (s *EntityStore[E]) Get(key string) (E, bool) {
	e, ok := s.items[key]
	return e, ok
}

// Remove deletes the entity at key. No-op if absent.
func (s *EntityStore[E]) Remove(key string) {
	delete(s.items, key)
}

// Len returns the number of entities currently stored.
func (s *EntityStore[E]) Len() int {
	return len(s.items)
}

// All returns every entity in deterministic key order.
func (s *EntityStore[E]) All() []E {
	out := make([]E, 0, len(s.items))
	for _, e := range s.items {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool {
		return s.keyFn(out[i]) < s.keyFn(out[j])
	})
	return out
}

// Filter returns every entity matching pred, in deterministic key order.
func (s *EntityStore[E]) Filter(pred func(E) bool) []E {
	var out []E
	for _, e := range s.items {
		if pred(e) {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return s.keyFn(out[i]) < s.keyFn(out[j])
	})
	return out
}

// Any reports whether at least one entity matches pred. Useful as an action
// precondition without allocating the filter result.
func (s *EntityStore[E]) Any(pred func(E) bool) bool {
	for _, e := range s.items {
		if pred(e) {
			return true
		}
	}
	return false
}

// Pick draws an entity matching pred, skipping the action step if none
// match. The draw is deterministic across rapid runs with the same seed.
func (s *EntityStore[E]) Pick(t *T, label string, pred func(E) bool) E {
	t.Helper()
	matches := s.Filter(pred)
	if len(matches) == 0 {
		var zero E
		t.Skip("no match for " + label)
		return zero
	}
	return Draw(t, label, matches)
}

// EntityCreated is a generic lifecycle fact. Models record one whenever they
// add an entity to the world; rules can scan history for these to reason
// about creation order without needing a typed fact per entity. Parents
// snapshots the entity's parent foreign keys at creation time so rules can
// reconstruct the dependency graph.
type EntityCreated struct {
	Type     string
	EntityID string
	Parents  map[string]string
}

func (e *EntityCreated) Key() string { return e.Type + ":" + e.EntityID }

// EntityDeleted is the dual of EntityCreated: a generic fact emitted when an
// entity is removed from the world.
type EntityDeleted struct {
	Type     string
	EntityID string
}

func (e *EntityDeleted) Key() string { return e.Type + ":" + e.EntityID }
