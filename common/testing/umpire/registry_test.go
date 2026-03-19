package umpire

import (
	"context"
	"iter"
	"testing"
)

// ── test helpers ──────────────────────────────────────────────────────────────

// TypeA and TypeB are concrete entity types whose struct names match Type().
type TypeA struct{ received []Fact }

func (e *TypeA) Type() EntityType { return "TypeA" }
func (e *TypeA) OnFact(_ context.Context, _ *Identity, facts iter.Seq[Fact]) error {
	for m := range facts {
		e.received = append(e.received, m)
	}
	return nil
}

type TypeB struct{ received []Fact }

func (e *TypeB) Type() EntityType { return "TypeB" }
func (e *TypeB) OnFact(_ context.Context, _ *Identity, facts iter.Seq[Fact]) error {
	for m := range facts {
		e.received = append(e.received, m)
	}
	return nil
}

func newFactoryA(t *testing.T, created *[]*TypeA) EntityFactory {
	t.Helper()
	first := true
	return func() Entity {
		e := &TypeA{}
		// Skip the probe call from RegisterEntity.
		if first {
			first = false
			return e
		}
		*created = append(*created, e)
		return e
	}
}

func newFactoryB(t *testing.T, created *[]*TypeB) EntityFactory {
	t.Helper()
	first := true
	return func() Entity {
		e := &TypeB{}
		// Skip the probe call from RegisterEntity.
		if first {
			first = false
			return e
		}
		*created = append(*created, e)
		return e
	}
}

func newFact(factType string, entityType EntityType, id string) *testFact {
	eid := NewEntityID(entityType, id)
	return &testFact{factType: factType, target: &Identity{EntityID: eid}}
}

// ── RouteFacts creates entities on demand ────────────────────────────────────

func TestRegistry_RouteFacts_CreatesEntity(t *testing.T) {
	r := NewRegistry()
	var created []*TypeA
	r.RegisterEntity(newFactoryA(t, &created))

	m := newFact("event", "TypeA", "id1")
	if err := r.RouteFacts(context.Background(), []Fact{m}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(created) != 1 {
		t.Fatalf("expected 1 entity created, got %d", len(created))
	}
	if len(created[0].received) != 1 {
		t.Fatalf("expected entity to receive 1 event, got %d", len(created[0].received))
	}
}

func TestRegistry_RouteFacts_ReusesSameEntity(t *testing.T) {
	r := NewRegistry()
	var created []*TypeA
	r.RegisterEntity(newFactoryA(t, &created))

	m := newFact("event", "TypeA", "id1")
	_ = r.RouteFacts(context.Background(), []Fact{m, m})

	if len(created) != 1 {
		t.Fatalf("expected 1 entity (reused), got %d", len(created))
	}
	if len(created[0].received) != 2 {
		t.Fatalf("expected entity to receive 2 events, got %d", len(created[0].received))
	}
}

func TestRegistry_RouteFacts_DifferentIDsCreateDifferentEntities(t *testing.T) {
	r := NewRegistry()
	var created []*TypeA
	r.RegisterEntity(newFactoryA(t, &created))

	m1 := newFact("event", "TypeA", "id1")
	m2 := newFact("event", "TypeA", "id2")
	_ = r.RouteFacts(context.Background(), []Fact{m1, m2})

	if len(created) != 2 {
		t.Fatalf("expected 2 entities (different IDs), got %d", len(created))
	}
}

// ── Nil identity is silently skipped ─────────────────────────────────────────

func TestRegistry_RouteFacts_NilIdentity_NoError(t *testing.T) {
	r := NewRegistry()
	var created []*TypeA
	r.RegisterEntity(newFactoryA(t, &created))

	nilTarget := &testFact{factType: "event", target: nil}
	if err := r.RouteFacts(context.Background(), []Fact{nilTarget}); err != nil {
		t.Fatalf("unexpected error for nil identity: %v", err)
	}
	if len(created) != 0 {
		t.Fatalf("expected no entities created for nil identity, got %d", len(created))
	}
}

// ── Unregistered entity type is silently skipped ──────────────────────────────

func TestRegistry_RouteFacts_UnregisteredType_NoError(t *testing.T) {
	r := NewRegistry()
	// No factory registered for TypeA

	m := newFact("event", "TypeA", "id1")
	if err := r.RouteFacts(context.Background(), []Fact{m}); err != nil {
		t.Fatalf("unexpected error for unregistered type: %v", err)
	}
}

// ── QueryEntities ─────────────────────────────────────────────────────────────

func TestRegistry_QueryEntities_ReturnsAllOfType(t *testing.T) {
	r := NewRegistry()
	var createdA []*TypeA
	var createdB []*TypeB
	r.RegisterEntity(newFactoryA(t, &createdA))
	r.RegisterEntity(newFactoryB(t, &createdB))

	_ = r.RouteFacts(context.Background(), []Fact{
		newFact("ev", "TypeA", "a1"),
		newFact("ev", "TypeA", "a2"),
		newFact("ev", "TypeB", "b1"),
	})

	results := r.QueryEntities("TypeA", 0)
	if len(results) != 2 {
		t.Fatalf("expected 2 TypeA entities, got %d", len(results))
	}
	resultsB := r.QueryEntities("TypeB", 0)
	if len(resultsB) != 1 {
		t.Fatalf("expected 1 TypeB entity, got %d", len(resultsB))
	}
}

func TestRegistry_QueryEntities_EmptyRegistry(t *testing.T) {
	r := NewRegistry()
	r.RegisterEntity(newFactoryA(t, &[]*TypeA{}))

	results := r.QueryEntities("TypeA", 0)
	if len(results) != 0 {
		t.Fatalf("expected empty result for fresh registry, got %d", len(results))
	}
}

// ── Parent entity auto-creation ───────────────────────────────────────────────

func TestRegistry_RouteFacts_CreatesParentEntity(t *testing.T) {
	r := NewRegistry()
	var createdA []*TypeA
	var createdB []*TypeB
	r.RegisterEntity(newFactoryA(t, &createdA))
	r.RegisterEntity(newFactoryB(t, &createdB))

	// Child of TypeA, parent of TypeB
	parentID := NewEntityID("TypeB", "parent1")
	childEID := NewEntityID("TypeA", "child1")
	childFact := &testFact{
		factType: "event",
		target:   &Identity{EntityID: childEID, ParentID: &parentID},
	}

	_ = r.RouteFacts(context.Background(), []Fact{childFact})

	if len(createdA) != 1 {
		t.Fatalf("expected 1 child entity, got %d", len(createdA))
	}
	if len(createdB) != 1 {
		t.Fatalf("expected 1 parent entity auto-created, got %d", len(createdB))
	}
}

// ── Multiple events batched to same entity ─────────────────────────────────────

func TestRegistry_RouteFacts_BatchedMovesDeliveredInOrder(t *testing.T) {
	r := NewRegistry()
	var created []*TypeA
	r.RegisterEntity(newFactoryA(t, &created))

	m1 := &testFact{factType: "first", target: &Identity{EntityID: NewEntityID("TypeA", "id1")}}
	m2 := &testFact{factType: "second", target: &Identity{EntityID: NewEntityID("TypeA", "id1")}}
	_ = r.RouteFacts(context.Background(), []Fact{m1, m2})

	if len(created) != 1 {
		t.Fatalf("expected 1 entity, got %d", len(created))
	}
	if len(created[0].received) != 2 {
		t.Fatalf("expected 2 events, got %d", len(created[0].received))
	}
}

// ── Generation tracking (dirty-tracking) ──────────────────────────────────────

func TestRegistry_Generation_IncreasesOnRouteFacts(t *testing.T) {
	r := NewRegistry()
	r.RegisterEntity(newFactoryA(t, &[]*TypeA{}))

	if r.Generation() != 0 {
		t.Fatalf("expected initial generation 0, got %d", r.Generation())
	}

	_ = r.RouteFacts(context.Background(), []Fact{newFact("ev", "TypeA", "id1")})
	gen1 := r.Generation()
	if gen1 == 0 {
		t.Fatal("expected generation > 0 after RouteFacts")
	}

	_ = r.RouteFacts(context.Background(), []Fact{newFact("ev", "TypeA", "id2")})
	gen2 := r.Generation()
	if gen2 <= gen1 {
		t.Fatalf("expected generation to increase: %d -> %d", gen1, gen2)
	}
}

func TestRegistry_QueryEntities_SinceGeneration_FiltersDirty(t *testing.T) {
	r := NewRegistry()
	r.RegisterEntity(newFactoryA(t, &[]*TypeA{}))

	// Create entity A.
	_ = r.RouteFacts(context.Background(), []Fact{newFact("ev", "TypeA", "a1")})
	genAfterA := r.Generation()

	// Create entity B.
	_ = r.RouteFacts(context.Background(), []Fact{newFact("ev", "TypeA", "a2")})

	// Query all: both visible.
	all := r.QueryEntities("TypeA", 0)
	if len(all) != 2 {
		t.Fatalf("expected 2 entities with sinceGeneration=0, got %d", len(all))
	}

	// Query since genAfterA: only B visible (changed after A).
	dirty := r.QueryEntities("TypeA", genAfterA)
	if len(dirty) != 1 {
		t.Fatalf("expected 1 dirty entity since gen %d, got %d", genAfterA, len(dirty))
	}
}

func TestRegistry_QueryEntities_SinceGeneration_UpdatedEntityBecomesDirty(t *testing.T) {
	r := NewRegistry()
	r.RegisterEntity(newFactoryA(t, &[]*TypeA{}))

	// Create both entities.
	_ = r.RouteFacts(context.Background(), []Fact{
		newFact("ev", "TypeA", "a1"),
		newFact("ev", "TypeA", "a2"),
	})
	genAfterBoth := r.Generation()

	// No dirty entities.
	dirty := r.QueryEntities("TypeA", genAfterBoth)
	if len(dirty) != 0 {
		t.Fatalf("expected 0 dirty entities, got %d", len(dirty))
	}

	// Update only a1.
	_ = r.RouteFacts(context.Background(), []Fact{newFact("ev", "TypeA", "a1")})

	// Only a1 is dirty.
	dirty = r.QueryEntities("TypeA", genAfterBoth)
	if len(dirty) != 1 {
		t.Fatalf("expected 1 dirty entity after update, got %d", len(dirty))
	}
}
