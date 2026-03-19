package umpire

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
)

// Registry manages entities and routes events to them.
type Registry struct {
	mu         sync.RWMutex
	factories  map[string]EntityFactory
	facts      map[string]bool
	entities   map[string]*entityRecord
	children   map[string][]string
	generation atomic.Uint64
}

// entityRecord wraps an entity with its change generation.
type entityRecord struct {
	entity     Entity
	generation uint64 // bumped each time RouteFacts delivers facts to this entity
}

// NewRegistry creates a new in-memory entity registry.
func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[string]EntityFactory),
		facts:     make(map[string]bool),
		entities:  make(map[string]*entityRecord),
		children:  make(map[string][]string),
	}
}

// RegisterEntity registers an entity type with its factory.
// Panics if the factory's entity Type() does not match the struct name.
func (r *Registry) RegisterEntity(factory EntityFactory, subscribesTo ...Fact) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Verify that entity Type() matches the concrete struct name.
	probe := factory()
	probeName := reflect.TypeOf(probe).Elem().Name()
	if string(probe.Type()) != probeName {
		panic(fmt.Sprintf(
			"RegisterEntity: %s.Type() returned %q, expected %q",
			probeName, probe.Type(), probeName,
		))
	}

	r.factories[string(probe.Type())] = factory
}

// RegisterFact validates and registers fact types.
// Panics if Name() does not match the struct name.
func (r *Registry) RegisterFact(probes ...Fact) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, probe := range probes {
		name := probe.Name()
		typeName := reflect.TypeOf(probe).Elem().Name()
		if name != typeName {
			panic(fmt.Sprintf(
				"RegisterFact: %s.Name() returned %q, expected %q (must match struct name)",
				typeName, name, typeName,
			))
		}
		r.facts[name] = true
	}
}

// RouteFacts routes a batch of events to the appropriate entities.
func (r *Registry) RouteFacts(ctx context.Context, facts []Fact) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	eventsByKey := make(map[string][]Fact)
	recordByKey := make(map[string]*entityRecord)
	identByKey := make(map[string]*Identity)

	for _, ev := range facts {
		// Broadcast facts are delivered to all existing entities of a given type.
		if bf, ok := ev.(BroadcastFact); ok {
			et := bf.BroadcastType()
			for key, rec := range r.entities {
				if rec.entity.Type() != et {
					continue
				}
				eventsByKey[key] = append(eventsByKey[key], ev)
				recordByKey[key] = rec
				// identByKey not set for broadcast — OnFact receives nil identity.
			}
			continue
		}
		ident := ev.TargetEntity()
		rec, err := r.getOrCreateRecord(ident)
		if err != nil {
			continue
		}
		if rec == nil {
			continue
		}
		key := identityKey(ident)
		eventsByKey[key] = append(eventsByKey[key], ev)
		recordByKey[key] = rec
		identByKey[key] = ident
	}

	var errs []error
	for key, evs := range eventsByKey {
		rec := recordByKey[key]
		ident := identByKey[key]
		if err := rec.entity.OnFact(ctx, ident, slices.Values(evs)); err != nil {
			errs = append(errs, fmt.Errorf("entity %s: %w", key, err))
		}
		// Bump generation so rules know this entity changed.
		rec.generation = r.generation.Add(1)
	}

	return errors.Join(errs...)
}

// getOrCreateRecord finds or creates an entity record for the given identity.
// Must be called with r.mu held.
func (r *Registry) getOrCreateRecord(ident *Identity) (*entityRecord, error) {
	if ident == nil {
		return nil, nil
	}

	key := identityKey(ident)
	if rec, ok := r.entities[key]; ok {
		return rec, nil
	}

	if ident.ParentID != nil {
		parentIdent := &Identity{EntityID: *ident.ParentID}
		if _, err := r.getOrCreateRecord(parentIdent); err != nil {
			return nil, fmt.Errorf("create parent entity: %w", err)
		}
	}

	factory, ok := r.factories[string(ident.EntityID.Type)]
	if !ok {
		return nil, fmt.Errorf("no factory for entity type %s", ident.EntityID.Type)
	}

	rec := &entityRecord{entity: factory()}
	r.entities[key] = rec

	if ident.ParentID != nil {
		pKey := entityIDKey(ident.ParentID)
		r.children[pKey] = append(r.children[pKey], key)
	}

	return rec, nil
}

// Generation returns the current global generation counter.
func (r *Registry) Generation() uint64 {
	return r.generation.Load()
}

// QueryEntities returns all entities of the given type with their entity ID.
// If sinceGeneration > 0, only entities changed after that generation are returned.
func (r *Registry) QueryEntities(et EntityType, sinceGeneration uint64) []EntityEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var result []EntityEntry
	for key, rec := range r.entities {
		if rec.entity.Type() != et {
			continue
		}
		if sinceGeneration > 0 && rec.generation <= sinceGeneration {
			continue
		}
		result = append(result, EntityEntry{Key: key, Entity: rec.entity})
	}
	return result
}

// EntityEntry pairs an entity with its registry key.
type EntityEntry struct {
	Key    string
	Entity Entity
}

// IdentityKey returns the canonical registry key for an entity identity.
func IdentityKey(ident *Identity) string {
	return identityKey(ident)
}

func identityKey(ident *Identity) string {
	if ident == nil {
		return ""
	}
	key := fmt.Sprintf("%s:%s", ident.EntityID.Type, ident.EntityID.ID)
	if ident.ParentID != nil {
		key = fmt.Sprintf("%s@%s:%s", key, ident.ParentID.Type, ident.ParentID.ID)
	}
	return key
}

func entityIDKey(id *EntityID) string {
	if id == nil {
		return ""
	}
	return fmt.Sprintf("%s:%s", id.Type, id.ID)
}
