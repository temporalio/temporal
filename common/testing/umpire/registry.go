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

// Registry manages entities and routes facts to them.
type Registry struct {
	mu         sync.RWMutex
	factories  map[string]EntityFactory
	facts      map[string]bool
	entities   map[string]*entityRecord
	generation atomic.Uint64
}

// entityRecord wraps an entity with its change generation and scoping root.
type entityRecord struct {
	entity     Entity
	generation uint64   // bumped each time RouteFacts delivers facts to this entity
	root       EntityID // outermost ancestor (e.g. the namespace); used to scope Check/Purge
}

// NewRegistry creates a new in-memory entity registry.
func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[string]EntityFactory),
		facts:     make(map[string]bool),
		entities:  make(map[string]*entityRecord),
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

// RouteFacts routes a batch of facts to the appropriate entities.
func (r *Registry) RouteFacts(ctx context.Context, facts []Fact) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	factsByKey := make(map[string][]Fact)
	recordByKey := make(map[string]*entityRecord)
	pathByKey := make(map[string]*EntityPath)

	for _, f := range facts {
		// Broadcast facts are delivered to all existing entities of a given type.
		if bf, ok := f.(BroadcastFact); ok {
			et := bf.BroadcastType()
			for key, rec := range r.entities {
				if rec.entity.Type() != et {
					continue
				}
				factsByKey[key] = append(factsByKey[key], f)
				recordByKey[key] = rec
				// pathByKey not set for broadcast — OnFact receives a nil path.
			}
			continue
		}
		path := f.TargetEntity()
		rec, err := r.getOrCreateRecord(path)
		if err != nil {
			continue
		}
		if rec == nil {
			continue
		}
		key := entityPathKey(path)
		factsByKey[key] = append(factsByKey[key], f)
		recordByKey[key] = rec
		pathByKey[key] = path
	}

	var errs []error
	for key, fs := range factsByKey {
		rec := recordByKey[key]
		path := pathByKey[key]
		if err := rec.entity.OnFact(ctx, path, slices.Values(fs)); err != nil {
			errs = append(errs, fmt.Errorf("entity %s: %w", key, err))
		}
		// Bump generation so rules know this entity changed.
		rec.generation = r.generation.Add(1)
	}

	return errors.Join(errs...)
}

// getOrCreateRecord finds or creates an entity record for the given path,
// creating any ancestor records that themselves have a registered factory.
// Ancestor types without a factory (e.g. the namespace root) are keying-only:
// they appear in the key and scoping root but hold no entity record.
// Must be called with r.mu held.
func (r *Registry) getOrCreateRecord(path *EntityPath) (*entityRecord, error) {
	if path == nil {
		return nil, nil
	}

	key := entityPathKey(path)
	if rec, ok := r.entities[key]; ok {
		return rec, nil
	}

	if parent := path.Parent(); parent != nil {
		if _, ok := r.factories[string(parent.EntityID.Type)]; ok {
			if _, err := r.getOrCreateRecord(parent); err != nil {
				return nil, fmt.Errorf("create parent entity: %w", err)
			}
		}
	}

	factory, ok := r.factories[string(path.EntityID.Type)]
	if !ok {
		return nil, fmt.Errorf("no factory for entity type %s", path.EntityID.Type)
	}

	rec := &entityRecord{entity: factory(), root: path.Root()}
	r.entities[key] = rec

	return rec, nil
}

// Generation returns the current global generation counter.
func (r *Registry) Generation() uint64 {
	return r.generation.Load()
}

// QueryEntities returns all entities of the given type with their registry key.
// If sinceGeneration > 0, only entities changed after that generation are returned.
// If scope is non-nil, only entities rooted at that ancestor are returned.
func (r *Registry) QueryEntities(et EntityType, sinceGeneration uint64, scope *EntityID) []EntityEntry {
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
		if scope != nil && rec.root != *scope {
			continue
		}
		result = append(result, EntityEntry{Key: key, Entity: rec.entity})
	}
	return result
}

// PurgeScope removes every entity rooted at the given ancestor and returns the
// number removed. Use it to drop all data collected for one namespace.
func (r *Registry) PurgeScope(root EntityID) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	var removed int
	for key, rec := range r.entities {
		if rec.root == root {
			delete(r.entities, key)
			removed++
		}
	}
	return removed
}

// EntityEntry pairs an entity with its registry key.
type EntityEntry struct {
	Key    string
	Entity Entity
}

// EntityPathKey returns the canonical registry key for an entity path,
// ordered root-first (e.g. "namespace:ns@Workflow:wf@WorkflowUpdate:upd").
func EntityPathKey(path *EntityPath) string {
	return entityPathKey(path)
}

func entityPathKey(path *EntityPath) string {
	if path == nil {
		return ""
	}
	key := ""
	for _, a := range path.Ancestors {
		key += fmt.Sprintf("%s:%s@", a.Type, a.ID)
	}
	return key + fmt.Sprintf("%s:%s", path.EntityID.Type, path.EntityID.ID)
}
