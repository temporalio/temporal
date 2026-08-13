package umpire

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sync"
)

// RelationType is the stable semantic name of a relationship between entity types.
type RelationType string

// RelationCardinality limits how many edges may occupy one side of a relation.
type RelationCardinality uint8

const (
	RelationMany RelationCardinality = iota
	RelationOne
)

// RelationSchema declares the endpoint types and cardinality of a relation.
type RelationSchema struct {
	Type              RelationType
	Source            EntityType
	Target            EntityType
	SourceCardinality RelationCardinality
	TargetCardinality RelationCardinality
}

// RelationEdge is one typed relationship between two entity identities.
type RelationEdge struct {
	Type   RelationType
	Scope  EntityID
	Source EntityID
	Target EntityID
}

var (
	ErrRelationSchema      = errors.New("invalid relation schema")
	ErrRelationEndpoint    = errors.New("invalid relation endpoint")
	ErrRelationCardinality = errors.New("relation cardinality exceeded")
)

// RelationError describes a rejected schema or edge mutation.
type RelationError struct {
	Type   RelationType
	Scope  EntityID
	Source EntityID
	Target EntityID
	Err    error
	Reason string
}

func (e *RelationError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Source.Type == "" && e.Target.Type == "" {
		return fmt.Sprintf("relation %q: %s: %v", e.Type, e.Reason, e.Err)
	}
	return fmt.Sprintf("relation %q %s -> %s: %s: %v", e.Type, e.Source, e.Target, e.Reason, e.Err)
}

func (e *RelationError) Unwrap() error { return e.Err }

// RelationStore owns validated relation schemas and atomically indexed runtime edges.
type RelationStore struct {
	mu      sync.RWMutex
	schemas map[RelationType]RelationSchema
	forward map[RelationType]map[EntityID]map[EntityID]struct{}
	reverse map[RelationType]map[EntityID]map[EntityID]struct{}
	scopes  map[RelationType]map[EntityID]map[EntityID]EntityID
}

// NewRelationStore validates schemas and returns an empty relation store.
func NewRelationStore(schemas ...RelationSchema) (*RelationStore, error) {
	store := &RelationStore{
		schemas: make(map[RelationType]RelationSchema, len(schemas)),
		forward: make(map[RelationType]map[EntityID]map[EntityID]struct{}, len(schemas)),
		reverse: make(map[RelationType]map[EntityID]map[EntityID]struct{}, len(schemas)),
		scopes:  make(map[RelationType]map[EntityID]map[EntityID]EntityID, len(schemas)),
	}
	for _, schema := range schemas {
		if err := validateRelationSchema(schema); err != nil {
			return nil, err
		}
		if _, exists := store.schemas[schema.Type]; exists {
			return nil, &RelationError{Type: schema.Type, Err: ErrRelationSchema, Reason: "duplicate type"}
		}
		store.schemas[schema.Type] = schema
		store.forward[schema.Type] = map[EntityID]map[EntityID]struct{}{}
		store.reverse[schema.Type] = map[EntityID]map[EntityID]struct{}{}
		store.scopes[schema.Type] = map[EntityID]map[EntityID]EntityID{}
	}
	return store, nil
}

func validateRelationSchema(schema RelationSchema) error {
	var reason string
	switch {
	case schema.Type == "":
		reason = "type is empty"
	case schema.Source == "":
		reason = "source type is empty"
	case schema.Target == "":
		reason = "target type is empty"
	case schema.SourceCardinality != RelationMany && schema.SourceCardinality != RelationOne:
		reason = "source cardinality is invalid"
	case schema.TargetCardinality != RelationMany && schema.TargetCardinality != RelationOne:
		reason = "target cardinality is invalid"
	default:
		return nil
	}
	return &RelationError{Type: schema.Type, Err: ErrRelationSchema, Reason: reason}
}

// Add atomically inserts an edge. It reports false for an already-present edge.
func (s *RelationStore) Add(edge RelationEdge) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	schema, err := s.validateEdge(edge)
	if err != nil {
		return false, err
	}
	targets := s.forward[edge.Type][edge.Source]
	if _, exists := targets[edge.Target]; exists {
		return false, nil
	}
	if schema.SourceCardinality == RelationOne && len(targets) > 0 {
		return false, relationCardinalityError(edge, "source already has a target")
	}
	sources := s.reverse[edge.Type][edge.Target]
	if schema.TargetCardinality == RelationOne && len(sources) > 0 {
		return false, relationCardinalityError(edge, "target already has a source")
	}
	if targets == nil {
		targets = map[EntityID]struct{}{}
		s.forward[edge.Type][edge.Source] = targets
	}
	if sources == nil {
		sources = map[EntityID]struct{}{}
		s.reverse[edge.Type][edge.Target] = sources
	}
	targets[edge.Target] = struct{}{}
	sources[edge.Source] = struct{}{}
	if s.scopes[edge.Type][edge.Source] == nil {
		s.scopes[edge.Type][edge.Source] = map[EntityID]EntityID{}
	}
	s.scopes[edge.Type][edge.Source][edge.Target] = edge.Scope
	return true, nil
}

// Remove atomically removes an edge. It reports false when the edge is absent.
func (s *RelationStore) Remove(edge RelationEdge) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, err := s.validateEdge(edge); err != nil {
		return false, err
	}
	targets := s.forward[edge.Type][edge.Source]
	if _, exists := targets[edge.Target]; !exists {
		return false, nil
	}
	delete(targets, edge.Target)
	delete(s.scopes[edge.Type][edge.Source], edge.Target)
	if len(targets) == 0 {
		delete(s.forward[edge.Type], edge.Source)
		delete(s.scopes[edge.Type], edge.Source)
	}
	sources := s.reverse[edge.Type][edge.Target]
	delete(sources, edge.Source)
	if len(sources) == 0 {
		delete(s.reverse[edge.Type], edge.Target)
	}
	return true, nil
}

func (s *RelationStore) validateEdge(edge RelationEdge) (RelationSchema, error) {
	schema, exists := s.schemas[edge.Type]
	if !exists {
		return RelationSchema{}, &RelationError{Type: edge.Type, Scope: edge.Scope, Source: edge.Source, Target: edge.Target, Err: ErrRelationSchema, Reason: "type is not registered"}
	}
	if edge.Source.ID == "" || edge.Source.Type != schema.Source {
		return RelationSchema{}, &RelationError{Type: edge.Type, Scope: edge.Scope, Source: edge.Source, Target: edge.Target, Err: ErrRelationEndpoint, Reason: fmt.Sprintf("source must be a non-empty %s", schema.Source)}
	}
	if edge.Target.ID == "" || edge.Target.Type != schema.Target {
		return RelationSchema{}, &RelationError{Type: edge.Type, Scope: edge.Scope, Source: edge.Source, Target: edge.Target, Err: ErrRelationEndpoint, Reason: fmt.Sprintf("target must be a non-empty %s", schema.Target)}
	}
	return schema, nil
}

func relationCardinalityError(edge RelationEdge, reason string) error {
	return &RelationError{Type: edge.Type, Scope: edge.Scope, Source: edge.Source, Target: edge.Target, Err: ErrRelationCardinality, Reason: reason}
}

// Targets returns a stable snapshot of targets for one relation source.
func (s *RelationStore) Targets(relationType RelationType, source EntityID) []EntityID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return sortedEntityIDs(s.forward[relationType][source])
}

// Sources returns a stable snapshot of sources for one relation target.
func (s *RelationStore) Sources(relationType RelationType, target EntityID) []EntityID {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return sortedEntityIDs(s.reverse[relationType][target])
}

func sortedEntityIDs(index map[EntityID]struct{}) []EntityID {
	result := make([]EntityID, 0, len(index))
	for id := range index {
		result = append(result, id)
	}
	slices.SortFunc(result, compareEntityID)
	return result
}

func compareEntityID(left, right EntityID) int {
	if result := cmp.Compare(left.Type, right.Type); result != 0 {
		return result
	}
	return cmp.Compare(left.ID, right.ID)
}

// Snapshot returns every edge in deterministic semantic order.
func (s *RelationStore) Snapshot() []RelationEdge {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []RelationEdge
	for relationType, bySource := range s.forward {
		for source, targets := range bySource {
			for target := range targets {
				result = append(result, RelationEdge{Type: relationType, Scope: s.scopes[relationType][source][target], Source: source, Target: target})
			}
		}
	}
	slices.SortFunc(result, func(left, right RelationEdge) int {
		if result := cmp.Compare(left.Type, right.Type); result != 0 {
			return result
		}
		if result := compareEntityID(left.Scope, right.Scope); result != 0 {
			return result
		}
		if result := compareEntityID(left.Source, right.Source); result != 0 {
			return result
		}
		return compareEntityID(left.Target, right.Target)
	})
	return result
}

// PurgeScope removes every edge derived for one scoping root.
func (s *RelationStore) PurgeScope(scope EntityID) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	var removed int
	for relationType, bySource := range s.forward {
		for source, targets := range bySource {
			for target := range targets {
				if s.scopes[relationType][source][target] != scope {
					continue
				}
				delete(targets, target)
				delete(s.scopes[relationType][source], target)
				sources := s.reverse[relationType][target]
				delete(sources, source)
				if len(sources) == 0 {
					delete(s.reverse[relationType], target)
				}
				removed++
			}
			if len(targets) == 0 {
				delete(bySource, source)
				delete(s.scopes[relationType], source)
			}
		}
	}
	return removed
}
