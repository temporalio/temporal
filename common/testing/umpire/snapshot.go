package umpire

import "slices"

// EntitySnapshot is the stable, read-only lifecycle view of one modeled entity.
type EntitySnapshot struct {
	Key           string
	Type          EntityType
	ID            string
	RootID        string
	PredecessorID string
	Initiator     string
	Current       string
	Terminal      bool
	Disposition   Disposition
	Attempt       int
	Visited       []Edge
}

// FactSnapshot is the stable, read-only identity of one observed fact.
type FactSnapshot struct {
	Name string
}

// ObservationQuery asks whether one protocol-level semantic observation has occurred.
type ObservationQuery struct {
	Predicate  string
	Arguments  []string
	Historical bool
}

// Snapshot is a defensive, scope-specific view of the runtime's semantic state.
type Snapshot struct {
	Generation uint64
	Entities   []EntitySnapshot
	Facts      []FactSnapshot
	Relations  []RelationEdge
}

// EntitiesOfType returns a defensive view of the entities with the requested type.
func (s Snapshot) EntitiesOfType(entityType EntityType) []EntitySnapshot {
	result := make([]EntitySnapshot, 0, len(s.Entities))
	for _, entity := range s.Entities {
		if entity.Type != entityType {
			continue
		}
		entity.Visited = slices.Clone(entity.Visited)
		result = append(result, entity)
	}
	return result
}

// FactNames returns fact names in observation order.
func (s Snapshot) FactNames() []string {
	result := make([]string, len(s.Facts))
	for i, fact := range s.Facts {
		result[i] = fact.Name
	}
	return result
}
