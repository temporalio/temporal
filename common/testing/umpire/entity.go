package umpire

import (
	"context"
	"iter"
)

// EntityType is a strongly-typed entity type identifier.
type EntityType string

// EntityID uniquely identifies an entity within its type.
type EntityID struct {
	Type EntityType
	ID   string
}

func (e EntityID) String() string { return string(e.Type) + ":" + e.ID }

// NewEntityID creates an EntityID with the given type and id.
func NewEntityID(entityType EntityType, id string) EntityID {
	return EntityID{Type: entityType, ID: id}
}

// Identity represents the full identity of an entity with optional parent.
type Identity struct {
	EntityID EntityID
	ParentID *EntityID
}

// Event is the interface that all events must implement.
type Fact interface {
	Name() string
	TargetEntity() *Identity
}

// BroadcastFact is a fact that is delivered to all entities of a given type,
// rather than a single targeted entity. The entity's OnFact handler is
// responsible for filtering by relevant fields (e.g. workflowID).
type BroadcastFact interface {
	Fact
	BroadcastType() EntityType
}

// Entity is the interface that all entities must implement.
type Entity interface {
	Type() EntityType
	OnFact(ctx context.Context, identity *Identity, facts iter.Seq[Fact]) error
}

// EntityFactory creates a new entity instance.
type EntityFactory func() Entity
