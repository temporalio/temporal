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

// EntityPath identifies the entity a fact targets: the entity's own EntityID plus
// its ancestors, ordered root-first (Ancestors[0] is the outermost, e.g. the namespace).
type EntityPath struct {
	EntityID  EntityID
	Ancestors []EntityID
}

// Root returns the outermost ancestor — the scoping root, e.g. the namespace —
// or the entity's own ID when it has no ancestors.
func (p *EntityPath) Root() EntityID {
	if len(p.Ancestors) > 0 {
		return p.Ancestors[0]
	}
	return p.EntityID
}

// Parent returns the path of the immediate parent, or nil at the root.
func (p *EntityPath) Parent() *EntityPath {
	n := len(p.Ancestors)
	if n == 0 {
		return nil
	}
	return &EntityPath{EntityID: p.Ancestors[n-1], Ancestors: p.Ancestors[:n-1]}
}

// Contains reports whether id is the entity itself or one of its ancestors.
func (p *EntityPath) Contains(id EntityID) bool {
	if p.EntityID == id {
		return true
	}
	for _, a := range p.Ancestors {
		if a == id {
			return true
		}
	}
	return false
}

// Fact is the interface that all facts must implement.
type Fact interface {
	Name() string
	TargetEntity() *EntityPath
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
	OnFact(ctx context.Context, path *EntityPath, facts iter.Seq[Fact]) error
}

// EntityFactory creates a new entity instance.
type EntityFactory func() Entity
