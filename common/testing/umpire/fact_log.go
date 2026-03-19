package umpire

import "sync"

// FactLog records all events for querying during tests.
type FactLog struct {
	mu    sync.RWMutex
	facts []Fact
}

// NewFactLog creates a new event log.
func NewFactLog() *FactLog {
	return &FactLog{facts: make([]Fact, 0)}
}

// Add records an event.
func (sb *FactLog) Add(m Fact) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.facts = append(sb.facts, m)
}

// AddAll records multiple events.
func (sb *FactLog) AddAll(ms []Fact) {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.facts = append(sb.facts, ms...)
}

// QueryByType returns events matching the given entity ID and event type string.
func (sb *FactLog) QueryByType(entityID EntityID, eventType string) []Fact {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	var result []Fact
	for _, m := range sb.facts {
		if m.Name() != eventType {
			continue
		}
		ident := m.TargetEntity()
		if ident == nil {
			continue
		}
		if matchesEntityID(ident.EntityID, entityID) {
			result = append(result, m)
			continue
		}
		if ident.ParentID != nil && matchesEntityID(*ident.ParentID, entityID) {
			result = append(result, m)
		}
	}
	return result
}

// QueryByID returns events targeting the given entity ID (or its children).
func (sb *FactLog) QueryByID(entityID EntityID) []Fact {
	sb.mu.RLock()
	defer sb.mu.RUnlock()

	var result []Fact
	for _, m := range sb.facts {
		ident := m.TargetEntity()
		if ident == nil {
			continue
		}
		if matchesEntityID(ident.EntityID, entityID) {
			result = append(result, m)
			continue
		}
		if ident.ParentID != nil && matchesEntityID(*ident.ParentID, entityID) {
			result = append(result, m)
		}
	}
	return result
}

// All returns a copy of all recorded events.
func (sb *FactLog) All() []Fact {
	sb.mu.RLock()
	defer sb.mu.RUnlock()
	result := make([]Fact, len(sb.facts))
	copy(result, sb.facts)
	return result
}

// Clear removes all recorded events.
func (sb *FactLog) Clear() {
	sb.mu.Lock()
	defer sb.mu.Unlock()
	sb.facts = make([]Fact, 0)
}

// matchesEntityID checks if a matches b (exact match on Type and ID).
func matchesEntityID(a, b EntityID) bool {
	return a.Type == b.Type && a.ID == b.ID
}
