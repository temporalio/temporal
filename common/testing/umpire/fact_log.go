package umpire

import "sync"

// FactLog records all facts for querying during tests.
type FactLog struct {
	mu    sync.RWMutex
	facts []Fact
}

// NewFactLog creates a new fact log.
func NewFactLog() *FactLog {
	return &FactLog{facts: make([]Fact, 0)}
}

// Add records a fact.
func (l *FactLog) Add(f Fact) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.facts = append(l.facts, f)
}

// AddAll records multiple facts.
func (l *FactLog) AddAll(fs []Fact) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.facts = append(l.facts, fs...)
}

// QueryByType returns facts matching the given entity ID and fact type string.
func (l *FactLog) QueryByType(entityID EntityID, factType string) []Fact {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var result []Fact
	for _, f := range l.facts {
		if f.Name() != factType {
			continue
		}
		path := f.TargetEntity()
		if path == nil {
			continue
		}
		if matchesEntityID(path.EntityID, entityID) {
			result = append(result, f)
			continue
		}
		if path.ParentID != nil && matchesEntityID(*path.ParentID, entityID) {
			result = append(result, f)
		}
	}
	return result
}

// QueryByID returns facts targeting the given entity ID (or its children).
func (l *FactLog) QueryByID(entityID EntityID) []Fact {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var result []Fact
	for _, f := range l.facts {
		path := f.TargetEntity()
		if path == nil {
			continue
		}
		if matchesEntityID(path.EntityID, entityID) {
			result = append(result, f)
			continue
		}
		if path.ParentID != nil && matchesEntityID(*path.ParentID, entityID) {
			result = append(result, f)
		}
	}
	return result
}

// All returns a copy of all recorded facts.
func (l *FactLog) All() []Fact {
	l.mu.RLock()
	defer l.mu.RUnlock()
	result := make([]Fact, len(l.facts))
	copy(result, l.facts)
	return result
}

// Clear removes all recorded facts.
func (l *FactLog) Clear() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.facts = make([]Fact, 0)
}

// matchesEntityID checks if a matches b (exact match on Type and ID).
func matchesEntityID(a, b EntityID) bool {
	return a.Type == b.Type && a.ID == b.ID
}
