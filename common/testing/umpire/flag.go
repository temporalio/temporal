package umpire

import (
	"fmt"
	"sync"
)

// Flag is a named boolean observable condition on an entity.
// Set by entity FSM transitions via the observer package.
type Flag struct {
	mu    sync.RWMutex
	value bool
	label string // "EntityType:FieldName"
}

// NewFlag creates a Flag with the given label.
func NewFlag(label string) *Flag {
	return &Flag{label: label}
}

func (m *Flag) Set() {
	m.mu.Lock()
	m.value = true
	m.mu.Unlock()
}

func (m *Flag) Clear() {
	m.mu.Lock()
	m.value = false
	m.mu.Unlock()
}

func (m *Flag) IsTrue() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.value
}

func (m *Flag) IsFalse() bool { return !m.IsTrue() }

func (m *Flag) Label() string { return m.label }

func (m *Flag) String() string {
	return fmt.Sprintf("Flag{%s=%v}", m.label, m.IsTrue())
}
