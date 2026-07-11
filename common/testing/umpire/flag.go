package umpire

import (
	"fmt"
	"sync"
)

// Flag is a named boolean observable condition on an entity,
// set and cleared by entity FSM transitions.
type Flag struct {
	mu    sync.RWMutex
	value bool
	label string // "EntityType:FieldName"
}

// NewFlag creates a Flag with the given label.
func NewFlag(label string) *Flag {
	return &Flag{label: label}
}

func (f *Flag) Set() {
	f.mu.Lock()
	f.value = true
	f.mu.Unlock()
}

func (f *Flag) Clear() {
	f.mu.Lock()
	f.value = false
	f.mu.Unlock()
}

func (f *Flag) IsTrue() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.value
}

func (f *Flag) IsFalse() bool { return !f.IsTrue() }

func (f *Flag) Label() string { return f.label }

func (f *Flag) String() string {
	return fmt.Sprintf("Flag{%s=%v}", f.label, f.IsTrue())
}
