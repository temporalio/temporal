package replication

import (
	"context"
	"sync"
	"sync/atomic"
)

// namespaceIsolationManager coordinates per-namespace dedicated LOW readers on
// the sender. When the receiver signals a namespace is throttled, a dedicated
// goroutine is created for it and the default LOW reader skips that namespace.
// When the namespace is un-throttled, the dedicated reader drains to the
// recorded cursor then removes itself.
type namespaceIsolationManager struct {
	mu               sync.RWMutex
	readers          map[string]*nsReaderHandle
	defaultLowCursor atomic.Int64
}

type nsReaderHandle struct {
	cancel      context.CancelFunc
	startCursor int64
	drainAt     atomic.Int64 // -1 = actively throttled; >= 0 = drain target
}

func newNamespaceIsolationManager() *namespaceIsolationManager {
	return &namespaceIsolationManager{
		readers: make(map[string]*nsReaderHandle),
	}
}

// UpdateDefaultLowCursor stores the most recent cursor position of the default LOW reader.
func (m *namespaceIsolationManager) UpdateDefaultLowCursor(cursor int64) {
	m.defaultLowCursor.Store(cursor)
}

// DefaultLowCursor returns the last stored default LOW cursor.
func (m *namespaceIsolationManager) DefaultLowCursor() int64 {
	return m.defaultLowCursor.Load()
}

// IsDefaultLowExcluded returns true when the default LOW reader should skip namespaceID
// because a dedicated reader (active or draining) owns it.
func (m *namespaceIsolationManager) IsDefaultLowExcluded(namespaceID string) bool {
	m.mu.RLock()
	_, ok := m.readers[namespaceID]
	m.mu.RUnlock()
	return ok
}

// ThrottleNamespace creates a dedicated reader entry for namespaceID and returns a child
// context (derived from parentCtx) and the cursor at which the reader should start.
// Returns (nil, 0, false) if a reader already exists for the namespace.
func (m *namespaceIsolationManager) ThrottleNamespace(parentCtx context.Context, namespaceID string) (context.Context, int64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.readers[namespaceID]; ok {
		return nil, 0, false
	}
	startCursor := m.defaultLowCursor.Load()
	ctx, cancel := context.WithCancel(parentCtx)
	h := &nsReaderHandle{
		cancel:      cancel,
		startCursor: startCursor,
	}
	h.drainAt.Store(-1)
	m.readers[namespaceID] = h
	return ctx, startCursor, true
}

// BeginDrain transitions namespaceID from actively-throttled to draining by recording the
// current default LOW cursor as the drain target. Returns the drain target, or -1 if not found.
func (m *namespaceIsolationManager) BeginDrain(namespaceID string) int64 {
	m.mu.RLock()
	h, ok := m.readers[namespaceID]
	m.mu.RUnlock()
	if !ok {
		return -1
	}
	drainAt := m.defaultLowCursor.Load()
	h.drainAt.Store(drainAt)
	return drainAt
}

// DrainTarget returns the drain cursor and whether the reader for namespaceID is draining.
func (m *namespaceIsolationManager) DrainTarget(namespaceID string) (int64, bool) {
	m.mu.RLock()
	h, ok := m.readers[namespaceID]
	m.mu.RUnlock()
	if !ok {
		return 0, false
	}
	v := h.drainAt.Load()
	return v, v >= 0
}

// Remove cancels and removes the dedicated reader for namespaceID, allowing the default
// LOW reader to include it again. Safe to call multiple times.
func (m *namespaceIsolationManager) Remove(namespaceID string) {
	m.mu.Lock()
	h, ok := m.readers[namespaceID]
	if ok {
		h.cancel()
		delete(m.readers, namespaceID)
	}
	m.mu.Unlock()
}

// Sync reconciles manager state against the new throttled set from the receiver ack.
// Returns namespaces to start dedicated readers for (start) and to begin draining (drain).
func (m *namespaceIsolationManager) Sync(newThrottled []string) (start, drain []string) {
	newSet := make(map[string]struct{}, len(newThrottled))
	for _, ns := range newThrottled {
		newSet[ns] = struct{}{}
	}

	m.mu.RLock()
	for ns, h := range m.readers {
		if _, inNew := newSet[ns]; !inNew && h.drainAt.Load() < 0 {
			drain = append(drain, ns)
		}
	}
	m.mu.RUnlock()

	for ns := range newSet {
		if !m.IsDefaultLowExcluded(ns) {
			start = append(start, ns)
		}
	}
	return start, drain
}
