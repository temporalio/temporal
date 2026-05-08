package replication

import (
	"context"
	"sync"
	"sync/atomic"
)

// namespaceIsolationManager coordinates per-namespace dedicated LOW readers on
// the sender. When the receiver signals a namespace is throttled, a dedicated
// goroutine is created for it and the default LOW reader skips that namespace.
// When the namespace is no longer throttled and its dedicated reader has caught
// up to defaultLowCursor, the reader is atomically removed so the default LOW
// reader resumes without a gap.
//
// All fields are protected by mu. nsReaderHandle.cursor is an atomic because it
// is written by the dedicated reader goroutine and read by the recv-loop goroutine
// without a lock (the handle itself is looked up under mu, but the cursor within
// the handle is updated without re-acquiring mu).
type namespaceIsolationManager struct {
	mu               sync.RWMutex
	readers          map[string]*nsReaderHandle
	defaultLowCursor int64 // protected by mu
}

type nsReaderHandle struct {
	cancel context.CancelFunc
	cursor atomic.Int64 // written by dedicated reader goroutine; read under mu
}

func newNamespaceIsolationManager() *namespaceIsolationManager {
	return &namespaceIsolationManager{
		readers: make(map[string]*nsReaderHandle),
	}
}

// UpdateDefaultLowCursor records the most recent cursor position of the default LOW reader.
func (m *namespaceIsolationManager) UpdateDefaultLowCursor(cursor int64) {
	m.mu.Lock()
	m.defaultLowCursor = cursor
	m.mu.Unlock()
}

// DefaultLowCursor returns the last stored default LOW cursor.
func (m *namespaceIsolationManager) DefaultLowCursor() int64 {
	m.mu.RLock()
	v := m.defaultLowCursor
	m.mu.RUnlock()
	return v
}

// EffectiveLowCursor returns the lowest cursor across the default LOW reader and
// all active dedicated namespace readers. This is the correct watermark to use
// when persisting progress — DefaultLowCursor alone overstates progress while
// dedicated readers are still behind.
func (m *namespaceIsolationManager) EffectiveLowCursor() int64 {
	m.mu.RLock()
	min := m.defaultLowCursor
	for _, h := range m.readers {
		if c := h.cursor.Load(); c < min {
			min = c
		}
	}
	m.mu.RUnlock()
	return min
}

// UpdateDedicatedCursor records the current cursor of the dedicated reader for
// namespaceID. Called by the sender after each sendTasks batch.
func (m *namespaceIsolationManager) UpdateDedicatedCursor(namespaceID string, cursor int64) {
	m.mu.RLock()
	h, ok := m.readers[namespaceID]
	m.mu.RUnlock()
	if ok {
		h.cursor.Store(cursor)
	}
}

// IsDefaultLowExcluded returns true when the default LOW reader should skip namespaceID
// because a dedicated reader owns it.
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
	startCursor := m.defaultLowCursor
	ctx, cancel := context.WithCancel(parentCtx)
	h := &nsReaderHandle{cancel: cancel}
	h.cursor.Store(startCursor)
	m.readers[namespaceID] = h
	return ctx, startCursor, true
}

// CheckAndRemoveIfCaughtUp atomically checks whether the dedicated reader for
// namespaceID has reached defaultLowCursor and, if so, cancels it and removes it
// from the exclusion set so the default LOW reader resumes without a gap.
// Returns true if the reader was removed. If the reader is still behind, it
// continues running; the caller should retry on the next receiver ack.
func (m *namespaceIsolationManager) CheckAndRemoveIfCaughtUp(namespaceID string) bool {
	m.mu.Lock()
	h, ok := m.readers[namespaceID]
	if !ok || h.cursor.Load() < m.defaultLowCursor {
		m.mu.Unlock()
		return false
	}
	h.cancel()
	delete(m.readers, namespaceID)
	m.mu.Unlock()
	return true
}

// Remove cancels and removes the dedicated reader for namespaceID. Safe to call
// multiple times (idempotent). Used for stream shutdown cleanup.
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
// Returns namespaces to start dedicated readers for (start) and namespaces whose
// dedicated readers should be checked for catch-up removal (drain).
func (m *namespaceIsolationManager) Sync(newThrottled []string) (start, drain []string) {
	newSet := make(map[string]struct{}, len(newThrottled))
	for _, ns := range newThrottled {
		newSet[ns] = struct{}{}
	}

	m.mu.RLock()
	for ns := range m.readers {
		if _, inNew := newSet[ns]; !inNew {
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
