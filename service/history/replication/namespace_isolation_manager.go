package replication

import (
	"context"
	"sync"
	"sync/atomic"
)

// namespaceIsolationManager coordinates per-namespace dedicated LOW readers on
// the sender. When the receiver signals a namespace is throttled, the default
// LOW reader skips that namespace but no goroutine is started yet (pause phase).
// When the namespace leaves the throttled list, a rate-limited dedicated goroutine
// is created to catch up (catchup phase). Once caught up to defaultLowCursor, the
// reader is atomically removed so the default LOW reader resumes without a gap.
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
	cancel   context.CancelFunc
	cursor   atomic.Int64 // written by dedicated reader goroutine; read under mu
	catching bool         // true once BeginCatchup has been called; protected by m.mu
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

// ThrottleNamespace registers namespaceID for dedicated handling and excludes it
// from the default LOW reader. No goroutine is started; the namespace is simply
// paused until BeginCatchup is called. Returns true if newly registered, false if
// already registered.
func (m *namespaceIsolationManager) ThrottleNamespace(namespaceID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.readers[namespaceID]; ok {
		return false
	}
	h := &nsReaderHandle{cancel: func() {}}
	h.cursor.Store(m.defaultLowCursor)
	m.readers[namespaceID] = h
	return true
}

// BeginCatchup transitions namespaceID from paused to catching up. It creates a
// child context (derived from parentCtx) and returns the cursor at which the
// dedicated reader should start. Returns (nil, 0, false) if the namespace is not
// registered or is already catching up.
func (m *namespaceIsolationManager) BeginCatchup(parentCtx context.Context, namespaceID string) (context.Context, int64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.readers[namespaceID]
	if !ok || h.catching {
		return nil, 0, false
	}
	ctx, cancel := context.WithCancel(parentCtx)
	h.cancel = cancel
	h.catching = true
	return ctx, h.cursor.Load(), true
}

// CheckAndRemoveIfCaughtUp atomically checks whether the dedicated reader for
// namespaceID has reached defaultLowCursor and, if so, cancels it and removes it
// from the exclusion set so the default LOW reader resumes without a gap.
// Returns true if the reader was removed. Returns false if the reader is still
// behind, has not yet started catchup, or is not registered.
func (m *namespaceIsolationManager) CheckAndRemoveIfCaughtUp(namespaceID string) bool {
	m.mu.Lock()
	h, ok := m.readers[namespaceID]
	if !ok || !h.catching || h.cursor.Load() < m.defaultLowCursor {
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
// Returns namespaces to register as paused (start) and namespaces whose throttling
// has been lifted and should begin or continue catchup (drain).
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
