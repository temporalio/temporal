package quotas

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	rateLimiterTTL             = 1 * time.Hour
	rateLimiterCleanupInterval = 1 * time.Hour
)

type (
	// RequestRateLimiterKeyFn extracts the map key from the request
	RequestRateLimiterKeyFn[K comparable] func(req Request) K

	rateLimiterEntry struct {
		rateLimiter RequestRateLimiter
		lastAccess  atomic.Int64
	}

	// MapRequestRateLimiterImpl is a generic wrapper rate limiter for a set of rate limiters
	// identified by a key. It periodically evicts entries that have not been accessed for
	// longer than the TTL.
	MapRequestRateLimiterImpl[K comparable] struct {
		rateLimiterGenFn RequestRateLimiterFn
		rateLimiterKeyFn RequestRateLimiterKeyFn[K]

		sync.RWMutex
		rateLimiters map[K]*rateLimiterEntry
		ttlNano      int64 // TTL in nanoseconds
		stopCh       chan struct{}
	}
)

func NewMapRequestRateLimiter[K comparable](
	rateLimiterGenFn RequestRateLimiterFn,
	rateLimiterKeyFn RequestRateLimiterKeyFn[K],
) *MapRequestRateLimiterImpl[K] {
	// Create channel before struct to pass to AddCleanup without capturing r
	stopCh := make(chan struct{})

	r := &MapRequestRateLimiterImpl[K]{
		rateLimiterGenFn: rateLimiterGenFn,
		rateLimiterKeyFn: rateLimiterKeyFn,
		rateLimiters:     make(map[K]*rateLimiterEntry),
		ttlNano:          int64(rateLimiterTTL),
		stopCh:           stopCh,
	}

	// Start background cleanup goroutine
	go r.cleanupLoop()

	// Register cleanup to stop goroutine when r is GC'd
	runtime.AddCleanup(r, func(ch chan struct{}) {
		close(ch)
	}, stopCh)

	return r
}

func namespaceRequestRateLimiterKeyFn(req Request) string {
	return req.Caller
}

func NewNamespaceRequestRateLimiter(
	rateLimiterGenFn RequestRateLimiterFn,
) *MapRequestRateLimiterImpl[string] {
	return NewMapRequestRateLimiter(rateLimiterGenFn, namespaceRequestRateLimiterKeyFn)
}

// Allow attempts to allow a request to go through. The method returns
// immediately with a true or false indicating if the request can make
// progress
func (r *MapRequestRateLimiterImpl[_]) Allow(
	now time.Time,
	request Request,
) bool {
	rateLimiter := r.getOrInitRateLimiter(now, request)
	return rateLimiter.Allow(now, request)
}

// Reserve returns a Reservation that indicates how long the caller
// must wait before event happen.
func (r *MapRequestRateLimiterImpl[_]) Reserve(
	now time.Time,
	request Request,
) Reservation {
	rateLimiter := r.getOrInitRateLimiter(now, request)
	return rateLimiter.Reserve(now, request)
}

// Wait waits till the deadline for a rate limit token to allow the request
// to go through.
func (r *MapRequestRateLimiterImpl[_]) Wait(
	ctx context.Context,
	request Request,
) error {
	rateLimiter := r.getOrInitRateLimiter(time.Now(), request)
	return rateLimiter.Wait(ctx, request)
}

func (r *MapRequestRateLimiterImpl[K]) getOrInitRateLimiter(
	now time.Time,
	req Request,
) RequestRateLimiter {
	key := r.rateLimiterKeyFn(req)
	nowNano := now.UnixNano()

	r.RLock()
	entry, ok := r.rateLimiters[key]
	r.RUnlock()

	if ok {
		entry.lastAccess.Store(nowNano)
		return entry.rateLimiter
	}

	newRateLimiter := r.rateLimiterGenFn(req)
	r.Lock()
	defer r.Unlock()

	if entry, ok := r.rateLimiters[key]; ok {
		entry.lastAccess.Store(nowNano)
		return entry.rateLimiter
	}

	entry = &rateLimiterEntry{rateLimiter: newRateLimiter}
	entry.lastAccess.Store(nowNano)
	r.rateLimiters[key] = entry
	return newRateLimiter
}

func (r *MapRequestRateLimiterImpl[K]) cleanupLoop() {
	ticker := time.NewTicker(rateLimiterCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case now := <-ticker.C:
			r.cleanup(now)
		}
	}
}

// cleanup uses a two-phase approach to minimize lock contention:
// 1. RLock: collect candidates for eviction
// 2. For each candidate: Lock, re-check, delete, Unlock
// This ensures read operations are only briefly blocked during actual deletions.
func (r *MapRequestRateLimiterImpl[K]) cleanup(now time.Time) {
	nowNano := now.UnixNano()
	ttlNano := r.ttlNano

	// Phase 1: collect expired keys under read lock
	var expiredKeys []K
	r.RLock()
	for k, e := range r.rateLimiters {
		if nowNano-e.lastAccess.Load() > ttlNano {
			expiredKeys = append(expiredKeys, k)
		}
	}
	r.RUnlock()

	// Phase 2: delete each expired key individually
	for _, k := range expiredKeys {
		r.Lock()
		if e, ok := r.rateLimiters[k]; ok && nowNano-e.lastAccess.Load() > ttlNano {
			delete(r.rateLimiters, k)
		}
		r.Unlock()
	}
}
