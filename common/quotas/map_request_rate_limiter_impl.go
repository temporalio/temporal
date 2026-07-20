package quotas

import (
	"context"
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
	// identified by a key. It evicts entries idle past the TTL; eviction is traffic-triggered
	// (once per interval) and swept in a short-lived goroutine.
	MapRequestRateLimiterImpl[K comparable] struct {
		rateLimiterGenFn RequestRateLimiterFn
		rateLimiterKeyFn RequestRateLimiterKeyFn[K]

		sync.RWMutex
		rateLimiters map[K]*rateLimiterEntry
		ttlNano      int64 // TTL in nanoseconds

		cleanupTicker *time.Ticker
	}
)

func NewMapRequestRateLimiter[K comparable](
	rateLimiterGenFn RequestRateLimiterFn,
	rateLimiterKeyFn RequestRateLimiterKeyFn[K],
) *MapRequestRateLimiterImpl[K] {
	return &MapRequestRateLimiterImpl[K]{
		rateLimiterGenFn: rateLimiterGenFn,
		rateLimiterKeyFn: rateLimiterKeyFn,
		rateLimiters:     make(map[K]*rateLimiterEntry),
		ttlNano:          int64(rateLimiterTTL),
		cleanupTicker:    time.NewTicker(rateLimiterCleanupInterval),
	}
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
	r.maybeCleanup(now)

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

// maybeCleanup sweeps once per interval, off the request path. The non-blocking
// receive drains at most one ticker tick, so only one sweeper starts even if many
// callers reach here at once.
func (r *MapRequestRateLimiterImpl[K]) maybeCleanup(now time.Time) {
	select {
	case <-r.cleanupTicker.C:
		go r.cleanup(now)
	default:
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
