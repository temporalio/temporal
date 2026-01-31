package quotas

import (
	"container/list"
	"context"
	"sync"
	"time"
)

const (
	rateLimiterTTL = 30 * time.Minute // Evict entries idle longer than this
)

type (
	// RequestRateLimiterKeyFn extracts the map key from the request
	RequestRateLimiterKeyFn[K comparable] func(req Request) K

	rateLimiterEntry[K comparable] struct {
		key         K
		rateLimiter RequestRateLimiter
		lastAccess  time.Time
	}

	// MapRequestRateLimiterImpl is a generic wrapper rate limiter for a set of rate limiters
	// identified by a key. It uses lazy eviction to clean up entries that have not been
	// accessed for longer than the TTL.
	//
	// Lazy eviction removes at most one expired entry per access. This is sufficient to
	// keep the map small because there are many more rate limiter lookups than new rate
	// limiter creations (most requests hit existing namespaces). Over time, the eviction
	// rate will match or exceed the creation rate, keeping memory bounded.
	MapRequestRateLimiterImpl[K comparable] struct {
		rateLimiterGenFn RequestRateLimiterFn
		rateLimiterKeyFn RequestRateLimiterKeyFn[K]

		mu         sync.Mutex
		byKey      map[K]*list.Element // key -> list element
		accessList *list.List          // ordered by last access (oldest at front)
		ttl        time.Duration
	}
)

func NewMapRequestRateLimiter[K comparable](
	rateLimiterGenFn RequestRateLimiterFn,
	rateLimiterKeyFn RequestRateLimiterKeyFn[K],
) *MapRequestRateLimiterImpl[K] {
	return &MapRequestRateLimiterImpl[K]{
		rateLimiterGenFn: rateLimiterGenFn,
		rateLimiterKeyFn: rateLimiterKeyFn,
		byKey:            make(map[K]*list.Element),
		accessList:       list.New(),
		ttl:              rateLimiterTTL,
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
	key := r.rateLimiterKeyFn(req)

	r.mu.Lock()
	defer r.mu.Unlock()

	// Cleanup: remove oldest entry if expired
	r.evictOldestIfExpired(now)

	// Lookup or create
	if elem, ok := r.byKey[key]; ok {
		// Update access time and move to back
		entry := elem.Value.(*rateLimiterEntry[K])
		entry.lastAccess = now
		r.accessList.MoveToBack(elem)
		return entry.rateLimiter
	}

	// Create new entry
	newRateLimiter := r.rateLimiterGenFn(req)
	entry := &rateLimiterEntry[K]{
		key:         key,
		rateLimiter: newRateLimiter,
		lastAccess:  now,
	}
	elem := r.accessList.PushBack(entry)
	r.byKey[key] = elem
	return newRateLimiter
}

func (r *MapRequestRateLimiterImpl[K]) evictOldestIfExpired(now time.Time) {
	front := r.accessList.Front()
	if front == nil {
		return
	}

	entry := front.Value.(*rateLimiterEntry[K])
	if now.Sub(entry.lastAccess) > r.ttl {
		r.accessList.Remove(front)
		delete(r.byKey, entry.key)
	}
}
