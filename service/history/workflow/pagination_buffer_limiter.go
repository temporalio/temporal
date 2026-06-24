package workflow

import "sync"

// PaginationBufferLimiter tracks in-flight bytes and enforces a process-wide total plus
// a per-namespace sub-limit, so one namespace cannot exhaust the whole process total.
type PaginationBufferLimiter struct {
	mu           sync.Mutex
	used         int64
	perNamespace map[string]int64
}

// tryReserve adds n bytes to the totals if both the process-wide total stays within
// processLimit and the namespace's total within namespaceLimit. A non-positive limit
// disables that check. Returns success and the resulting process-wide total.
func (b *PaginationBufferLimiter) tryReserve(namespace string, n, processLimit, namespaceLimit int64) (bool, int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if processLimit > 0 && b.used+n > processLimit {
		return false, b.used
	}
	if namespaceLimit > 0 && b.perNamespace[namespace]+n > namespaceLimit {
		return false, b.used
	}
	b.used += n
	if b.perNamespace == nil {
		b.perNamespace = make(map[string]int64)
	}
	b.perNamespace[namespace] += n
	return true, b.used
}

// release returns n bytes for the given namespace and the total and reports the resulting
// process-wide total.
func (b *PaginationBufferLimiter) release(namespace string, n int64) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if n != 0 {
		b.used -= n
		if remaining := b.perNamespace[namespace] - n; remaining > 0 {
			b.perNamespace[namespace] = remaining
		} else {
			delete(b.perNamespace, namespace)
		}
	}
	return b.used
}

// Used reports the current process-wide in-flight byte total.
func (b *PaginationBufferLimiter) Used() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// NewPaginationBufferLimiter returns an empty limiter
func NewPaginationBufferLimiter() *PaginationBufferLimiter {
	return &PaginationBufferLimiter{}
}
