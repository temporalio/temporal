package limiter

import "sync"

// KeyedBytesLimiter tracks in-flight bytes and enforces a global total plus a per-key
// sub-limit, so one key cannot exhaust the whole total. The limits are supplied by the
// caller on each reservation, so the limiter itself holds no configuration and only does
// the accounting. It is safe for concurrent use.
type KeyedBytesLimiter struct {
	mu     sync.Mutex
	used   int64
	perKey map[string]int64
}

// TryReserve adds n bytes to the totals if both the global total stays within totalLimit
// and the key's total within keyLimit. A non-positive limit disables that check. Returns
// success and the resulting global total.
func (l *KeyedBytesLimiter) TryReserve(key string, n, totalLimit, keyLimit int64) (bool, int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if totalLimit > 0 && l.used+n > totalLimit {
		return false, l.used
	}
	if keyLimit > 0 && l.perKey[key]+n > keyLimit {
		return false, l.used
	}
	l.used += n
	if l.perKey == nil {
		l.perKey = make(map[string]int64)
	}
	l.perKey[key] += n
	return true, l.used
}

// Release returns n bytes for the given key and reports the resulting global total.
func (l *KeyedBytesLimiter) Release(key string, n int64) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if n != 0 {
		l.used -= n
		if remaining := l.perKey[key] - n; remaining > 0 {
			l.perKey[key] = remaining
		} else {
			delete(l.perKey, key)
		}
	}
	return l.used
}

// Used reports the current global in-flight byte total.
func (l *KeyedBytesLimiter) Used() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.used
}

// NewKeyedBytesLimiter returns an empty limiter.
func NewKeyedBytesLimiter() *KeyedBytesLimiter {
	return &KeyedBytesLimiter{}
}
