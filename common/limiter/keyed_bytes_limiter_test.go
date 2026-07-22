package limiter

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKeyedBytesLimiter_Reserve(t *testing.T) {
	l := &KeyedBytesLimiter{}
	const key = "k"

	// Reservation within limit succeeds and tracks the running total.
	ok, used := l.TryReserve(key, 100, 250, 0)
	require.True(t, ok)
	require.Equal(t, int64(100), used)

	ok, used = l.TryReserve(key, 100, 250, 0)
	require.True(t, ok)
	require.Equal(t, int64(200), used)

	// A reservation that would exceed the limit is rejected.
	ok, used = l.TryReserve(key, 100, 250, 0)
	require.False(t, ok)
	require.Equal(t, int64(200), used)

	// A non-positive limit disables the check.
	ok, _ = l.TryReserve(key, 1<<40, 0, 0)
	require.True(t, ok)

	// Release brings the counter back down.
	l.Release(key, 1<<40)
	l.Release(key, 200)
	require.Equal(t, int64(0), l.Used())
	require.Equal(t, int64(0), l.perKey[key])
}

// TestKeyedBytesLimiter_KeyLimit verifies the per-key limit is enforced independently of
// the global total and that one key's usage does not block another.
func TestKeyedBytesLimiter_KeyLimit(t *testing.T) {
	l := &KeyedBytesLimiter{}

	// k1 can fill up to its key limit even though the total has room.
	ok, _ := l.TryReserve("k1", 100, 1000, 150)
	require.True(t, ok)
	ok, _ = l.TryReserve("k1", 100, 1000, 150)
	require.False(t, ok, "second reservation exceeds k1's 150-byte key limit")
	require.Equal(t, int64(100), l.perKey["k1"])

	// A different key has its own budget and is unaffected.
	ok, _ = l.TryReserve("k2", 100, 1000, 150)
	require.True(t, ok)
	require.Equal(t, int64(200), l.Used(), "global total tracks both keys")

	l.Release("k1", 100)
	l.Release("k2", 100)
	require.Equal(t, int64(0), l.Used())
}

// TestKeyedBytesLimiter_ConcurrentNetsToZero exercises concurrent reserve/release on a
// shared limiter.
func TestKeyedBytesLimiter_ConcurrentNetsToZero(t *testing.T) {
	l := &KeyedBytesLimiter{}
	const goroutines = 50

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range 100 {
				if ok, _ := l.TryReserve("k", 1024, 1<<40, 0); ok {
					l.Release("k", 1024)
				}
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int64(0), l.Used())
}
