package umpire

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// SerializeStrategy must guarantee mutual exclusion among same-key calls while
// allowing distinct keys to run concurrently.
func TestSerializeStrategy(t *testing.T) {
	// keyFn treats the request value as the key directly.
	s := SerializeStrategy(func(_ string, req any) string { return req.(string) })

	var inFlight atomic.Int32
	var maxConcurrentSameKey atomic.Int32
	invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		n := inFlight.Add(1)
		for {
			old := maxConcurrentSameKey.Load()
			if n <= old || maxConcurrentSameKey.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(2 * time.Millisecond)
		inFlight.Add(-1)
		return nil
	}

	call := func(key string) {
		_ = s.Client(context.Background(), "/svc/M", key, nil, nil, invoker)
	}

	// Many goroutines hammering the SAME key must never overlap.
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); call("entity-A") }()
	}
	wg.Wait()
	require.Equal(t, int32(1), maxConcurrentSameKey.Load(),
		"calls sharing a key must be serialized")

	// Distinct keys are free to overlap (empty key disables serialization too).
	require.NotPanics(t, func() {
		var wg2 sync.WaitGroup
		for _, k := range []string{"a", "b", "c", ""} {
			k := k
			wg2.Add(1)
			go func() { defer wg2.Done(); call(k) }()
		}
		wg2.Wait()
	})
}
