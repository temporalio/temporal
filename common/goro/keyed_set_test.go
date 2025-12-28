package goro_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/goro"
)

func TestKeyedSet_Starts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ks := goro.NewKeyedSet[int](ctx)

	var running atomic.Int32
	f := func(ctx context.Context, key int) {
		running.Add(1)
		<-ctx.Done()
		running.Add(-1)
	}

	// Start with 3 goroutines
	target := map[int]struct{}{1: {}, 2: {}, 3: {}}
	ks.Sync(target, f)

	require.Eventually(t, func() bool {
		return running.Load() == 3
	}, time.Second, time.Millisecond)
}

func TestKeyedSet_Cancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ks := goro.NewKeyedSet[int](ctx)

	var running atomic.Int32
	f := func(ctx context.Context, key int) {
		running.Add(1)
		<-ctx.Done()
		running.Add(-1)
	}

	target := map[int]struct{}{1: {}, 2: {}, 3: {}}
	ks.Sync(target, f)

	require.Eventually(t, func() bool {
		return running.Load() == 3
	}, time.Second, time.Millisecond)

	// Remove key 2
	target = map[int]struct{}{1: {}, 3: {}}
	ks.Sync(target, f)

	// Goroutine for key 2 should be canceled
	require.Eventually(t, func() bool {
		return running.Load() == 2
	}, time.Second, time.Millisecond)
}

func TestKeyedSet_StartsAndCancelsTogether(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ks := goro.NewKeyedSet[string](ctx)

	var running sync.Map
	f := func(ctx context.Context, key string) {
		running.Store(key, true)
		<-ctx.Done()
		running.Delete(key)
	}

	runningKeys := func() (keys []string) {
		running.Range(func(key, _ any) bool {
			keys = append(keys, key.(string))
			return true
		})
		return keys
	}

	// Start with {"a", "b"}
	ks.Sync(map[string]struct{}{"a": {}, "b": {}}, f)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.ElementsMatch(c, []string{"a", "b"}, runningKeys())
	}, time.Second, time.Millisecond)

	// Change to {"b", "c"} - should cancel "a", keep "b", start "c"
	ks.Sync(map[string]struct{}{"b": {}, "c": {}}, f)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.ElementsMatch(c, []string{"b", "c"}, runningKeys())
	}, time.Second, time.Millisecond)

	// Cancel all
	ks.Sync(nil, nil)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Empty(c, runningKeys())
	}, time.Second, time.Millisecond)
}

func TestKeyedSet_BaseContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ks := goro.NewKeyedSet[int](ctx)

	var running atomic.Int32
	f := func(ctx context.Context, key int) {
		running.Add(1)
		<-ctx.Done()
		running.Add(-1)
	}

	// Start goroutines
	ks.Sync(map[int]struct{}{1: {}, 2: {}}, f)
	require.Eventually(t, func() bool { return running.Load() == 2 }, time.Second, time.Millisecond)

	// Cancel base context
	cancel()
	require.Eventually(t, func() bool { return running.Load() == 0 }, time.Second, time.Millisecond)
}

func TestKeyedSet_ConcurrentSync(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ks := goro.NewKeyedSet[int](ctx)

	f := func(ctx context.Context, key int) {
		<-ctx.Done()
	}

	// Run concurrent syncs - should not panic
	var wg sync.WaitGroup
	for i := range 1000 {
		wg.Go(func() {
			target := map[int]struct{}{i % 5: {}, (i + 1) % 5: {}}
			ks.Sync(target, f)
		})
	}
	wg.Wait()

	ks.Sync(nil, nil)
}
