package notification

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
)

func oneShard(namespace.ID, string) int32 { return 1 }

// testKey builds an arbitrary subscription key for the generic notifier tests; the generic
// pub/sub is key-agnostic, so these don't use the time-skipping-specific key constructor.
func testKey(namespaceID, workflowID string) definition.WorkflowKey {
	return definition.NewWorkflowKey(namespaceID, workflowID, "")
}

// liveKeys returns the number of keys currently held in the notifier's internal map,
// used to prove keys are reclaimed (no leak) once their last waiter unwatches.
func liveKeys[T any](t *testing.T, n PubSubNotifier[T]) int {
	t.Helper()
	impl, ok := n.(*pubSubNotifierImpl[T])
	require.True(t, ok)
	return impl.subscriptions.Len()
}

func TestPubSubNotifier(t *testing.T) {
	t.Run("fan-out delivers to every subscriber on the key", func(t *testing.T) {
		n := NewPubSubNotifier[int](oneShard, 5)
		key := testKey("ns", "wf")
		_, ch1, err := n.Watch(key)
		require.NoError(t, err)
		_, ch2, err := n.Watch(key)
		require.NoError(t, err)
		n.Notify(key, 7)
		require.Equal(t, 7, <-ch1)
		require.Equal(t, 7, <-ch2)
	})

	t.Run("notify to a key with no subscribers is a no-op", func(t *testing.T) {
		n := NewPubSubNotifier[int](oneShard, 5)
		require.NotPanics(t, func() { n.Notify(testKey("ns", "absent"), 1) })
		require.Zero(t, liveKeys(t, n))
	})

	t.Run("notify is non-blocking and drops when the waiter buffer is full", func(t *testing.T) {
		n := NewPubSubNotifier[int](oneShard, 5)
		key := testKey("ns", "wf")
		_, ch, err := n.Watch(key)
		require.NoError(t, err)
		n.Notify(key, 1)                                  // fills the buffered-1 channel
		require.NotPanics(t, func() { n.Notify(key, 2) }) // dropped; must not block or panic
		require.Equal(t, 1, <-ch)
		select {
		case v := <-ch:
			require.Failf(t, "unexpected extra value", "got %d", v)
		default:
		}
	})

	t.Run("watch is rejected past the per-key cap", func(t *testing.T) {
		const limit = 3
		n := NewPubSubNotifier[int](oneShard, limit)
		key := testKey("ns", "wf")
		for range limit {
			_, _, err := n.Watch(key)
			require.NoError(t, err)
		}
		_, _, err := n.Watch(key)
		var resourceExhausted *serviceerror.ResourceExhausted
		require.ErrorAs(t, err, &resourceExhausted)
	})

	t.Run("keys are isolated from each other", func(t *testing.T) {
		n := NewPubSubNotifier[int](oneShard, 1)
		keyA := testKey("ns", "a")
		keyB := testKey("ns", "b")
		_, chA, err := n.Watch(keyA)
		require.NoError(t, err)
		_, _, err = n.Watch(keyB) // different key is unaffected by A's cap of 1
		require.NoError(t, err)
		n.Notify(keyA, 8)
		require.Equal(t, 8, <-chA)
	})

	t.Run("unwatch frees a slot", func(t *testing.T) {
		n := NewPubSubNotifier[int](oneShard, 1)
		key := testKey("ns", "wf")
		id, _, err := n.Watch(key)
		require.NoError(t, err)
		_, _, err = n.Watch(key) // at the cap of 1
		require.Error(t, err)
		require.NoError(t, n.Unwatch(key, id))
		_, _, err = n.Watch(key) // slot freed, succeeds again
		require.NoError(t, err)
	})

	t.Run("unwatching the last subscriber removes the key (no leak)", func(t *testing.T) {
		n := NewPubSubNotifier[int](oneShard, 5)
		key := testKey("ns", "wf")
		id1, _, err := n.Watch(key)
		require.NoError(t, err)
		id2, _, err := n.Watch(key)
		require.NoError(t, err)
		require.Equal(t, 1, liveKeys(t, n)) // one key, two subscribers
		require.NoError(t, n.Unwatch(key, id1))
		require.Equal(t, 1, liveKeys(t, n)) // one subscriber remains -> key stays
		require.NoError(t, n.Unwatch(key, id2))
		require.Zero(t, liveKeys(t, n)) // last subscriber gone -> key removed
	})

	t.Run("unwatch of an unknown subscriber errors without panicking", func(t *testing.T) {
		n := NewPubSubNotifier[int](oneShard, 5)
		key := testKey("ns", "wf")
		_, _, err := n.Watch(key)
		require.NoError(t, err)
		var unwatchErr error
		require.NotPanics(t, func() { unwatchErr = n.Unwatch(key, "does-not-exist") })
		require.Error(t, unwatchErr)
	})

	t.Run("unwatch of an unknown key is a no-op without panicking", func(t *testing.T) {
		n := NewPubSubNotifier[int](oneShard, 5)
		require.NotPanics(t, func() { _ = n.Unwatch(testKey("ns", "absent"), "x") })
		require.Zero(t, liveKeys(t, n))
	})

	t.Run("nil payload is delivered without panicking", func(t *testing.T) {
		type payload struct{ v int }
		n := NewPubSubNotifier[*payload](oneShard, 5)
		key := testKey("ns", "wf")
		_, ch, err := n.Watch(key)
		require.NoError(t, err)
		require.NotPanics(t, func() { n.Notify(key, nil) })
		require.Nil(t, <-ch)
	})

	// Stresses the ConcurrentTxMap's per-key guarding: concurrent Watch/Notify/Unwatch
	// must not panic or race (run with -race), and every key must be reclaimed once its
	// waiters leave. Uses a high cap so the cap itself is not exercised here.
	t.Run("concurrent watch/notify/unwatch is panic-free and leaks no keys", func(t *testing.T) {
		n := NewPubSubNotifier[int](oneShard, 1000)
		const goroutines = 200
		var wg sync.WaitGroup
		wg.Add(goroutines)
		for i := range goroutines {
			go func(i int) {
				defer wg.Done()
				key := testKey("ns", fmt.Sprintf("wf-%d", i%16)) // 16 shared keys
				id, ch, err := n.Watch(key)
				if err != nil {
					return
				}
				n.Notify(key, i)
				select {
				case <-ch:
				default:
				}
				_ = n.Unwatch(key, id)
			}(i)
		}
		wg.Wait()
		require.Zero(t, liveKeys(t, n), "all keys must be removed after their waiters unwatch")
	})
}

func TestNoopNotifier(t *testing.T) {
	n := NewNoopNotifier[int]()
	key := testKey("ns", "wf")

	id, ch, err := n.Watch(key)
	require.NoError(t, err)
	require.NotPanics(t, func() { n.Notify(key, 1) }) // dropped
	_, open := <-ch
	require.False(t, open) // Watch returns an already-closed channel: never blocks
	require.NoError(t, n.Unwatch(key, id))
}
