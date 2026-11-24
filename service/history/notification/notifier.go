// Package notification provides a generic, keyed in-memory pub/sub used by
// server-side long polls: a waiter subscribes on a key of type K and is woken with a
// value of type T when something publishes to that key. Concrete features (e.g.
// time-skipping fast-forward) instantiate PubSubNotifier[K, T] with their own key and
// payload types.
package notification

import (
	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/collection"
)

// PubSubNotifier is a keyed pub/sub over payloads of type T. Notify fans out
// synchronously on the publisher's goroutine (the one producing the change, not the
// waiter's) with non-blocking sends over buffered-1 channels: a value is dropped when a
// waiter already has one pending.
// Watch rejects a new subscriber once a key already has the configured
// maximum, bounding the number of concurrent waiters per key.
type PubSubNotifier[K comparable, T any] interface {
	Notify(key K, value T)
	Watch(key K) (subscriberID string, channel <-chan T, err error)
	Unwatch(key K, subscriberID string) error
}

type pubSubNotifierImpl[K comparable, T any] struct {
	maxSubscribersPerKey int
	// key: K, value: map[subscriberID]chan T. The inner map is
	// not thread-safe on its own; every access is guarded by the ConcurrentTxMap's
	// per-key action callbacks. Subscribers per key are expected to be few.
	subscriptions collection.ConcurrentTxMap
}

// noopNotifier drops every notification and registers no subscribers.
type noopNotifier[K comparable, T any] struct{}

// NewNoopNotifier returns a PubSubNotifier for components (and tests) that never
// participate in the long poll: publishing is a no-op and watching returns an already
// closed channel so an accidental waiter wakes immediately instead of blocking forever.
func NewNoopNotifier[K comparable, T any]() PubSubNotifier[K, T] {
	return noopNotifier[K, T]{}
}

func (noopNotifier[K, T]) Notify(K, T) {}

func (noopNotifier[K, T]) Watch(K) (string, <-chan T, error) {
	ch := make(chan T)
	close(ch)
	return "", ch, nil
}

func (noopNotifier[K, T]) Unwatch(K, string) error { return nil }

// NewPubSubNotifier creates a notifier that admits at most maxSubscribersPerKey
// concurrent waiters per key; Watch beyond that returns a ResourceExhausted error.
//
// hashKey only stripes the internal map's locks for concurrency — any uniform hash of the
// key works and it carries no history-shard meaning here. It need not cover every field of K:
// keys are matched by equality, so a hash over a subset only affects lock distribution.
func NewPubSubNotifier[K comparable, T any](hashKey func(K) uint32, maxSubscribersPerKey int) PubSubNotifier[K, T] {
	if hashKey == nil {
		// A caller bug, but not a fatal one, and better caught here than as a nil-func panic on
		// the first Watch: a constant is still a correct hash, costing lock striping not lookups.
		hashKey = func(K) uint32 { return 0 }
	}
	// The any-typed hash exists only because ConcurrentTxMap is not generic.
	hashFn := func(key any) uint32 {
		k, ok := key.(K)
		if !ok {
			// Unreachable: the map is only ever keyed through Notify/Watch/Unwatch, which are
			// typed K. Checked for sanity, and degrading beats panicking as above.
			return 0
		}
		return hashKey(k)
	}
	return &pubSubNotifierImpl[K, T]{
		maxSubscribersPerKey: maxSubscribersPerKey,
		subscriptions:        collection.NewShardedConcurrentTxMap(1024, hashFn),
	}
}

func (n *pubSubNotifierImpl[K, T]) Watch(key K) (string, <-chan T, error) {
	channel := make(chan T, 1)
	subscriberID := uuid.NewString()
	subscribers := map[string]chan T{subscriberID: channel}

	_, _, err := n.subscriptions.PutOrDo(key, subscribers, func(_ any, value any) error {
		existing, ok := value.(map[string]chan T)
		if !ok {
			return serviceerror.NewInternal("unexpected subscription value type")
		}
		if len(existing) >= n.maxSubscribersPerKey {
			return serviceerror.NewResourceExhaustedf(
				enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT,
				"too many concurrent waiters (limit %d) for this execution", n.maxSubscribersPerKey)
		}
		if _, ok := existing[subscriberID]; ok {
			return serviceerror.NewUnavailable("unable to watch execution")
		}
		existing[subscriberID] = channel
		return nil
	})
	if err != nil {
		return "", nil, err
	}
	return subscriberID, channel, nil
}

func (n *pubSubNotifierImpl[K, T]) Unwatch(key K, subscriberID string) error {
	success := true
	n.subscriptions.RemoveIf(key, func(_ any, value any) bool {
		subscribers, ok := value.(map[string]chan T)
		if !ok {
			success = false
			return false
		}
		if _, ok := subscribers[subscriberID]; !ok {
			success = false
		} else {
			delete(subscribers, subscriberID)
		}
		return len(subscribers) == 0
	})
	if !success {
		return serviceerror.NewInternal("unable to unwatch execution")
	}
	return nil
}

func (n *pubSubNotifierImpl[K, T]) Notify(key K, value T) {
	_, _, _ = n.subscriptions.GetAndDo(key, func(_ any, existing any) error {
		subscribers, ok := existing.(map[string]chan T)
		if !ok {
			return nil
		}
		for _, channel := range subscribers {
			select {
			case channel <- value:
			default:
				// Buffer (size 1) is full: this value is dropped, so a newer one can be lost
				// behind an older one. Delivery is best-effort; subscribers must recover on
				// their own (re-read state on wake, or time out and get polled again).
			}
		}
		return nil
	})
}
