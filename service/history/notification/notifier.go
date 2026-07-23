// Package notification provides a generic, keyed in-memory pub/sub used by
// server-side long polls: a waiter subscribes on a workflow's (namespace,
// workflowID) key and is woken with a value of type T when something publishes
// to that key. Concrete features (e.g. time-skipping fast-forward) instantiate
// PubSubNotifier[T] with their own payload type.
package notification

import (
	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
)

// PubSubNotifier is a keyed pub/sub over payloads of type T. Notify fans out
// synchronously on the publisher's goroutine (the one producing the change, not the
// waiter's) with non-blocking sends over buffered-1 channels: a value is dropped when a
// waiter already has one pending, which is safe for callers that re-read authoritative
// state on wake. Watch rejects a new subscriber once a key already has the configured
// maximum, bounding the number of concurrent waiters per key.
type PubSubNotifier[T any] interface {
	Notify(key definition.WorkflowKey, value T)
	Watch(key definition.WorkflowKey) (subscriberID string, channel <-chan T, err error)
	Unwatch(key definition.WorkflowKey, subscriberID string) error
}

type pubSubNotifierImpl[T any] struct {
	workflowIDToShardID  func(namespace.ID, string) int32
	maxSubscribersPerKey int
	// key: definition.WorkflowKey, value: map[subscriberID]chan T. The inner map is
	// not thread-safe on its own; every access is guarded by the ConcurrentTxMap's
	// per-key action callbacks. Subscribers per key are expected to be few.
	subscriptions collection.ConcurrentTxMap
}

// noopNotifier drops every notification and registers no subscribers.
type noopNotifier[T any] struct{}

// NewNoopNotifier returns a PubSubNotifier for components (and tests) that never
// participate in the long poll: publishing is a no-op and watching returns an already
// closed channel so an accidental waiter wakes immediately instead of blocking forever.
func NewNoopNotifier[T any]() PubSubNotifier[T] {
	return noopNotifier[T]{}
}

func (noopNotifier[T]) Notify(definition.WorkflowKey, T) {}

func (noopNotifier[T]) Watch(definition.WorkflowKey) (string, <-chan T, error) {
	ch := make(chan T)
	close(ch)
	return "", ch, nil
}

func (noopNotifier[T]) Unwatch(definition.WorkflowKey, string) error { return nil }

// NewPubSubNotifier creates a notifier that admits at most maxSubscribersPerKey
// concurrent waiters per key; Watch beyond that returns a ResourceExhausted error.
func NewPubSubNotifier[T any](workflowIDToShardID func(namespace.ID, string) int32, maxSubscribersPerKey int) PubSubNotifier[T] {
	hashFn := func(key any) uint32 {
		wk, ok := key.(definition.WorkflowKey)
		if !ok {
			return 0
		}
		return uint32(workflowIDToShardID(namespace.ID(wk.NamespaceID), wk.WorkflowID))
	}
	return &pubSubNotifierImpl[T]{
		workflowIDToShardID:  workflowIDToShardID,
		maxSubscribersPerKey: maxSubscribersPerKey,
		subscriptions:        collection.NewShardedConcurrentTxMap(1024, hashFn),
	}
}

func (n *pubSubNotifierImpl[T]) Watch(key definition.WorkflowKey) (string, <-chan T, error) {
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

func (n *pubSubNotifierImpl[T]) Unwatch(key definition.WorkflowKey, subscriberID string) error {
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

func (n *pubSubNotifierImpl[T]) Notify(key definition.WorkflowKey, value T) {
	_, _, _ = n.subscriptions.GetAndDo(key, func(_ any, existing any) error {
		subscribers, ok := existing.(map[string]chan T)
		if !ok {
			return nil
		}
		for _, channel := range subscribers {
			select {
			case channel <- value:
			default:
				// Buffered (size 1) channel already holds a pending value; the waiter
				// re-reads authoritative state on wake, so dropping is safe.
			}
		}
		return nil
	})
}
