// Package ffnotifier provides an in-memory pub/sub used by the
// PollWorkflowExecutionTimeSkipping long poll: a waiter subscribes on a
// workflow's (namespace, workflowID) key and is woken when that workflow's
// fast-forward state changes. It mirrors service/history/events.Notifier but
// carries a fast-forward payload and keys on execution rather than run.
package ffnotifier

import (
	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
)

type (
	// Notification carries the current fast-forward info to waiters. Key
	// identifies the execution by namespace + workflowID (RunID is empty).
	// Closed is true when the run reached a terminal state without a
	// continuation (no retry / cron / continue-as-new), meaning a pending
	// fast-forward can no longer complete.
	Notification struct {
		Key             definition.WorkflowKey
		FastForwardInfo *commonpb.TimeSkippingFastForwardInfo
		Closed          bool
	}

	Notifier interface {
		NotifyFastForwardUpdate(notification *Notification)
		Watch(key definition.WorkflowKey) (subscriberID string, channel <-chan *Notification, err error)
		Unwatch(key definition.WorkflowKey, subscriberID string) error
	}

	NotifierImpl struct {
		workflowIDToShardID func(namespace.ID, string) int32
		// key: definition.WorkflowKey, value: map[subscriberID]chan *Notification.
		// The inner map is not thread-safe on its own; every access is guarded by
		// the ConcurrentTxMap's per-key action callbacks. Subscribers per workflow
		// are expected to be few.
		subscriptions collection.ConcurrentTxMap
	}
)

var _ Notifier = (*NotifierImpl)(nil)

// NewKey builds the subscription key for an execution. Fast-forward waiters key
// on namespace + workflowID only, so RunID is always empty.
func NewKey(namespaceID string, workflowID string) definition.WorkflowKey {
	return definition.NewWorkflowKey(namespaceID, workflowID, "")
}

func NewNotifier(workflowIDToShardID func(namespace.ID, string) int32) *NotifierImpl {
	hashFn := func(key any) uint32 {
		wk, ok := key.(definition.WorkflowKey)
		if !ok {
			return 0
		}
		return uint32(workflowIDToShardID(namespace.ID(wk.NamespaceID), wk.WorkflowID))
	}
	return &NotifierImpl{
		workflowIDToShardID: workflowIDToShardID,
		subscriptions:       collection.NewShardedConcurrentTxMap(1024, hashFn),
	}
}

func (n *NotifierImpl) Watch(key definition.WorkflowKey) (string, <-chan *Notification, error) {
	channel := make(chan *Notification, 1)
	subscriberID := uuid.NewString()
	subscribers := map[string]chan *Notification{subscriberID: channel}

	_, _, err := n.subscriptions.PutOrDo(key, subscribers, func(_ any, value any) error {
		existing, ok := value.(map[string]chan *Notification)
		if !ok {
			return serviceerror.NewInternal("unexpected subscription value type")
		}
		if _, ok := existing[subscriberID]; ok {
			return serviceerror.NewUnavailable("unable to watch fast-forward updates")
		}
		existing[subscriberID] = channel
		return nil
	})
	if err != nil {
		return "", nil, err
	}
	return subscriberID, channel, nil
}

func (n *NotifierImpl) Unwatch(key definition.WorkflowKey, subscriberID string) error {
	success := true
	n.subscriptions.RemoveIf(key, func(_ any, value any) bool {
		subscribers, ok := value.(map[string]chan *Notification)
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
		return serviceerror.NewInternal("unable to unwatch fast-forward updates")
	}
	return nil
}

func (n *NotifierImpl) NotifyFastForwardUpdate(notification *Notification) {
	_, _, _ = n.subscriptions.GetAndDo(notification.Key, func(_ any, value any) error {
		subscribers, ok := value.(map[string]chan *Notification)
		if !ok {
			return nil
		}
		for _, channel := range subscribers {
			select {
			case channel <- notification:
			default:
				// Buffered (size 1) channel already holds a pending notification;
				// the waiter re-reads current state on wake, so dropping is safe.
			}
		}
		return nil
	})
}
