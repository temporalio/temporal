package history

import (
	"sync"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
)

// TimeSkippingFastForwardNotifier broadcasts time-skipping fast-forward updates to all waiters of an
// execution (e.g. DescribeWorkflowExecution long-polls). Unlike ChasmNotifier, it delivers a payload
// so waiters can act on the fresh TimeSkippingInfo without re-reading mutable state. Each subscriber
// gets its own coalescing channel (buffer of one holding the latest update) so a terminal update is
// never lost even if the subscriber is momentarily busy.
type TimeSkippingFastForwardNotifier struct {
	lock        sync.Mutex
	nextID      int64
	subscribers map[chasm.ExecutionKey]map[int64]chan *commonpb.TimeSkippingInfo
}

func NewTimeSkippingFastForwardNotifier() *TimeSkippingFastForwardNotifier {
	return &TimeSkippingFastForwardNotifier{
		subscribers: make(map[chasm.ExecutionKey]map[int64]chan *commonpb.TimeSkippingInfo),
	}
}

// Subscribe returns a channel that receives the current TimeSkippingInfo whenever the execution's
// fast-forward changes, along with an unsubscribe function. The channel is coalescing: it only ever
// holds the latest update. The caller must call unsubscribe when done; it is safe to call multiple
// times.
func (n *TimeSkippingFastForwardNotifier) Subscribe(
	key chasm.ExecutionKey,
) (<-chan *commonpb.TimeSkippingInfo, func()) {
	n.lock.Lock()
	defer n.lock.Unlock()

	id := n.nextID
	n.nextID++
	ch := make(chan *commonpb.TimeSkippingInfo, 1)
	byID, ok := n.subscribers[key]
	if !ok {
		byID = make(map[int64]chan *commonpb.TimeSkippingInfo)
		n.subscribers[key] = byID
	}
	byID[id] = ch

	return ch, sync.OnceFunc(func() {
		n.lock.Lock()
		defer n.lock.Unlock()
		if byID, ok := n.subscribers[key]; ok {
			delete(byID, id)
			if len(byID) == 0 {
				delete(n.subscribers, key)
			}
		}
	})
}

// Notify delivers info to every subscriber of key, replacing any pending update so each subscriber
// always sees the latest.
func (n *TimeSkippingFastForwardNotifier) Notify(key chasm.ExecutionKey, info *commonpb.TimeSkippingInfo) {
	n.lock.Lock()
	defer n.lock.Unlock()
	for _, ch := range n.subscribers[key] {
		// Drop a stale pending update, then enqueue the latest; both sends are non-blocking.
		select {
		case <-ch:
		default:
		}
		select {
		case ch <- info:
		default:
		}
	}
}
