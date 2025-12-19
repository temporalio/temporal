package history

import (
	"sync"

	"go.temporal.io/server/chasm"
)

type subscriptionTracker struct {
	ch             chan struct{}
	numSubscribers int
}

// ChasmNotifier allows subscribers to receive notifications relating to a CHASM execution.
type ChasmNotifier struct {
	// TODO(saa-preview): use ShardedConcurrentTxMap
	executions map[chasm.ExecutionKey]*subscriptionTracker
	// TODO(saa-preview): consider RWMutex
	lock sync.Mutex
}

// NewChasmNotifier creates a new instance of ChasmNotifier.
func NewChasmNotifier() *ChasmNotifier {
	return &ChasmNotifier{
		executions: make(map[chasm.ExecutionKey]*subscriptionTracker),
	}
}

// Subscribe returns a channel that will be closed when there is a notification relating to the
// execution, along with an unsubscribe function. No data will be written to the channel: on
// notification, the caller should determine whether the execution state they are waiting for has
// been reached and resubscribe if necessary, while holding a lock on the execution. The caller must
// arrange for the unsubscribe function to be called when they have finished monitoring the channel
// for notifications. It is safe to call the unsubscribe function multiple times and concurrently.
func (n *ChasmNotifier) Subscribe(key chasm.ExecutionKey) (<-chan struct{}, func()) {
	n.lock.Lock()
	defer n.lock.Unlock()
	s, ok := n.executions[key]
	if !ok {
		s = &subscriptionTracker{ch: make(chan struct{})}
		n.executions[key] = s
	}
	s.numSubscribers++
	return s.ch, sync.OnceFunc(func() {
		n.lock.Lock()
		defer n.lock.Unlock()
		if n.executions[key] == s {
			s.numSubscribers--
			if s.numSubscribers == 0 {
				delete(n.executions, key)
			}
		}
	})
}

// Notify notifies all subscribers subscribed to key by closing the channel.
func (n *ChasmNotifier) Notify(key chasm.ExecutionKey) {
	n.lock.Lock()
	defer n.lock.Unlock()
	if s, ok := n.executions[key]; ok {
		close(s.ch)
		delete(n.executions, key)
	}
}
