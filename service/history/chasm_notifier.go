package history

import (
	"sync"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/metrics"
)

type (
	// ChasmNotifier allows subscribers to receive notifications relating to a CHASM execution.
	ChasmNotifier struct {
		executions map[chasm.EntityKey]chan struct{}
		lock       sync.Mutex
	}
)

// NewChasmNotifier creates a new instance of ChasmNotifier.
func NewChasmNotifier(metricsHandler metrics.Handler) *ChasmNotifier {
	return &ChasmNotifier{
		executions: make(map[chasm.EntityKey]chan struct{}),
	}
}

// Subscribe returns a channel that will be closed when there is a notification relating to the
// execution. No data will be written to the channel.
func (n *ChasmNotifier) Subscribe(key chasm.EntityKey) <-chan struct{} {
	n.lock.Lock()
	defer n.lock.Unlock()
	if ch, ok := n.executions[key]; ok {
		return ch
	}
	ch := make(chan struct{})
	n.executions[key] = ch
	return ch
}

// Notify notifies all subscribers subscribed to key by closing the channel.
func (n *ChasmNotifier) Notify(key chasm.EntityKey) {
	n.lock.Lock()
	defer n.lock.Unlock()
	if ch, ok := n.executions[key]; ok {
		close(ch)
		delete(n.executions, key)
	}
}
