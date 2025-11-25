// This file implements ChasmNotifier, which allows subscribers to subscribe to notifications
// relating to components in a specified CHASM execution. It is based on the events.Notifier
// implementation.
package history

import (
	"sync"

	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/metrics"
)

type (
	// ChasmNotifier allows subscribers to subscribe to notifications relating to a CHASM execution.
	ChasmNotifier struct {
		executions map[chasm.EntityKey]chan struct{}
		lock       sync.Mutex
	}
)

// NewChasmNotifier creates a new instance of ChasmNotifier allowing subscribers to receive
// notifications relating to a CHASM execution.
func NewChasmNotifier(metricsHandler metrics.Handler) *ChasmNotifier {
	return &ChasmNotifier{
		executions: make(map[chasm.EntityKey]chan struct{}),
	}
}

// Subscribe returns a channel that will be closed when there is a notification relating to the
// execution. No data will be written to the channel.
func (n *ChasmNotifier) Subscribe(key chasm.EntityKey) (<-chan struct{}, error) {
	var ch chan struct{}
	var ok bool
	n.lock.Lock()
	defer n.lock.Unlock()
	if ch, ok = n.executions[key]; !ok {
		ch = make(chan struct{})
		n.executions[key] = ch
	}
	return ch, nil
}

// Notify notifies all subscribers subscribed to key by closing the channel.
func (n *ChasmNotifier) Notify(key chasm.EntityKey) {
	// If key exists, notify all subscribers by closing the channel, and create a new channel for
	// subsequent notifications.
	n.lock.Lock()
	defer n.lock.Unlock()
	if ch, ok := n.executions[key]; ok {
		close(ch)
		n.executions[key] = make(chan struct{})
	}
}
