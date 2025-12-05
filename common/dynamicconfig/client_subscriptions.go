package dynamicconfig

import (
	"sync"

	expmaps "golang.org/x/exp/maps"
)

type (
	// NotifyingClientImpl implements NotifyingClient and is intended to be embedded in another struct.
	// NotifyingClientImpl must not be copied after first use.
	NotifyingClientImpl struct {
		subscriptionLock sync.Mutex
		subscriptionIdx  int
		subscriptions    map[int]ClientUpdateFunc
	}
)

var _ NotifyingClient = (*NotifyingClientImpl)(nil)

func NewNotifyingClientImpl() NotifyingClientImpl {
	return NotifyingClientImpl{subscriptions: make(map[int]ClientUpdateFunc)}
}

// Subscribe adds a subscription to all updates from this Client.
func (n *NotifyingClientImpl) Subscribe(f ClientUpdateFunc) (cancel func()) {
	n.subscriptionLock.Lock()
	defer n.subscriptionLock.Unlock()

	n.subscriptionIdx++
	id := n.subscriptionIdx
	n.subscriptions[id] = f

	return func() {
		n.subscriptionLock.Lock()
		defer n.subscriptionLock.Unlock()
		delete(n.subscriptions, id)
	}
}

// PublishUpdates calls all subscribed update functions with the changed keys.
func (n *NotifyingClientImpl) PublishUpdates(changed map[Key][]ConstrainedValue) {
	if len(changed) == 0 {
		return
	}

	n.subscriptionLock.Lock()
	subscriptions := expmaps.Values(n.subscriptions)
	n.subscriptionLock.Unlock()

	for _, update := range subscriptions {
		update(changed)
	}
}
