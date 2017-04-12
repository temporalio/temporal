package membership

import (
	"sync"
	"time"

	"github.com/uber/cadence/common"

	"github.com/dgryski/go-farm"
	"github.com/uber-common/bark"
	"github.com/uber/ringpop-go"
	"github.com/uber/ringpop-go/events"
	"github.com/uber/ringpop-go/hashring"
	"github.com/uber/ringpop-go/swim"
)

const (
	// RoleKey label is set by every single service as soon as it bootstraps its
	// ringpop instance. The data for this key is the service name
	RoleKey                = "serviceName"
	defaultRefreshInterval = time.Second * 10
	replicaPoints          = 100
)

type ringpopServiceResolver struct {
	service    string
	isStarted  bool
	isStopped  bool
	rp         *ringpop.Ringpop
	shutdownCh chan struct{}
	shutdownWG sync.WaitGroup
	logger     bark.Logger

	ringLock sync.RWMutex
	ring     *hashring.HashRing

	listenerLock sync.RWMutex
	listeners    map[string]chan<- *ChangedEvent
}

var _ ServiceResolver = (*ringpopServiceResolver)(nil)

func newRingpopServiceResolver(service string, rp *ringpop.Ringpop, logger bark.Logger) *ringpopServiceResolver {
	return &ringpopServiceResolver{
		service:    service,
		rp:         rp,
		logger:     logger.WithFields(bark.Fields{"component": "ServiceResolver", RoleKey: service}),
		ring:       hashring.New(farm.Fingerprint32, replicaPoints),
		listeners:  make(map[string]chan<- *ChangedEvent),
		shutdownCh: make(chan struct{}),
	}
}

// Start starts the oracle
func (r *ringpopServiceResolver) Start() error {
	r.ringLock.Lock()
	defer r.ringLock.Unlock()

	if r.isStarted {
		return nil
	}

	r.rp.AddListener(r)
	addrs, err := r.rp.GetReachableMembers(swim.MemberWithLabelAndValue(RoleKey, r.service))
	if err != nil {
		return err
	}

	for _, addr := range addrs {
		labels := r.getLabelsMap()
		r.ring.AddMembers(NewHostInfo(addr, labels))
	}

	r.shutdownWG.Add(1)
	go r.refreshRingWorker()

	r.isStarted = true
	return nil
}

// Stop stops the resolver
func (r *ringpopServiceResolver) Stop() error {
	r.ringLock.Lock()
	defer r.ringLock.Unlock()
	r.listenerLock.Lock()
	defer r.listenerLock.Unlock()

	if r.isStopped {
		return nil
	}

	if r.isStarted {
		r.rp.RemoveListener(r)
		r.ring = hashring.New(farm.Fingerprint32, replicaPoints)
		r.listeners = make(map[string]chan<- *ChangedEvent)
		close(r.shutdownCh)
	}

	if success := common.AwaitWaitGroup(&r.shutdownWG, time.Minute); !success {
		r.logger.Warn("service resolver timed out on shutdown.")
	}
	return nil
}

// Lookup finds the host in the ring responsible for serving the given key
func (r *ringpopServiceResolver) Lookup(key string) (*HostInfo, error) {
	r.ringLock.RLock()
	defer r.ringLock.RUnlock()
	addr, found := r.ring.Lookup(key)
	if !found {
		return nil, ErrInsufficientHosts
	}
	return NewHostInfo(addr, r.getLabelsMap()), nil
}

func (r *ringpopServiceResolver) AddListener(name string, notifyChannel chan<- *ChangedEvent) error {
	r.listenerLock.Lock()
	defer r.listenerLock.Unlock()
	_, ok := r.listeners[name]
	if ok {
		return ErrListenerAlreadyExist
	}
	r.listeners[name] = notifyChannel
	return nil
}

func (r *ringpopServiceResolver) RemoveListener(name string) error {
	r.listenerLock.Lock()
	defer r.listenerLock.Unlock()
	_, ok := r.listeners[name]
	if !ok {
		return nil
	}
	delete(r.listeners, name)
	return nil
}

// HandleEvent handles updates from ringpop
func (r *ringpopServiceResolver) HandleEvent(event events.Event) {
	// We only care about RingChangedEvent
	e, ok := event.(events.RingChangedEvent)
	if ok {
		r.logger.Info("Received a ring changed event")
		// Note that we receive events asynchronously, possibly out of order.
		// We cannot rely on the content of the event, rather we load everything
		// from ringpop when we get a notification that something changed.
		r.refresh()
		r.emitEvent(e)
	}
}

func (r *ringpopServiceResolver) refresh() {
	r.ringLock.Lock()
	defer r.ringLock.Unlock()

	r.ring = hashring.New(farm.Fingerprint32, replicaPoints)

	addrs, err := r.rp.GetReachableMembers(swim.MemberWithLabelAndValue(RoleKey, r.service))
	if err != nil {
		// This should never happen!
		r.logger.Fatalf("Error during ringpop refresh.  Error: %v", err)
	}

	for _, addr := range addrs {
		host := NewHostInfo(addr, r.getLabelsMap())
		r.ring.AddMembers(host)
	}

	r.logger.Debugf("Current reachable members: %v", addrs)
}

func (r *ringpopServiceResolver) emitEvent(rpEvent events.RingChangedEvent) {
	// Marshall the event object into the required type
	event := &ChangedEvent{}
	for _, addr := range rpEvent.ServersAdded {
		event.HostsAdded = append(event.HostsAdded, NewHostInfo(addr, r.getLabelsMap()))
	}
	for _, addr := range rpEvent.ServersRemoved {
		event.HostsRemoved = append(event.HostsRemoved, NewHostInfo(addr, r.getLabelsMap()))
	}
	for _, addr := range rpEvent.ServersUpdated {
		event.HostsUpdated = append(event.HostsUpdated, NewHostInfo(addr, r.getLabelsMap()))
	}

	// Notify listeners
	r.listenerLock.RLock()
	defer r.listenerLock.RUnlock()

	for name, ch := range r.listeners {
		select {
		case ch <- event:
		default:
			r.logger.WithFields(bark.Fields{`listenerName`: name}).Error("Failed to send listener notification, channel full")
		}
	}
}

func (r *ringpopServiceResolver) refreshRingWorker() {
	defer r.shutdownWG.Done()

	refreshTicker := time.NewTicker(defaultRefreshInterval)
	defer refreshTicker.Stop()

	for {
		select {
		case <-r.shutdownCh:
			return
		case <-refreshTicker.C:
			r.refresh()
		}
	}
}

func (r *ringpopServiceResolver) getLabelsMap() map[string]string {
	labels := make(map[string]string)
	labels[RoleKey] = r.service
	return labels
}
