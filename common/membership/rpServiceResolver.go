// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package membership

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/uber/ringpop-go/events"
	"github.com/uber/ringpop-go/hashring"
	"github.com/uber/ringpop-go/swim"

	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
)

const (
	// RoleKey label is set by every single service as soon as it bootstraps its
	// ringpop instance. The data for this key is the service name
	RoleKey                = "serviceName"
	minRefreshInternal     = time.Second * 4
	defaultRefreshInterval = time.Second * 10
	replicaPoints          = 100
)

type ringpopServiceResolver struct {
	status      int32
	service     string
	rp          *RingPop
	refreshChan chan struct{}
	shutdownCh  chan struct{}
	shutdownWG  sync.WaitGroup
	logger      log.Logger

	ringLock            sync.RWMutex
	ringLastRefreshTime time.Time
	ring                *hashring.HashRing

	listenerLock sync.RWMutex
	listeners    map[string]chan<- *ChangedEvent
}

var _ ServiceResolver = (*ringpopServiceResolver)(nil)

func newRingpopServiceResolver(
	service string,
	rp *RingPop,
	logger log.Logger,
) *ringpopServiceResolver {

	return &ringpopServiceResolver{
		status:      common.DaemonStatusInitialized,
		service:     service,
		rp:          rp,
		refreshChan: make(chan struct{}),
		shutdownCh:  make(chan struct{}),
		logger:      logger.WithTags(tag.ComponentServiceResolver, tag.Service(service)),

		ring: hashring.New(farm.Fingerprint32, replicaPoints),

		listeners: make(map[string]chan<- *ChangedEvent),
	}
}

// Start starts the oracle
func (r *ringpopServiceResolver) Start() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	r.rp.AddListener(r)
	if err := r.refresh(); err != nil {
		r.logger.Fatal("unable to start ring pop service resolver", tag.Error(err))
	}

	r.shutdownWG.Add(1)
	go r.refreshRingWorker()
}

// Stop stops the resolver
func (r *ringpopServiceResolver) Stop() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	r.ringLock.Lock()
	defer r.ringLock.Unlock()
	r.listenerLock.Lock()
	defer r.listenerLock.Unlock()
	r.rp.RemoveListener(r)
	r.ring = hashring.New(farm.Fingerprint32, replicaPoints)
	r.listeners = make(map[string]chan<- *ChangedEvent)
	close(r.shutdownCh)

	if success := common.AwaitWaitGroup(&r.shutdownWG, time.Minute); !success {
		r.logger.Warn("service resolver timed out on shutdown.")
	}
}

// Lookup finds the host in the ring responsible for serving the given key
func (r *ringpopServiceResolver) Lookup(
	key string,
) (*HostInfo, error) {

	r.ringLock.RLock()
	defer r.ringLock.RUnlock()
	addr, found := r.ring.Lookup(key)
	if !found {
		select {
		case r.refreshChan <- struct{}{}:
		default:
		}
		return nil, ErrInsufficientHosts
	}
	return NewHostInfo(addr, r.getLabelsMap()), nil
}

func (r *ringpopServiceResolver) AddListener(
	name string,
	notifyChannel chan<- *ChangedEvent,
) error {

	r.listenerLock.Lock()
	defer r.listenerLock.Unlock()
	_, ok := r.listeners[name]
	if ok {
		return ErrListenerAlreadyExist
	}
	r.listeners[name] = notifyChannel
	return nil
}

func (r *ringpopServiceResolver) RemoveListener(
	name string,
) error {

	r.listenerLock.Lock()
	defer r.listenerLock.Unlock()
	_, ok := r.listeners[name]
	if !ok {
		return nil
	}
	delete(r.listeners, name)
	return nil
}

func (r *ringpopServiceResolver) MemberCount() int {
	return r.ring.ServerCount()
}

func (r *ringpopServiceResolver) Members() []*HostInfo {
	var servers []*HostInfo
	for _, s := range r.ring.Servers() {
		servers = append(servers, NewHostInfo(s, r.getLabelsMap()))
	}

	return servers
}

// HandleEvent handles updates from ringpop
func (r *ringpopServiceResolver) HandleEvent(
	event events.Event,
) {

	// We only care about RingChangedEvent
	e, ok := event.(events.RingChangedEvent)
	if ok {
		r.logger.Info("Received a ring changed event")
		// Note that we receive events asynchronously, possibly out of order.
		// We cannot rely on the content of the event, rather we load everything
		// from ringpop when we get a notification that something changed.
		if err := r.refresh(); err != nil {
			r.logger.Error("error refreshing ring when receiving a ring changed event", tag.Error(err))
		}
		r.emitEvent(e)
	}
}

func (r *ringpopServiceResolver) refresh() error {
	r.ringLock.Lock()
	defer r.ringLock.Unlock()

	if r.ringLastRefreshTime.After(time.Now().Add(-minRefreshInternal)) {
		// refresh too frequently
		return nil
	}

	r.ring = hashring.New(farm.Fingerprint32, replicaPoints)

	addrs, err := r.rp.GetReachableMembers(swim.MemberWithLabelAndValue(RoleKey, r.service))
	if err != nil {
		return err
	}

	for _, addr := range addrs {
		host := NewHostInfo(addr, r.getLabelsMap())
		r.ring.AddMembers(host)
	}

	r.ringLastRefreshTime = time.Now()
	r.logger.Debug("Current reachable members", tag.Addresses(addrs))
	return nil
}

func (r *ringpopServiceResolver) emitEvent(
	rpEvent events.RingChangedEvent,
) {

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
			r.logger.Error("Failed to send listener notification, channel full", tag.ListenerName(name))
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
		case <-r.refreshChan:
			if err := r.refresh(); err != nil {
				r.logger.Error("error periodically refreshing ring", tag.Error(err))
			}
		case <-refreshTicker.C:
			if err := r.refresh(); err != nil {
				r.logger.Error("error periodically refreshing ring", tag.Error(err))
			}
		}
	}
}

func (r *ringpopServiceResolver) getLabelsMap() map[string]string {
	labels := make(map[string]string)
	labels[RoleKey] = r.service
	return labels
}
