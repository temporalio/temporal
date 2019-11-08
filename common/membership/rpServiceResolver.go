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
	"time"

	"github.com/dgryski/go-farm"
	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
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
	port       int
	isStarted  bool
	isStopped  bool
	rp         *ringpop.Ringpop
	shutdownCh chan struct{}
	shutdownWG sync.WaitGroup
	logger     log.Logger

	ringLock sync.RWMutex
	ring     *hashring.HashRing

	listenerLock sync.RWMutex
	listeners    map[string]chan<- *ChangedEvent
}

var _ ServiceResolver = (*ringpopServiceResolver)(nil)

func newRingpopServiceResolver(service string, port int, rp *ringpop.Ringpop, logger log.Logger) *ringpopServiceResolver {
	return &ringpopServiceResolver{
		service:    service,
		port:       port,
		rp:         rp,
		logger:     logger.WithTags(tag.ComponentServiceResolver, tag.Service(service)),
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

	r.isStopped = true
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

	serviceAddress, err := replaceServicePort(addr, r.port)
	if err != nil {
		return nil, err
	}
	return NewHostInfo(serviceAddress, r.getLabelsMap()), nil
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
		// This will happen when service stop and destroy ringpop while there are go-routines pending to call this.
		r.logger.Warn("Error during ringpop refresh.", tag.Error(err))
		return
	}

	for _, addr := range addrs {
		host := NewHostInfo(addr, r.getLabelsMap())
		r.ring.AddMembers(host)
	}

	r.logger.Debug("Current reachable members", tag.Addresses(addrs))
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
