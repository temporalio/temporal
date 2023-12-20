// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package ringpop

import (
	"errors"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/temporalio/ringpop-go"
	"github.com/temporalio/tchannel-go"
	"golang.org/x/exp/slices"

	"github.com/dgryski/go-farm"
	"github.com/temporalio/ringpop-go/events"
	"github.com/temporalio/ringpop-go/hashring"
	rpmembership "github.com/temporalio/ringpop-go/membership"
	"github.com/temporalio/ringpop-go/swim"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/util"
)

const (
	// roleKey label is set by every single service as soon as it bootstraps its
	// ringpop instance. The data for this key is the service name
	roleKey = "serviceName"

	// portKey label is set by every single service as soon as it bootstraps its
	// ringpop instance. The data for this key represents the TCP port through which
	// the service can be accessed.
	portKey = "servicePort"

	minRefreshInternal     = time.Second * 4
	defaultRefreshInterval = time.Second * 10
	replicaPoints          = 100
)

type (
	serviceResolver struct {
		service     primitives.ServiceName
		port        int
		rp          *ringpop.Ringpop
		refreshChan chan struct{}
		shutdownCh  chan struct{}
		shutdownWG  sync.WaitGroup
		logger      log.Logger

		ringAndHosts atomic.Value // holds a ringAndHosts

		refreshLock     sync.Mutex
		lastRefreshTime time.Time

		listenerLock sync.RWMutex
		listeners    map[string]chan<- *membership.ChangedEvent
	}

	ringAndHosts struct {
		// We need to store a separate map from address to hostInfo because HashRing doesn't
		// return the rpmembership.Member that was passed to it, it only returns the address.
		ring  *hashring.HashRing
		hosts map[string]*hostInfo
	}
)

var _ membership.ServiceResolver = (*serviceResolver)(nil)

func newServiceResolver(
	service primitives.ServiceName,
	port int,
	rp *ringpop.Ringpop,
	logger log.Logger,
) *serviceResolver {
	resolver := &serviceResolver{
		service:     service,
		port:        port,
		rp:          rp,
		refreshChan: make(chan struct{}),
		shutdownCh:  make(chan struct{}),
		logger:      log.With(logger, tag.ComponentServiceResolver, tag.Service(service)),
		listeners:   make(map[string]chan<- *membership.ChangedEvent),
	}
	resolver.ringAndHosts.Store(ringAndHosts{
		ring:  newHashRing(),
		hosts: make(map[string]*hostInfo),
	})
	return resolver
}

func newHashRing() *hashring.HashRing {
	return hashring.New(farm.Fingerprint32, replicaPoints)
}

// Start starts the oracle
func (r *serviceResolver) Start() {
	r.rp.AddListener(r)
	if err := r.refresh(); err != nil {
		r.logger.Fatal("unable to start ring pop service resolver", tag.Error(err))
	}

	r.shutdownWG.Add(1)
	go r.refreshRingWorker()
}

// Stop stops the resolver
func (r *serviceResolver) Stop() {
	r.listenerLock.Lock()
	defer r.listenerLock.Unlock()
	r.rp.RemoveListener(r)
	r.ringAndHosts.Store(ringAndHosts{
		ring:  newHashRing(),
		hosts: nil,
	})
	r.listeners = make(map[string]chan<- *membership.ChangedEvent)
	close(r.shutdownCh)

	if success := common.AwaitWaitGroup(&r.shutdownWG, time.Minute); !success {
		r.logger.Warn("service resolver timed out on shutdown.")
	}
}

func (r *serviceResolver) RequestRefresh() {
	select {
	case r.refreshChan <- struct{}{}:
	default:
	}
}

// Lookup finds the host in the ring responsible for serving the given key
func (r *serviceResolver) Lookup(key string) (membership.HostInfo, error) {
	ring, hosts := r.ring()
	addr, found := ring.Lookup(key)
	if !found {
		r.RequestRefresh()
		return nil, membership.ErrInsufficientHosts
	}
	return hosts[addr], nil
}

func (r *serviceResolver) LookupN(key string, n int) []membership.HostInfo {
	if n <= 0 {
		return nil
	}
	ring, hosts := r.ring()
	addrs := ring.LookupN(key, n)
	if len(addrs) == 0 {
		r.RequestRefresh()
		return nil
	}
	return util.MapSlice(addrs, func(addr string) membership.HostInfo { return hosts[addr] })
}

func (r *serviceResolver) AddListener(
	name string,
	notifyChannel chan<- *membership.ChangedEvent,
) error {
	r.listenerLock.Lock()
	defer r.listenerLock.Unlock()
	_, ok := r.listeners[name]
	if ok {
		return membership.ErrListenerAlreadyExist
	}
	r.listeners[name] = notifyChannel
	return nil
}

func (r *serviceResolver) RemoveListener(
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

func (r *serviceResolver) MemberCount() int {
	_, hosts := r.ring()
	return len(hosts)
}

func (r *serviceResolver) Members() []membership.HostInfo {
	_, hosts := r.ring()
	servers := make([]membership.HostInfo, 0, len(hosts))
	for _, host := range hosts {
		servers = append(servers, host)
	}
	return servers
}

// HandleEvent handles updates from ringpop
func (r *serviceResolver) HandleEvent(
	event events.Event,
) {
	// We only care about RingChangedEvent
	if _, ok := event.(events.RingChangedEvent); ok {
		r.logger.Debug("Received a ring changed event")
		// Note that we receive events asynchronously, possibly out of order.
		// We cannot rely on the content of the event, rather we load everything
		// from ringpop when we get a notification that something changed.
		if err := r.refresh(); err != nil {
			r.logger.Error("error refreshing ring when receiving a ring changed event", tag.Error(err))
		}
	}
}

func (r *serviceResolver) refresh() error {
	var event *membership.ChangedEvent
	var err error
	defer func() {
		if event != nil {
			r.emitEvent(event)
		}
	}()
	r.refreshLock.Lock()
	defer r.refreshLock.Unlock()
	event, err = r.refreshLocked()
	return err
}

func (r *serviceResolver) refreshWithBackoff() error {
	var event *membership.ChangedEvent
	var err error
	defer func() {
		if event != nil {
			r.emitEvent(event)
		}
	}()
	r.refreshLock.Lock()
	defer r.refreshLock.Unlock()
	if r.lastRefreshTime.After(time.Now().UTC().Add(-minRefreshInternal)) {
		// refresh too frequently
		return nil
	}
	event, err = r.refreshLocked()
	return err
}

func (r *serviceResolver) refreshLocked() (*membership.ChangedEvent, error) {
	hosts, err := r.getReachableMembers()
	if err != nil {
		return nil, err
	}

	newMembersMap, changedEvent := r.compareMembers(hosts)
	if changedEvent == nil {
		return nil, nil
	}

	ring := newHashRing()
	ring.AddMembers(util.MapSlice(hosts, func(h *hostInfo) rpmembership.Member { return h })...)

	r.lastRefreshTime = time.Now().UTC()
	r.ringAndHosts.Store(ringAndHosts{
		ring:  ring,
		hosts: newMembersMap,
	})

	addrs := util.MapSlice(hosts, func(h *hostInfo) string { return h.summary() })
	slices.Sort(addrs)
	r.logger.Info("Current reachable members", tag.Addresses(addrs))

	return changedEvent, nil
}

func (r *serviceResolver) getReachableMembers() ([]*hostInfo, error) {
	members, err := r.rp.GetReachableMemberObjects(swim.MemberWithLabelAndValue(roleKey, string(r.service)))
	if err != nil {
		return nil, err
	}

	hosts := make([]*hostInfo, len(members))
	for i, member := range members {
		servicePort := r.port

		// Each temporal service in the ring should advertise which port it has its gRPC listener
		// on via a service label. If we cannot find the label, we will assume that the
		// temporal service is listening on the same port that this node is listening on.
		servicePortLabel, ok := member.Label(portKey)
		if ok {
			servicePort, err = strconv.Atoi(servicePortLabel)
			if err != nil {
				return nil, err
			}
		} else {
			r.logger.Debug("unable to find roleport label for ringpop member. using local service's port", tag.Service(r.service))
		}

		hostPort, err := replaceServicePort(member.Address, servicePort)
		if err != nil {
			return nil, err
		}

		// We can share member.Labels without copying since we never modify it.
		hosts[i] = newHostInfo(hostPort, member.Labels)
	}

	return hosts, nil
}

func (r *serviceResolver) emitEvent(event *membership.ChangedEvent) {
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

func (r *serviceResolver) refreshRingWorker() {
	defer r.shutdownWG.Done()

	refreshTicker := time.NewTicker(defaultRefreshInterval)
	defer refreshTicker.Stop()

	for {
		select {
		case <-r.shutdownCh:
			return
		case <-r.refreshChan:
			if err := r.refreshWithBackoff(); err != nil {
				r.logger.Error("error refreshing ring by request", tag.Error(err))
			}
		case <-refreshTicker.C:
			if err := r.refreshWithBackoff(); err != nil {
				r.logger.Error("error periodically refreshing ring", tag.Error(err))
			}
		}
	}
}

func (r *serviceResolver) ring() (*hashring.HashRing, map[string]*hostInfo) {
	ring := r.ringAndHosts.Load().(ringAndHosts)
	return ring.ring, ring.hosts
}

func (r *serviceResolver) compareMembers(hosts []*hostInfo) (map[string]*hostInfo, *membership.ChangedEvent) {
	event := &membership.ChangedEvent{}
	changed := false
	_, prevHosts := r.ring() // note that this is always called with refreshLock so we can't miss an update here
	newMembersMap := make(map[string]*hostInfo, len(hosts))
	for _, host := range hosts {
		newMembersMap[host.GetAddress()] = host
		if prev, ok := prevHosts[host.GetAddress()]; !ok {
			event.HostsAdded = append(event.HostsAdded, host)
			changed = true
		} else if prev.labelsChecksum != host.labelsChecksum {
			event.HostsChanged = append(event.HostsChanged, host)
			changed = true
		}
	}
	for addr, prev := range prevHosts {
		if _, ok := newMembersMap[addr]; !ok {
			event.HostsRemoved = append(event.HostsRemoved, prev)
			changed = true
		}
	}
	if changed {
		return newMembersMap, event
	}
	return newMembersMap, nil
}

// buildBroadcastHostPort return the listener hostport from an existing tchannel
// and overrides the address with broadcastAddress if specified
func buildBroadcastHostPort(listenerPeerInfo tchannel.LocalPeerInfo, broadcastAddress string) (string, error) {
	// Ephemeral port check copied from ringpop-go/ringpop.go/channelAddressResolver
	// Check that TChannel is listening on a real hostport. By default,
	// TChannel listens on an ephemeral host/port. The real port is then
	// assigned by the OS when ListenAndServe is called. If the hostport is
	// ephemeral, it means TChannel is not yet listening and the hostport
	// cannot be resolved.
	if listenerPeerInfo.IsEphemeralHostPort() {
		return "", ringpop.ErrEphemeralAddress
	}

	// Parse listener hostport
	listenerIPString, port, err := net.SplitHostPort(listenerPeerInfo.HostPort)
	if err != nil {
		return "", err
	}

	// Broadcast IP override
	if broadcastAddress != "" {
		// Parse supplied broadcastAddress override
		ip := net.ParseIP(broadcastAddress)
		if ip == nil {
			return "", errors.New("broadcastAddress set but unknown failure encountered while parsing")
		}

		// If no errors, use the parsed IP with the port from our listener
		return net.JoinHostPort(ip.String(), port), nil
	}

	listenerIP := net.ParseIP(listenerIPString)
	if listenerIP == nil {
		return "", errors.New("unable to parse listenerIP")
	}

	if listenerIP.IsUnspecified() {
		return "", errors.New("broadcastAddress required when listening on all interfaces (0.0.0.0/[::])")
	}

	return listenerPeerInfo.HostPort, nil
}
