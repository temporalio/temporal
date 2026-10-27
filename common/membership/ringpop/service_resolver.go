package ringpop

import (
	"errors"
	"math"
	"net"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgryski/go-farm"
	"github.com/temporalio/ringpop-go"
	"github.com/temporalio/ringpop-go/events"
	"github.com/temporalio/ringpop-go/hashring"
	rpmembership "github.com/temporalio/ringpop-go/membership"
	"github.com/temporalio/ringpop-go/swim"
	"github.com/temporalio/tchannel-go"
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

	// draining label is set by frontend when it starts shutting down (the same period where it
	// would return "not serving" to grpc health checks). Data is `true` or `false` (missing
	// means false).
	drainingKey = "draining"

	// These labels control the visibility time of hosts in membership rings.
	// Value is seconds since unix epoch in decimal.
	startAtKey = "startAt"
	stopAtKey  = "stopAt"
	// These labels, if present, make the join/leave gradual: each key switches at a different
	// time between startAt/stopAt and that time plus this window. Value is seconds in decimal.
	startWindowKey = "startWin"
	stopWindowKey  = "stopWin"

	minRefreshInternal     = time.Second * 4
	defaultRefreshInterval = time.Second * 10
)

const (
	// refreshModeAlways means always do a refresh right now.
	refreshModeAlways refreshMode = iota
	// refreshModeLazy means only do a refresh if it's been minRefreshInternal since the last one.
	refreshModeLazy
)

type (
	serviceResolver struct {
		service       primitives.ServiceName
		port          int
		rp            *ringpop.Ringpop
		replicaPoints int
		refreshChan   chan struct{}
		shutdownCh    chan struct{}
		shutdownWG    sync.WaitGroup
		logger        log.Logger

		ringAndHosts atomic.Value // holds a ringAndHosts

		refreshLock         sync.Mutex
		lastRefreshTime     time.Time
		scheduledRefreshMap map[int64]*time.Timer

		listenerLock sync.RWMutex
		listeners    map[string]chan<- *membership.ChangedEvent
	}

	ringAndHosts struct {
		// We need to store a separate map from address to hostInfo because HashRing doesn't
		// return the rpmembership.Member that was passed to it, it only returns the address.
		ring  *hashring.HashRing
		hosts map[string]*hostInfo
		// Hosts that are joining or leaving, keyed by address. Hosts in the ring but not in
		// this map are present for all keys.
		presence map[string]*presence
		// End of the latest gradual change window.
		gradualUntil time.Time
	}

	refreshMode int
)

var _ membership.ServiceResolver = (*serviceResolver)(nil)

// errMissingLabel is not a real error, just a sentinel value
var errMissingLabel = errors.New("missing label")

func newServiceResolver(
	service primitives.ServiceName,
	port int,
	rp *ringpop.Ringpop,
	replicaPoints int,
	logger log.Logger,
) *serviceResolver {
	resolver := &serviceResolver{
		service:             service,
		port:                port,
		rp:                  rp,
		replicaPoints:       replicaPoints,
		refreshChan:         make(chan struct{}),
		shutdownCh:          make(chan struct{}),
		logger:              log.With(logger, tag.ComponentServiceResolver, tag.Service(service)),
		scheduledRefreshMap: make(map[int64]*time.Timer),
		listeners:           make(map[string]chan<- *membership.ChangedEvent),
	}
	resolver.ringAndHosts.Store(ringAndHosts{
		ring:  newHashRing(replicaPoints),
		hosts: make(map[string]*hostInfo),
	})
	return resolver
}

func newHashRing(replicaPoints int) *hashring.HashRing {
	return hashring.New(farm.Fingerprint32, replicaPoints)
}

// Start starts the oracle
func (r *serviceResolver) Start() {
	r.rp.AddListener(r)
	if err := r.refresh(refreshModeAlways); err != nil {
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
		ring:  newHashRing(r.replicaPoints),
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
	addrs, hosts := r.lookupN(key, 1, time.Now())
	if len(addrs) == 0 {
		r.RequestRefresh()
		return nil, membership.ErrInsufficientHosts
	}
	return hosts[addrs[0]], nil
}

func (r *serviceResolver) LookupN(key string, n int) []membership.HostInfo {
	if n <= 0 {
		return nil
	}
	addrs, hosts := r.lookupN(key, n, time.Now())
	if len(addrs) == 0 {
		r.RequestRefresh()
		return nil
	}
	return util.MapSlice(addrs, func(addr string) membership.HostInfo { return hosts[addr] })
}

func (r *serviceResolver) lookupN(key string, n int, now time.Time) ([]string, map[string]*hostInfo) {
	rh := r.loadRingAndHosts()
	if len(rh.presence) == 0 {
		return rh.ring.LookupN(key, n), rh.hosts
	}
	keyHash := gradualKeyHash(key)
	return rh.ring.LookupNFiltered(key, n, func(addr string) bool {
		p, ok := rh.presence[addr]
		return !ok || p.present(keyHash, now)
	}), rh.hosts
}

func (r *serviceResolver) GradualChangeUntil() time.Time {
	return r.loadRingAndHosts().gradualUntil
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

func (r *serviceResolver) AvailableMemberCount() int {
	_, hosts := r.ring()
	n := 0
	for _, host := range hosts {
		if !isDraining(host) {
			n++
		}
	}
	return n
}

func (r *serviceResolver) Members() []membership.HostInfo {
	_, hosts := r.ring()
	servers := make([]membership.HostInfo, 0, len(hosts))
	for _, host := range hosts {
		servers = append(servers, host)
	}
	return servers
}

func (r *serviceResolver) AvailableMembers() []membership.HostInfo {
	_, hosts := r.ring()
	var servers []membership.HostInfo
	for _, host := range hosts {
		if !isDraining(host) {
			servers = append(servers, host)
		}
	}
	return servers
}

// HandleEvent handles updates from ringpop
func (r *serviceResolver) HandleEvent(
	event events.Event,
) {
	// We only about membership.ChangeEvent. Normally ringpop converts membership.ChangeEvent
	// into events.RingChangedEvent when its internal hash ring changes, but since we construct
	// our own hash rings with filtering, we have to handle the lower-level event ourselves.
	if _, ok := event.(rpmembership.ChangeEvent); ok {
		r.logger.Debug("Received a ring changed event")
		// Note that we receive events asynchronously, possibly out of order.
		// We cannot rely on the content of the event, rather we load everything
		// from ringpop when we get a notification that something changed.
		if err := r.refresh(refreshModeAlways); err != nil {
			r.logger.Error("error refreshing ring when receiving a ring changed event", tag.Error(err))
		}
	}
}

func (r *serviceResolver) refresh(mode refreshMode) error {
	var event *membership.ChangedEvent
	var err error
	defer func() {
		if event != nil {
			r.emitEvent(event)
		}
	}()

	r.refreshLock.Lock()
	defer r.refreshLock.Unlock()

	if mode == refreshModeLazy && r.lastRefreshTime.After(time.Now().UTC().Add(-minRefreshInternal)) {
		return nil // refreshed too recently
	}

	event, err = r.refreshLocked()
	return err
}

func (r *serviceResolver) refreshLocked() (*membership.ChangedEvent, error) {
	hosts, err := r.getReachableMembers()
	if err != nil {
		return nil, err
	}

	hosts, presence, gradualUntil, nextEvent := filterMembersByTime(hosts, time.Now())

	// if we found an add/remove event, schedule another refresh right at that time
	r.scheduleRefresh(nextEvent)

	newMembersMap, changedEvent := r.compareMembers(hosts)
	if changedEvent == nil {
		// Membership didn't change, but presence might have (e.g. a join window ended).
		prev := r.loadRingAndHosts()
		r.ringAndHosts.Store(ringAndHosts{
			ring:         prev.ring,
			hosts:        prev.hosts,
			presence:     presence,
			gradualUntil: gradualUntil,
		})
		return nil, nil
	}

	ring := newHashRing(r.replicaPoints)
	ring.AddMembers(util.MapSlice(hosts, func(h *hostInfo) rpmembership.Member { return h })...)

	r.lastRefreshTime = time.Now().UTC()
	r.ringAndHosts.Store(ringAndHosts{
		ring:         ring,
		hosts:        newMembersMap,
		presence:     presence,
		gradualUntil: gradualUntil,
	})

	addrs := util.MapSlice(hosts, func(h *hostInfo) string { return h.summary() })
	slices.Sort(addrs)
	r.logger.Info("Current reachable members", tag.Addresses(addrs))

	return changedEvent, nil
}

func (r *serviceResolver) scheduleRefresh(nextEvent int64) {
	if nextEvent == 0 {
		return
	}
	if _, ok := r.scheduledRefreshMap[nextEvent]; ok {
		return // already have a timer scheduled for this time
	}
	nextEventTime := time.Unix(nextEvent, 0)
	r.scheduledRefreshMap[nextEvent] = time.AfterFunc(time.Until(nextEventTime), func() {
		// force refresh asap
		if err := r.refresh(refreshModeAlways); err != nil {
			r.logger.Error("error refreshing ring on scheduled event", tag.Error(err))
		}
		// clean up map
		r.refreshLock.Lock()
		defer r.refreshLock.Unlock()
		delete(r.scheduledRefreshMap, nextEvent)
	})
	r.logger.Debug("Membership will refresh at scheduled event", tag.Timestamp(nextEventTime))
}

func (r *serviceResolver) getReachableMembers() ([]*hostInfo, error) {
	members, err := r.rp.GetReachableMemberObjects(swim.MemberWithLabelAndValue(roleKey, string(r.service)))
	if err != nil {
		return nil, err
	}

	// Turn swim.Members into hostInfo
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

// filterMembersByTime filters hosts by their startAt/stopAt labels (and windows), and returns
// the hosts that should be in the ring, presence info for hosts that are joining or leaving,
// the end of the latest gradual change window, and the next time (in unix seconds) at which
// we should refresh again (or zero). We only need to keep track of one event since we'll
// refresh at that time and find the next one.
func filterMembersByTime(
	hosts []*hostInfo,
	now time.Time,
) ([]*hostInfo, map[string]*presence, time.Time, int64) {
	var presences map[string]*presence
	var gradualUntil time.Time
	nextEvent := int64(math.MaxInt64)
	addEvent := func(t time.Time) {
		if t.After(now) {
			nextEvent = min(nextEvent, t.Unix())
		}
	}

	hosts = slices.DeleteFunc(slices.Clone(hosts), func(host *hostInfo) bool {
		var p presence
		if start, end, ok := parseWindowLabels(host, startAtKey, startWindowKey); ok {
			if now.Before(start) {
				// start time is in the future: exclude and schedule refresh at that time
				addEvent(start)
				return true
			} else if now.Before(end) {
				// in join window: schedule refresh at the end to clear presence
				p.joinStart, p.joinEnd = start, end
				addEvent(end)
			}
		}
		if start, end, ok := parseWindowLabels(host, stopAtKey, stopWindowKey); ok {
			if !now.Before(end) {
				return true // stop time is in the past
			}
			// stop time or window is in the future: schedule refresh at the end to remove it.
			// Before that, presence will filter it out of lookups as appropriate.
			p.leaveStart, p.leaveEnd = start, end
			addEvent(end)
		}
		if p != (presence{}) {
			if presences == nil {
				presences = make(map[string]*presence)
			}
			presences[host.GetAddress()] = &p
			if end := p.gradualEnd(); end.After(gradualUntil) {
				gradualUntil = end
			}
		}
		return false
	})

	if nextEvent == math.MaxInt64 {
		nextEvent = 0
	}
	return hosts, presences, gradualUntil, nextEvent
}

// parseWindowLabels returns the start and end of a window given by a start time label and an
// optional window duration label.
func parseWindowLabels(host *hostInfo, atKey, windowKey string) (start, end time.Time, ok bool) {
	at, err := parseIntLabel(host, atKey)
	if err != nil {
		return time.Time{}, time.Time{}, false // ignore label if missing or can't parse
	}
	start = time.Unix(at, 0)
	window, err := parseIntLabel(host, windowKey)
	if err != nil || window <= 0 {
		return start, start, true
	}
	window = min(window, int64(maxGradualChangeWindow/time.Second))
	return start, start.Add(time.Duration(window) * time.Second), true
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
			if err := r.refresh(refreshModeLazy); err != nil {
				r.logger.Error("error refreshing ring by request", tag.Error(err))
			}
		case <-refreshTicker.C:
			if err := r.refresh(refreshModeLazy); err != nil {
				r.logger.Error("error periodically refreshing ring", tag.Error(err))
			}
		}
	}
}

func (r *serviceResolver) ring() (*hashring.HashRing, map[string]*hostInfo) {
	ring := r.loadRingAndHosts()
	return ring.ring, ring.hosts
}

func (r *serviceResolver) loadRingAndHosts() ringAndHosts {
	return r.ringAndHosts.Load().(ringAndHosts) //nolint:revive // unchecked-type-assertion: we only ever store ringAndHosts
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

// parseIntLabel returns the value of the given label as an integer.
func parseIntLabel(host *hostInfo, label string) (int64, error) {
	str, ok := host.Label(label)
	if !ok {
		return 0, errMissingLabel
	}
	return strconv.ParseInt(str, 10, 64)
}

func isDraining(host *hostInfo) bool {
	if drainingStr, ok := host.Label(drainingKey); ok {
		if draining, err := strconv.ParseBool(drainingStr); err == nil {
			return draining
		}
	}
	return false
}
