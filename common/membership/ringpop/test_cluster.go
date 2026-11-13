package ringpop

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/ringpop-go"
	"github.com/temporalio/tchannel-go"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/mock/gomock"
)

// testCluster is a type that represents a test ringpop cluster
type testCluster struct {
	hostUUIDs    []string
	hostAddrs    []string
	hostInfoList []membership.HostInfo
	rings        []*monitor
	channels     []*tchannel.Channel
	seedNode     string
}

// newTestCluster creates a new test cluster with the given name and cluster size
// All the nodes in the test cluster will register themselves in Ringpop
// with the specified name. This is only intended for unit tests.
func newTestCluster(
	t *testing.T,
	ringPopApp string,
	size int,
	listenIPAddr string,
	seed string,
	serviceName primitives.ServiceName,
	broadcastAddress string,
	joinTimes []time.Time,
	evictBeforeStart []bool,
	testInitialBootstrapFailure bool,
) *testCluster {
	logger := log.NewTestLogger()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockMgr := persistence.NewMockClusterMetadataManager(ctrl)
	mockMgr.EXPECT().PruneClusterMembership(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockMgr.EXPECT().UpsertClusterMembership(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	cluster := &testCluster{
		hostUUIDs:    make([]string, size),
		hostAddrs:    make([]string, size),
		hostInfoList: make([]membership.HostInfo, size),
		rings:        make([]*monitor, size),
		channels:     make([]*tchannel.Channel, size),
		seedNode:     seed,
	}

	for i := range size {
		var err error
		cluster.channels[i], err = tchannel.NewChannel(ringPopApp, nil)
		if err != nil {
			logger.Error("Failed to create tchannel", tag.Error(err))
			return nil
		}
		listenAddr := listenIPAddr + ":0"
		err = cluster.channels[i].ListenAndServe(listenAddr)
		if err != nil {
			logger.Error("tchannel listen failed", tag.Error(err))
			return nil
		}
		cluster.hostUUIDs[i] = uuid.NewString()
		cluster.hostAddrs[i], err = buildBroadcastHostPort(cluster.channels[i].PeerInfo(), broadcastAddress)
		if err != nil {
			logger.Error("Failed to build broadcast hostport", tag.Error(err))
			return nil
		}
		cluster.hostInfoList[i] = newHostInfo(cluster.hostAddrs[i], nil)
	}

	// if seed node is already supplied, use it; if not, set it
	if cluster.seedNode == "" {
		cluster.seedNode = cluster.hostAddrs[0]
	}
	logger.Info("seedNode", tag.Name(cluster.seedNode))

	seedAddress, seedPort, err := splitHostPortTyped(cluster.seedNode)
	if err != nil {
		logger.Error("unable to split host port", tag.Error(err))
		return nil
	}
	// MarshalBinary never fails for UUIDs
	hostID, _ := uuid.New().MarshalBinary()

	seedMember := &persistence.ClusterMember{
		HostID:        hostID,
		RPCAddress:    seedAddress,
		RPCPort:       seedPort,
		SessionStart:  time.Now().UTC(),
		LastHeartbeat: time.Now().UTC(),
	}

	firstGetClusterMemberCall := testInitialBootstrapFailure
	mockMgr.EXPECT().GetClusterMembers(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *persistence.GetClusterMembersRequest) (*persistence.GetClusterMembersResponse, error) {
			res := &persistence.GetClusterMembersResponse{ActiveMembers: []*persistence.ClusterMember{seedMember}}

			hostID, _ := uuid.New().MarshalBinary()
			if firstGetClusterMemberCall {
				// The first time GetClusterMembers is invoked, we simulate returning a stale/bad heartbeat.
				// All subsequent calls only return the single "good" seed member
				// This ensures that we exercise the retry path in bootstrap properly.
				badSeedMember := &persistence.ClusterMember{
					HostID:        hostID,
					RPCAddress:    seedAddress,
					RPCPort:       seedPort + 1,
					SessionStart:  time.Now().UTC(),
					LastHeartbeat: time.Now().UTC(),
				}
				res = &persistence.GetClusterMembersResponse{ActiveMembers: []*persistence.ClusterMember{seedMember, badSeedMember}}
			}

			firstGetClusterMemberCall = false
			return res, nil
		}).AnyTimes()

	for i := range size {
		node := i
		resolver := func() (string, error) {
			return buildBroadcastHostPort(cluster.channels[node].PeerInfo(), broadcastAddress)
		}

		ringPop, err := ringpop.New(ringPopApp, ringpop.Channel(cluster.channels[i]), ringpop.AddressResolverFunc(resolver))
		if err != nil {
			logger.Error("failed to create ringpop instance", tag.Error(err))
			return nil
		}
		_, port, _ := splitHostPortTyped(cluster.hostAddrs[i])
		var joinTime time.Time
		if i < len(joinTimes) {
			joinTime = joinTimes[i]
		}
		cluster.rings[i] = newMonitor(
			serviceName,
			config.ServicePortMap{serviceName: int(port)}, // use same port for "grpc" port
			ringPop,
			logger,
			mockMgr,
			resolver,
			2*time.Second,
			defaultSnapshotExpiry,
			3*time.Second,
			func() time.Time { return joinTime },
			0,
			100,
		)
		if i < len(evictBeforeStart) && evictBeforeStart[i] {
			// Tests assert on the outcome (membership), not the error.
			_ = cluster.rings[i].EvictSelf()
		}
		cluster.rings[i].Start()
	}
	return cluster
}

// GetSeedNode returns the seedNode for this cluster
func (c *testCluster) GetSeedNode() string {
	return c.seedNode
}

// DrainHost marks the given host draining
func (c *testCluster) DrainHost(hostID string) {
	for i, uuid := range c.hostUUIDs {
		if uuid == hostID {
			c.rings[i].SetDraining(true)
		}
	}
}

// KillHost kills the given host within the cluster
func (c *testCluster) KillHost(hostID string) {
	for i := 0; i < len(c.hostUUIDs); i++ {
		if c.hostUUIDs[i] == hostID {
			c.rings[i].Stop()
			c.channels[i].Close()
			c.rings[i] = nil
			c.channels[i] = nil
		}
	}
}

// Stop stops the cluster
func (c *testCluster) Stop() {
	for i := 0; i < len(c.hostAddrs); i++ {
		if c.rings[i] != nil {
			c.rings[i].Stop()
			c.channels[i].Close()
		}
	}
}

// GetHostInfoList returns the list of all hosts within the cluster
func (c *testCluster) GetHostInfoList() []membership.HostInfo {
	return c.hostInfoList
}

// GetHostAddrs returns all host addrs within the cluster
func (c *testCluster) GetHostAddrs() []string {
	return c.hostAddrs
}

// FindHostByAddr returns the host info corresponding to
// the given addr, if it exists
func (c *testCluster) FindHostByAddr(addr string) (membership.HostInfo, bool) {
	for _, hi := range c.hostInfoList {
		if hi.GetAddress() == addr {
			return hi, true
		}
	}
	return nil, false
}

// snapshotRefreshStore is a shared persistence fake for testing discovery across isolated nodes
type snapshotRefreshStore struct {
	mu sync.Mutex

	// members stores membership records keyed by the node monitors' hostports.
	members map[string]*persistence.ClusterMember
	// blocked records the hostports whose persistence calls should fail.
	blocked map[string]bool
}

// manager creates a mock persistence manager for a node identified by source
func (s *snapshotRefreshStore) manager(ctrl *gomock.Controller, source string) *persistence.MockClusterMetadataManager {
	manager := persistence.NewMockClusterMetadataManager(ctrl)

	// Prune expired membership records unless this node is blocked from persistence
	manager.EXPECT().PruneClusterMembership(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ *persistence.PruneClusterMembershipRequest) error {
			if s.isBlocked(source) {
				return errors.New("persistence unavailable")
			}

			now := time.Now().UTC()
			s.mu.Lock()
			defer s.mu.Unlock()
			for hostport, member := range s.members {
				if member.RecordExpiry.Before(now) {
					delete(s.members, hostport)
				}
			}
			return nil
		}).AnyTimes()

	// Upsert cluster membership and heartbeat unless this node is blocked from persistence
	manager.EXPECT().UpsertClusterMembership(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.UpsertClusterMembershipRequest) error {
			if s.isBlocked(source) {
				return errors.New("persistence unavailable")
			}

			member := &persistence.ClusterMember{
				Role:          request.Role,
				HostID:        append([]byte(nil), request.HostID...),
				RPCAddress:    append(net.IP(nil), request.RPCAddress...),
				RPCPort:       request.RPCPort,
				SessionStart:  request.SessionStart,
				LastHeartbeat: time.Now(),
				RecordExpiry:  time.Now().Add(request.RecordExpiry),
			}
			s.mu.Lock()
			s.members[source] = member
			s.mu.Unlock()
			return nil
		}).AnyTimes()

	// Get cluster members from the database unless this node is blocked
	manager.EXPECT().GetClusterMembers(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *persistence.GetClusterMembersRequest) (*persistence.GetClusterMembersResponse, error) {
			if s.isBlocked(source) {
				return nil, errors.New("persistence unavailable")
			}

			s.mu.Lock()
			defer s.mu.Unlock()
			response := &persistence.GetClusterMembersResponse{}
			for _, member := range s.members {
				if time.Since(member.LastHeartbeat) <= request.LastHeartbeatWithin {
					response.ActiveMembers = append(response.ActiveMembers, cloneClusterMember(member))
				}
			}
			return response, nil
		}).AnyTimes()
	return manager
}

// isBlocked reports whether persistence calls from source should fail
func (s *snapshotRefreshStore) isBlocked(source string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.blocked[source]
}

// block prevents persistence calls from the specified hostports
func (s *snapshotRefreshStore) block(hostports []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, hostport := range hostports {
		s.blocked[hostport] = true
	}
}

// unblock allows persistence calls from the specified hostports again
func (s *snapshotRefreshStore) unblock(hostports []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, hostport := range hostports {
		s.blocked[hostport] = false
	}
}

// membersAreStale reports whether each listed member's last heartbeat is older than maxAge
func (s *snapshotRefreshStore) membersAreStale(hostports []string, maxAge time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, hostport := range hostports {
		member, ok := s.members[hostport]
		if !ok || member == nil || time.Since(member.LastHeartbeat) <= maxAge {
			return false
		}
	}
	return true
}

// cloneClusterMember returns a copy of member, including copies of its mutable fields
func cloneClusterMember(member *persistence.ClusterMember) *persistence.ClusterMember {
	if member == nil {
		return nil
	}

	clone := *member
	clone.HostID = append([]byte(nil), member.HostID...)
	clone.RPCAddress = append(net.IP(nil), member.RPCAddress...)
	return &clone
}

// snapshotRefreshTestCluster groups monitors that share a snapshotRefreshStore
type snapshotRefreshTestCluster struct {
	// monitors[i] is the monitor running at addrs[i].
	monitors []*monitor
	channels []*tchannel.Channel
	addrs    []string
}

// addMonitor creates and starts a monitor backed by the shared snapshot refresh store
func (c *snapshotRefreshTestCluster) addMonitor(t *testing.T, store *snapshotRefreshStore, app string, snapshotExpiry time.Duration) {
	t.Helper()

	channel, err := tchannel.NewChannel(app, nil)
	require.NoError(t, err)
	require.NoError(t, channel.ListenAndServe("127.0.0.1:0"))

	resolver := func() (string, error) {
		return buildBroadcastHostPort(channel.PeerInfo(), "127.0.0.1")
	}
	addr, err := resolver()
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	manager := store.manager(ctrl, addr)

	ringPop, err := ringpop.New(app, ringpop.Channel(channel), ringpop.AddressResolverFunc(resolver))
	require.NoError(t, err)

	_, port, err := splitHostPortTyped(addr)
	require.NoError(t, err)

	monitor := newMonitor(
		primitives.HistoryService,
		config.ServicePortMap{primitives.HistoryService: int(port)},
		ringPop,
		log.NewTestLogger(),
		manager,
		resolver,
		2*time.Second,
		snapshotExpiry,
		3*time.Second,
		time.Time{},
		100,
	)
	c.monitors = append(c.monitors, monitor)
	c.channels = append(c.channels, channel)
	c.addrs = append(c.addrs, addr)

	monitor.Start()
	t.Cleanup(func() {
		monitor.Stop()
		channel.Close()
	})
}

// hasPartitionedViews reports whether the monitors' visibility graph has multiple components
// (non-overlapping subsets) to detect partitioned membership views
func (c *snapshotRefreshTestCluster) hasPartitionedViews() bool {
	visibility := make(map[string]map[string]struct{}, len(c.addrs))

	for _, addr := range c.addrs {
		visibility[addr] = make(map[string]struct{})
	}
	for i, monitor := range c.monitors {
		members, err := monitor.GetReachableMembers()
		if err != nil {
			return false
		}
		source := c.addrs[i]
		for _, member := range members {
			if _, ok := visibility[member]; !ok {
				visibility[member] = make(map[string]struct{})
			}
			visibility[source][member] = struct{}{}
			visibility[member][source] = struct{}{}
		}
	}

	visited := make(map[string]struct{}, len(visibility))
	components := 0
	for addr := range visibility {
		if _, ok := visited[addr]; ok {
			continue
		}

		components++
		if components > 1 {
			return true
		}

		toVisit := []string{addr}
		for len(toVisit) > 0 {
			current := toVisit[len(toVisit)-1]
			toVisit = toVisit[:len(toVisit)-1]
			if _, ok := visited[current]; ok {
				continue
			}
			visited[current] = struct{}{}
			for neighbor := range visibility[current] {
				toVisit = append(toVisit, neighbor)
			}
		}
	}
	return false
}

// allHaveMembers reports whether every monitor currently sees count reachable members
func (c *snapshotRefreshTestCluster) allHaveMembers(count int) bool {
	for _, monitor := range c.monitors {
		members, err := monitor.GetReachableMembers()
		if err != nil || len(members) != count {
			return false
		}
	}
	return true
}

// gatedTestMonitor is a monitor that isn't started yet, and whose bootstrap blocks fetching
// bootstrap hosts until release is called (or the monitor is stopped). This lets tests act on
// the monitor while Start is in the middle of bootstrapping.
type gatedTestMonitor struct {
	*monitor
	addr    string
	channel *tchannel.Channel
	// entered is closed once Start has reached bootstrap.
	entered     chan struct{}
	gate        chan struct{}
	releaseOnce sync.Once
}

func newGatedTestMonitor(
	t *testing.T,
	ringPopApp string,
	seed string,
	serviceName primitives.ServiceName,
	logger log.Logger,
) *gatedTestMonitor {
	t.Helper()
	ctrl := gomock.NewController(t)

	ch, err := tchannel.NewChannel(ringPopApp, nil)
	if err != nil {
		t.Fatalf("failed to create tchannel: %v", err)
	}
	if err = ch.ListenAndServe("127.0.0.1:0"); err != nil {
		t.Fatalf("tchannel listen failed: %v", err)
	}
	resolver := func() (string, error) {
		return buildBroadcastHostPort(ch.PeerInfo(), "127.0.0.1")
	}
	addr, err := resolver()
	if err != nil {
		t.Fatalf("failed to build broadcast hostport: %v", err)
	}
	seedAddress, seedPort, err := splitHostPortTyped(seed)
	if err != nil {
		t.Fatalf("unable to split seed host port: %v", err)
	}
	seedHostID, _ := uuid.New().MarshalBinary()
	seedMember := &persistence.ClusterMember{
		HostID:        seedHostID,
		RPCAddress:    seedAddress,
		RPCPort:       seedPort,
		SessionStart:  time.Now().UTC(),
		LastHeartbeat: time.Now().UTC(),
	}

	m := &gatedTestMonitor{
		addr:    addr,
		channel: ch,
		entered: make(chan struct{}),
		gate:    make(chan struct{}),
	}
	var enteredOnce sync.Once
	mockMgr := persistence.NewMockClusterMetadataManager(ctrl)
	mockMgr.EXPECT().PruneClusterMembership(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockMgr.EXPECT().UpsertClusterMembership(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	mockMgr.EXPECT().GetClusterMembers(gomock.Any(), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ *persistence.GetClusterMembersRequest) (*persistence.GetClusterMembersResponse, error) {
			enteredOnce.Do(func() { close(m.entered) })
			select {
			case <-m.gate:
				return &persistence.GetClusterMembersResponse{ActiveMembers: []*persistence.ClusterMember{seedMember}}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}).AnyTimes()

	rp, err := ringpop.New(ringPopApp, ringpop.Channel(ch), ringpop.AddressResolverFunc(resolver))
	if err != nil {
		t.Fatalf("failed to create ringpop instance: %v", err)
	}
	_, port, _ := splitHostPortTyped(addr)
	m.monitor = newMonitor(
		serviceName,
		config.ServicePortMap{serviceName: int(port)},
		rp,
		logger,
		mockMgr,
		resolver,
		2*time.Second,
		defaultSnapshotExpiry,
		3*time.Second,
		func() time.Time { return time.Time{} },
		0,
		100,
	)
	t.Cleanup(func() {
		m.release()
		m.Stop()
		ch.Close()
	})
	return m
}

// release unblocks bootstrap.
func (m *gatedTestMonitor) release() {
	m.releaseOnce.Do(func() { close(m.gate) })
}

// fatalRecordingLogger records Fatal calls instead of exiting the process. Like a real Fatal, it
// doesn't return: it ends the calling goroutine, so it must only be called off the test goroutine.
type fatalRecordingLogger struct {
	log.Logger
	fatals atomic.Int32
}

func (l *fatalRecordingLogger) Fatal(msg string, tags ...tag.Tag) {
	l.fatals.Add(1)
	l.Error(msg, tags...)
	runtime.Goexit()
}
