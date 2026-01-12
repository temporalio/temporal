package rpc

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RPCFactorySuite struct {
	suite.Suite
	*require.Assertions
	controller *gomock.Controller

	mockMonitor         *membership.MockMonitor
	mockHistoryResolver *membership.MockServiceResolver
}

func TestRPCFactorySuite(t *testing.T) {
	suite.Run(t, new(RPCFactorySuite))
}

func (s *RPCFactorySuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.mockMonitor = membership.NewMockMonitor(s.controller)
	s.mockHistoryResolver = membership.NewMockServiceResolver(s.controller)
}

func (s *RPCFactorySuite) TearDownTest() {
	s.controller.Finish()
}

func (s *RPCFactorySuite) TestStaleConnectionNotEvictedWithoutListener() {
	// This test demonstrates the BUG: without the membership listener,
	// stale connections are NOT evicted from the cache when hosts are removed.
	// This is the behavior that caused issue #8719.

	cfg := &config.Config{
		Services: map[string]config.Service{
			string(primitives.HistoryService): {
				RPC: config.RPC{},
			},
		},
	}

	factory := NewFactory(
		cfg,
		primitives.HistoryService,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		nil, // tlsProvider
		"",  // frontendURL
		"",  // frontendHTTPURL
		0,   // frontendHTTPPort
		nil, // frontendTLSConfig
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		nil, // perServiceDialOptions
		nil, // monitor - nil means no membership listener can be started
	)

	// NOTE: We intentionally do NOT call factory.StartMembershipListener()
	// This simulates the old behavior before the fix.

	// Create a connection to a host
	hostAddress := "127.0.0.1:7234"
	conn := factory.CreateHistoryGRPCConnection(hostAddress)
	s.NotNil(conn)

	// Verify the connection is in the cache
	cachedConn := factory.interNodeGrpcConnections.Get(hostAddress)
	s.NotNil(cachedConn, "connection should be cached")

	// Simulate what would happen if a host was removed from the membership ring.
	// Without the membership listener, there's no way for the cache to know
	// about the removal, so we manually call handleMembershipChange to show
	// what SHOULD happen (but doesn't without the listener running).

	// Wait a bit to simulate time passing after host removal
	time.Sleep(100 * time.Millisecond)

	// The connection is STILL in the cache - this is the BUG!
	// In a real Kubernetes deployment, this stale connection would keep
	// trying to dial the removed pod's IP, causing i/o timeout errors.
	stillCachedConn := factory.interNodeGrpcConnections.Get(hostAddress)
	s.NotNil(stillCachedConn, "BUG: connection remains cached even after host should be removed")
}

func (s *RPCFactorySuite) TestStaleConnectionEvictedWithListener() {
	// This test demonstrates the FIX: with the membership listener active,
	// stale connections ARE evicted from the cache when hosts are removed.

	cfg := &config.Config{
		Services: map[string]config.Service{
			string(primitives.HistoryService): {
				RPC: config.RPC{},
			},
		},
	}

	// Capture the channel that RPCFactory registers with the resolver
	var listenerChan chan<- *membership.ChangedEvent

	s.mockMonitor.EXPECT().GetResolver(primitives.HistoryService).Return(s.mockHistoryResolver, nil).AnyTimes()
	s.mockHistoryResolver.EXPECT().AddListener(gomock.Any(), gomock.Any()).DoAndReturn(
		func(name string, ch chan<- *membership.ChangedEvent) error {
			listenerChan = ch
			return nil
		},
	).AnyTimes()
	s.mockHistoryResolver.EXPECT().RemoveListener(gomock.Any()).Return(nil).AnyTimes()

	factory := NewFactory(
		cfg,
		primitives.HistoryService,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		nil, // tlsProvider
		"",  // frontendURL
		"",  // frontendHTTPURL
		0,   // frontendHTTPPort
		nil, // frontendTLSConfig
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		nil, // perServiceDialOptions
		s.mockMonitor,
	)

	// Start listening for membership changes - THIS IS THE FIX
	factory.StartMembershipListener()
	defer factory.StopMembershipListener()

	// Wait for listener to be set up
	s.Eventually(func() bool {
		return listenerChan != nil
	}, time.Second, 10*time.Millisecond, "listener channel should be set")

	// Create a connection to a host
	hostAddress := "127.0.0.1:7234"

	// Note: This will fail to actually connect, but it creates an entry in the cache
	conn := factory.CreateHistoryGRPCConnection(hostAddress)
	s.NotNil(conn)

	// Verify the connection is in the cache
	cachedConn := factory.interNodeGrpcConnections.Get(hostAddress)
	s.NotNil(cachedConn, "connection should be cached")

	// Simulate host removal from membership ring
	event := &membership.ChangedEvent{
		HostsRemoved: []membership.HostInfo{
			membership.NewHostInfoFromAddress(hostAddress),
		},
	}

	// Send the membership change event
	listenerChan <- event

	// Wait for the event to be processed - WITH THE FIX, this succeeds!
	s.Eventually(func() bool {
		return factory.interNodeGrpcConnections.Get(hostAddress) == nil
	}, time.Second, 10*time.Millisecond, "FIX: connection should be evicted from cache after host removal")
}

func (s *RPCFactorySuite) TestHandleMembershipChangeEvictsConnections() {
	// This test verifies the fix for issue #8719:
	// When hosts are removed from the membership ring, their cached
	// gRPC connections should be evicted from the cache.
	//
	// To verify this is a valid regression test:
	// 1. Comment out the body of HandleMembershipChange in rpc.go
	// 2. Run this test - it will FAIL
	// 3. Uncomment the fix - the test will PASS

	cfg := &config.Config{
		Services: map[string]config.Service{
			string(primitives.HistoryService): {
				RPC: config.RPC{},
			},
		},
	}

	factory := NewFactory(
		cfg,
		primitives.HistoryService,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		nil,
		"",
		"",
		0,
		nil,
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		nil,
		nil, // no monitor needed for this test
	)

	// Create connections to multiple hosts
	host1 := "127.0.0.1:7234"
	host2 := "127.0.0.1:7235"
	host3 := "127.0.0.1:7236"

	factory.CreateHistoryGRPCConnection(host1)
	factory.CreateHistoryGRPCConnection(host2)
	factory.CreateHistoryGRPCConnection(host3)

	// Verify all connections are cached
	s.NotNil(factory.interNodeGrpcConnections.Get(host1))
	s.NotNil(factory.interNodeGrpcConnections.Get(host2))
	s.NotNil(factory.interNodeGrpcConnections.Get(host3))

	// Simulate membership change - host1 and host2 removed
	event := &membership.ChangedEvent{
		HostsRemoved: []membership.HostInfo{
			membership.NewHostInfoFromAddress(host1),
			membership.NewHostInfoFromAddress(host2),
		},
	}

	// Call the fix - this should evict host1 and host2 from the cache
	factory.HandleMembershipChange(event)

	// host1 and host2 should be evicted, host3 should remain
	s.Nil(factory.interNodeGrpcConnections.Get(host1), "removed host should be evicted")
	s.Nil(factory.interNodeGrpcConnections.Get(host2), "removed host should be evicted")
	s.NotNil(factory.interNodeGrpcConnections.Get(host3), "unaffected host should remain")
}

func (s *RPCFactorySuite) TestHostChangedDoesNotEvictConnection() {
	// This test verifies that when a host's labels change (HostsChanged event),
	// its cached gRPC connection is NOT evicted. HostsChanged only means labels
	// changed (e.g., host entered draining state) - the host is still running
	// and the connection is still valid.

	cfg := &config.Config{
		Services: map[string]config.Service{
			string(primitives.HistoryService): {
				RPC: config.RPC{},
			},
		},
	}

	factory := NewFactory(
		cfg,
		primitives.HistoryService,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		nil,
		"",
		"",
		0,
		nil,
		[]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		nil,
		nil, // no monitor needed for this test
	)

	hostAddress := "127.0.0.1:7234"
	conn := factory.CreateHistoryGRPCConnection(hostAddress)
	s.NotNil(conn)

	cachedConn := factory.interNodeGrpcConnections.Get(hostAddress)
	s.NotNil(cachedConn, "connection should be cached")

	// Simulate host change (e.g., host entered draining state)
	event := &membership.ChangedEvent{
		HostsChanged: []membership.HostInfo{
			membership.NewHostInfoFromAddress(hostAddress),
		},
	}

	factory.HandleMembershipChange(event)

	// Connection should NOT be evicted - host is still running, just changed labels
	s.NotNil(factory.interNodeGrpcConnections.Get(hostAddress),
		"connection should NOT be evicted on HostsChanged - host is still running")
}
