package rpc

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

// The cache is keyed by host address, and grpc.NewClient is lazy: no dial
// happens until the first RPC, which these tests never make. So the addresses
// below are only map keys and never have to be reachable.
const (
	hostA = "cache-test-host-a:1234"
	hostB = "cache-test-host-b:1234"
)

func newCacheTestFactory() *RPCFactory {
	return NewFactory(nil, "tester", log.NewNoopLogger(), metrics.NoopMetricsHandler, nil, "", "", 0, nil, nil, nil, nil, nil, nil)
}

func cachedConn(f *RPCFactory, hostName string) (*grpc.ClientConn, bool) {
	f.connectionsMu.RLock()
	defer f.connectionsMu.RUnlock()
	c, ok := f.internodeGRPCConnections[hostName]
	return c, ok
}

func cachedConnCount(f *RPCFactory) int {
	f.connectionsMu.RLock()
	defer f.connectionsMu.RUnlock()
	return len(f.internodeGRPCConnections)
}

// dial returns nil after logging Fatal, which a no-op logger does not act on.
// Caching that nil would make every later lookup dereference it.
func TestInternodeConnCache_DoesNotCacheFailedDial(t *testing.T) {
	f := newCacheTestFactory()

	// A control character makes gRPC's target parsing fail, so dial returns nil.
	const badHost = "cache-test-host-\x7f"
	require.Nil(t, f.CreateHistoryGRPCConnection(badHost))

	_, cached := cachedConn(f, badHost)
	require.False(t, cached, "a failed dial must not be cached")
	require.Nil(t, f.CreateHistoryGRPCConnection(badHost))
}

func TestInternodeConnCache_SharesConnPerHost(t *testing.T) {
	f := newCacheTestFactory()

	c1 := f.CreateHistoryGRPCConnection(hostA)
	require.NotNil(t, c1)
	require.Same(t, c1, f.CreateHistoryGRPCConnection(hostA))

	// Different host address gets its own connection.
	require.NotSame(t, c1, f.CreateHistoryGRPCConnection(hostB))
}

func TestInternodeConnCache_ReplacesShutdownConn(t *testing.T) {
	f := newCacheTestFactory()

	c1 := f.CreateHistoryGRPCConnection(hostA)
	require.NoError(t, c1.Close()) // simulate the downstream cache closing it on membership change
	require.Equal(t, connectivity.Shutdown, c1.GetState())

	c2 := f.CreateHistoryGRPCConnection(hostA)
	require.NotSame(t, c1, c2)
	require.NotEqual(t, connectivity.Shutdown, c2.GetState())
}

func TestInternodeConnCache_CleanupRemovesShutdownConns(t *testing.T) {
	f := newCacheTestFactory()

	live := f.CreateHistoryGRPCConnection(hostA)
	dead := f.CreateHistoryGRPCConnection(hostB)
	require.NoError(t, dead.Close())

	f.cleanupInternodeConns()

	_, deadOK := cachedConn(f, hostB)
	require.False(t, deadOK, "shut-down connection should be swept")
	liveConn, liveOK := cachedConn(f, hostA)
	require.True(t, liveOK, "live connection should be retained")
	require.Same(t, live, liveConn)
}

func TestInternodeConnCache_EvictsOnMembershipRemoval(t *testing.T) {
	f := newCacheTestFactory()
	defer f.Close()
	conn := f.CreateHistoryGRPCConnection(hostA)
	require.NotNil(t, conn)

	f.evictInternodeConn(hostA)

	_, cached := cachedConn(f, hostA)
	require.False(t, cached, "evicted host should be removed from cache")

	// Next call re-dials.
	conn2 := f.CreateHistoryGRPCConnection(hostA)
	require.NotNil(t, conn2)
	require.NotSame(t, conn, conn2)
}

func TestInternodeConnCache_WatchMembershipEvicts(t *testing.T) {
	ctrl := gomock.NewController(t)

	resolver := membership.NewMockServiceResolver(ctrl)
	monitor := membership.NewMockMonitor(ctrl)

	// History resolver succeeds; matching returns unknown (not all services run all resolvers).
	monitor.EXPECT().GetResolver(primitives.HistoryService).Return(resolver, nil)
	monitor.EXPECT().GetResolver(primitives.MatchingService).Return(nil, membership.ErrUnknownService)

	// Capture the listener channel so we can inject membership events.
	listenerCh := make(chan chan<- *membership.ChangedEvent, 1)
	resolver.EXPECT().AddListener(gomock.Any(), gomock.Any()).Do(
		func(_ string, ch chan<- *membership.ChangedEvent) {
			listenerCh <- ch
		}).Return(nil)
	resolver.EXPECT().RemoveListener(gomock.Any()).Return(nil).AnyTimes()
	resolver.EXPECT().Members().Return(nil).AnyTimes()

	f := NewFactory(nil, "tester", log.NewNoopLogger(), metrics.NoopMetricsHandler,
		nil, "", "", 0, nil, nil, nil, nil, monitor, nil)
	f.membershipEvictionDelay = 10 * time.Millisecond
	f.membershipEvictionTickInterval = 20 * time.Millisecond

	conn := f.CreateHistoryGRPCConnection(hostA)
	require.NotNil(t, conn)

	f.StartMembershipEviction()
	defer f.Close()

	// Wait for the watcher to register its listener.
	var ch chan<- *membership.ChangedEvent
	select {
	case ch = <-listenerCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for membership listener registration")
	}

	// Send removal event.
	ch <- &membership.ChangedEvent{
		HostsRemoved: []membership.HostInfo{membership.NewHostInfoFromAddress(hostA)},
	}

	// Eviction uses a delay + ticker; wait for it.
	require.Eventually(t, func() bool {
		_, ok := cachedConn(f, hostA)
		return !ok
	}, 2*time.Second, 10*time.Millisecond, "connection should be evicted after membership removal")
}

func TestInternodeConnCache_WatchMembershipCancelsEvictionOnReAdd(t *testing.T) {
	ctrl := gomock.NewController(t)

	resolver := membership.NewMockServiceResolver(ctrl)
	monitor := membership.NewMockMonitor(ctrl)

	monitor.EXPECT().GetResolver(primitives.HistoryService).Return(resolver, nil)
	monitor.EXPECT().GetResolver(primitives.MatchingService).Return(nil, membership.ErrUnknownService)

	listenerCh := make(chan chan<- *membership.ChangedEvent, 1)
	resolver.EXPECT().AddListener(gomock.Any(), gomock.Any()).Do(
		func(_ string, ch chan<- *membership.ChangedEvent) {
			listenerCh <- ch
		}).Return(nil)
	resolver.EXPECT().RemoveListener(gomock.Any()).Return(nil).AnyTimes()
	// Host is still a member — re-add cancels eviction.
	resolver.EXPECT().Members().Return([]membership.HostInfo{
		membership.NewHostInfoFromAddress(hostA),
	}).AnyTimes()

	f := NewFactory(nil, "tester", log.NewNoopLogger(), metrics.NoopMetricsHandler,
		nil, "", "", 0, nil, nil, nil, nil, monitor, nil)
	f.membershipEvictionDelay = 500 * time.Millisecond
	f.membershipEvictionTickInterval = 20 * time.Millisecond

	conn := f.CreateHistoryGRPCConnection(hostA)
	require.NotNil(t, conn)

	f.StartMembershipEviction()
	defer f.Close()

	var ch chan<- *membership.ChangedEvent
	select {
	case ch = <-listenerCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for membership listener registration")
	}

	// Remove then immediately re-add.
	ch <- &membership.ChangedEvent{
		HostsRemoved: []membership.HostInfo{membership.NewHostInfoFromAddress(hostA)},
	}
	ch <- &membership.ChangedEvent{
		HostsAdded: []membership.HostInfo{membership.NewHostInfoFromAddress(hostA)},
	}

	// Wait longer than the eviction delay — connection should NOT be evicted because re-added.
	time.Sleep(700 * time.Millisecond)
	_, cached := cachedConn(f, hostA)
	require.True(t, cached, "re-added host should not be evicted")
}

func TestInternodeConnCache_ConcurrentCreate(t *testing.T) {
	f := newCacheTestFactory()

	const n = 32
	got := make([]*grpc.ClientConn, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			got[i] = f.CreateHistoryGRPCConnection(hostA)
		}()
	}
	wg.Wait()

	require.Equal(t, 1, cachedConnCount(f))
	cached, ok := cachedConn(f, hostA)
	require.True(t, ok)
	for i, conn := range got {
		require.Same(t, cached, conn, "caller %d got a connection the cache discarded", i)
	}
}
