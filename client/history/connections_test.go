package history

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

const testConnAddr rpcAddress = "conn-pool-test-host:1234"

// sharedConnFactory mimics the RPCFactory internode cache: every caller asking
// for the same address gets the same *grpc.ClientConn back.
type sharedConnFactory struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn

	// createDelay widens the window in which concurrent callers are all still
	// creating, so overlapping creates are exercised reliably.
	createDelay time.Duration
}

func newSharedConnFactory() *sharedConnFactory {
	return &sharedConnFactory{conns: make(map[string]*grpc.ClientConn)}
}

func (f *sharedConnFactory) CreateHistoryGRPCConnection(rpcAddress string) *grpc.ClientConn {
	if f.createDelay > 0 {
		time.Sleep(f.createDelay)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if conn, ok := f.conns[rpcAddress]; ok {
		return conn
	}
	// grpc.NewClient is lazy, so this address never has to be reachable.
	conn, err := grpc.NewClient(rpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	f.conns[rpcAddress] = conn
	return conn
}

func newTestConnectionPool(t *testing.T, factory RPCFactory) *connectionPoolImpl[grpc.ClientConnInterface] {
	ctrl := gomock.NewController(t)
	resolver := membership.NewMockServiceResolver(ctrl)
	resolver.EXPECT().AddListener(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	resolver.EXPECT().RemoveListener(gomock.Any()).Return(nil).AnyTimes()
	resolver.EXPECT().Members().Return(nil).AnyTimes()

	return NewConnectionPool(
		resolver,
		factory,
		func(cc grpc.ClientConnInterface) grpc.ClientConnInterface { return cc },
		log.NewNoopLogger(),
		func() time.Duration { return time.Minute },
	)
}

// The factory shares one connection per host, so callers that lose the cache
// race hold the very connection the winner stored. Closing it would leave the
// pool serving a shut-down connection for the rest of the process, because a
// cached entry is never revalidated.
func TestConnectionPool_ConcurrentCreateKeepsSharedConnUsable(t *testing.T) {
	factory := newSharedConnFactory()
	factory.createDelay = 50 * time.Millisecond
	pool := newTestConnectionPool(t, factory)
	t.Cleanup(pool.Close)

	const callers = 8
	got := make([]*grpc.ClientConn, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := range callers {
		go func() {
			defer wg.Done()
			got[i] = pool.getOrCreateClientConn(testConnAddr).grpcConn
		}()
	}
	wg.Wait()

	v, ok := pool.conns.Load(testConnAddr)
	require.True(t, ok, "expected a pooled connection")
	pooled := v.(clientConnection[grpc.ClientConnInterface]).grpcConn

	require.NotEqual(t, connectivity.Shutdown, pooled.GetState(),
		"pool closed the shared connection it had just cached")
	for i, conn := range got {
		require.Same(t, pooled, conn, "caller %d got a different connection", i)
	}
}
