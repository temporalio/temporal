package history

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"google.golang.org/grpc"
)

type (
	clientConnection[C any] struct {
		grpcClient C
		grpcConn   *grpc.ClientConn
	}

	rpcAddress string

	connectionPoolImpl[C any] struct {
		mu struct {
			sync.RWMutex
			conns map[rpcAddress]clientConnection[C]
		}

		historyServiceResolver membership.ServiceResolver
		rpcFactory             RPCFactory
		clientCtor             func(grpc.ClientConnInterface) C
		logger                 log.Logger
	}

	// RPCFactory is a subset of the [go.temporal.io/server/common/rpc.RPCFactory] interface to make testing easier.
	RPCFactory interface {
		CreateHistoryGRPCConnection(rpcAddress string) *grpc.ClientConn
	}

	connectionPool[C any] interface {
		getOrCreateClientConn(addr rpcAddress) clientConnection[C]
		getAllClientConns() []clientConnection[C]
		resetConnectBackoff(clientConnection[C])
	}
)

func NewConnectionPool[C any](
	historyServiceResolver membership.ServiceResolver,
	rpcFactory RPCFactory,
	clientCtor func(grpc.ClientConnInterface) C,
	logger log.Logger,
	connectionCloseDelay dynamicconfig.DurationPropertyFn,
) *connectionPoolImpl[C] {
	c := &connectionPoolImpl[C]{
		historyServiceResolver: historyServiceResolver,
		rpcFactory:             rpcFactory,
		clientCtor:             clientCtor,
		logger:                 logger,
	}
	c.mu.conns = make(map[rpcAddress]clientConnection[C])

	// Close cached conns whose host leaves the membership ring. The listener
	// goroutine lives for the lifetime of the pool (process).
	go c.watchMembershipForClose(connectionCloseDelay)
	return c
}

func (c *connectionPoolImpl[C]) watchMembershipForClose(connectionCloseDelay dynamicconfig.DurationPropertyFn) {
	listenerName := fmt.Sprintf("historyConnectionPool-%s", uuid.New().String())
	ch := make(chan *membership.ChangedEvent, 1)
	if err := c.historyServiceResolver.AddListener(listenerName, ch); err != nil {
		c.logger.Error("Failed to subscribe history connection pool to membership", tag.Error(err))
		return
	}
	for event := range ch {
		for _, h := range event.HostsRemoved {
			go c.closeConnAfterDelay(h.GetAddress(), connectionCloseDelay())
		}
	}
}

func (c *connectionPoolImpl[C]) closeConnAfterDelay(addr string, delay time.Duration) {
	time.Sleep(delay)
	for _, m := range c.historyServiceResolver.Members() {
		if m.GetAddress() == addr {
			return
		}
	}
	c.closeConn(rpcAddress(addr))
}

func (c *connectionPoolImpl[C]) getOrCreateClientConn(addr rpcAddress) clientConnection[C] {
	c.mu.RLock()
	cc, ok := c.mu.conns[addr]
	c.mu.RUnlock()
	if ok {
		return cc
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if cc, ok = c.mu.conns[addr]; ok {
		return cc
	}
	grpcConn := c.rpcFactory.CreateHistoryGRPCConnection(string(addr))
	cc = clientConnection[C]{
		grpcClient: c.clientCtor(grpcConn),
		grpcConn:   grpcConn,
	}

	c.mu.conns[addr] = cc
	return cc
}

func (c *connectionPoolImpl[C]) getAllClientConns() []clientConnection[C] {
	hostInfos := c.historyServiceResolver.Members()

	var clientConns []clientConnection[C]
	for _, hostInfo := range hostInfos {
		cc := c.getOrCreateClientConn(rpcAddress(hostInfo.GetAddress()))
		clientConns = append(clientConns, cc)
	}

	return clientConns
}

func (c *connectionPoolImpl[C]) resetConnectBackoff(cc clientConnection[C]) {
	cc.grpcConn.ResetConnectBackoff()
}

// closeConn removes the conn for addr from the pool and closes it.
func (c *connectionPoolImpl[C]) closeConn(addr rpcAddress) {
	c.mu.Lock()
	cc, ok := c.mu.conns[addr]
	if ok {
		delete(c.mu.conns, addr)
	}
	c.mu.Unlock()
	if ok {
		if err := cc.grpcConn.Close(); err != nil {
			c.logger.Warn("Error closing evicted gRPC connection", tag.Error(err))
		}
	}
}
