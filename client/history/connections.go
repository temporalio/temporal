package history

import (
	"sync"

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
		closeConn(addr rpcAddress)
	}
)

func NewConnectionPool[C any](
	historyServiceResolver membership.ServiceResolver,
	rpcFactory RPCFactory,
	clientCtor func(grpc.ClientConnInterface) C,
	logger log.Logger,
) *connectionPoolImpl[C] {
	c := &connectionPoolImpl[C]{
		historyServiceResolver: historyServiceResolver,
		rpcFactory:             rpcFactory,
		clientCtor:             clientCtor,
		logger:                 logger,
	}
	c.mu.conns = make(map[rpcAddress]clientConnection[C])
	return c
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
