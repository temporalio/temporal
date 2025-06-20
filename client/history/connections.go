//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination connections_mock.go

package history

import (
	"sync"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/membership"
	"google.golang.org/grpc"
)

type (
	clientConnection struct {
		historyClient historyservice.HistoryServiceClient
		grpcConn      *grpc.ClientConn
	}

	rpcAddress string

	connectionPoolImpl struct {
		mu struct {
			sync.RWMutex
			conns map[rpcAddress]clientConnection
		}

		historyServiceResolver membership.ServiceResolver
		rpcFactory             RPCFactory
	}

	// RPCFactory is a subset of the [go.temporal.io/server/common/rpc.RPCFactory] interface to make testing easier.
	RPCFactory interface {
		CreateHistoryGRPCConnection(rpcAddress string) *grpc.ClientConn
	}

	connectionPool interface {
		getOrCreateClientConn(addr rpcAddress) clientConnection
		getAllClientConns() []clientConnection
		resetConnectBackoff(clientConnection)
	}
)

func newConnectionPool(
	historyServiceResolver membership.ServiceResolver,
	rpcFactory RPCFactory,
) *connectionPoolImpl {
	c := &connectionPoolImpl{
		historyServiceResolver: historyServiceResolver,
		rpcFactory:             rpcFactory,
	}
	c.mu.conns = make(map[rpcAddress]clientConnection)
	return c
}

func (c *connectionPoolImpl) getOrCreateClientConn(addr rpcAddress) clientConnection {
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
	cc = clientConnection{
		historyClient: historyservice.NewHistoryServiceClient(grpcConn),
		grpcConn:      grpcConn,
	}

	c.mu.conns[addr] = cc
	return cc
}

func (c *connectionPoolImpl) getAllClientConns() []clientConnection {
	hostInfos := c.historyServiceResolver.Members()

	var clientConns []clientConnection
	for _, hostInfo := range hostInfos {
		cc := c.getOrCreateClientConn(rpcAddress(hostInfo.GetAddress()))
		clientConns = append(clientConns, cc)
	}

	return clientConns
}

func (c *connectionPoolImpl) resetConnectBackoff(cc clientConnection) {
	cc.grpcConn.ResetConnectBackoff()
}
