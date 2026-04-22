package history

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"go.temporal.io/server/common/goro"
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

		logger             log.Logger
		listenerName       string
		membershipUpdateCh chan *membership.ChangedEvent
		goros              goro.Group
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
) *connectionPoolImpl[C] {
	c := &connectionPoolImpl[C]{
		historyServiceResolver: historyServiceResolver,
		rpcFactory:             rpcFactory,
		clientCtor:             clientCtor,
		logger:                 logger,
		listenerName:           fmt.Sprintf("historyConnectionPoolListener-%s", uuid.New().String()),
		membershipUpdateCh:     make(chan *membership.ChangedEvent, 1),
	}
	c.mu.conns = make(map[rpcAddress]clientConnection[C])
	c.goros.Go(c.evictionLoop)
	return c
}

func (c *connectionPoolImpl[C]) stop() {
	c.goros.Cancel()
	c.goros.Wait()
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

// evictionLoop listens for membership changes and closes cached connections
// whose addresses are no longer in the ring. Stale *grpc.ClientConn objects
// keep re-dialing dead hosts indefinitely; this closes them to stop the spam.
func (c *connectionPoolImpl[C]) evictionLoop(ctx context.Context) error {
	if err := c.historyServiceResolver.AddListener(c.listenerName, c.membershipUpdateCh); err != nil {
		c.logger.Error("historyConnectionPool: error adding membership listener", tag.Error(err))
		return err
	}
	defer func() {
		if err := c.historyServiceResolver.RemoveListener(c.listenerName); err != nil {
			c.logger.Warn("historyConnectionPool: error removing membership listener", tag.Error(err))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.membershipUpdateCh:
			c.evictStale()
		}
	}
}

func (c *connectionPoolImpl[C]) evictStale() {
	live := make(map[rpcAddress]struct{})
	for _, hostInfo := range c.historyServiceResolver.Members() {
		live[rpcAddress(hostInfo.GetAddress())] = struct{}{}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	for addr, cc := range c.mu.conns {
		if _, ok := live[addr]; ok {
			continue
		}
		if cc.grpcConn != nil {
			_ = cc.grpcConn.Close()
		}
		delete(c.mu.conns, addr)
		c.logger.Info("historyConnectionPool: evicted stale connection", tag.Address(string(addr)))
	}
}
