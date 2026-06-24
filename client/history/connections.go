package history

import (
	"fmt"
	"sync"
	"time"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"google.golang.org/grpc"
)

const evictionCheckInterval = 30 * time.Second

type (
	clientConnection[C any] struct {
		grpcClient C
		grpcConn   *grpc.ClientConn
	}

	rpcAddress string

	connectionPoolImpl[C any] struct {
		conns                  *sync.Map // rpcAddress -> clientConnection[C]
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
	conns := &sync.Map{}

	c := &connectionPoolImpl[C]{
		conns:                  conns,
		historyServiceResolver: historyServiceResolver,
		rpcFactory:             rpcFactory,
		clientCtor:             clientCtor,
		logger:                 logger,
	}

	// Close cached conns whose host leaves the ring; exits when resolver stops.
	go watchMembershipForClose[C](historyServiceResolver, logger, conns, connectionCloseDelay)
	return c
}

func watchMembershipForClose[C any](
	resolver membership.ServiceResolver,
	logger log.Logger,
	conns *sync.Map,
	connectionCloseDelay dynamicconfig.DurationPropertyFn,
) {
	listenerName := fmt.Sprintf("%p", conns)
	ch := make(chan *membership.ChangedEvent, 1)
	if err := resolver.AddListener(listenerName, ch); err != nil {
		logger.Error("Failed to subscribe history connection pool to membership", tag.Error(err))
		return
	}
	defer func() { _ = resolver.RemoveListener(listenerName) }()

	// Reap departed hosts via a per-address deadline checked by a single ticker;
	// a re-add resets it to the latest removal.
	evictAt := make(map[rpcAddress]time.Time)
	ticker := time.NewTicker(evictionCheckInterval)
	defer ticker.Stop()
	done := resolver.Done()
	for {
		select {
		case <-done:
			return // resolver stopped
		case event := <-ch:
			for _, h := range event.HostsRemoved {
				evictAt[rpcAddress(h.GetAddress())] = time.Now().Add(connectionCloseDelay())
			}
			for _, h := range event.HostsAdded {
				delete(evictAt, rpcAddress(h.GetAddress()))
			}
		case <-ticker.C:
			reapClosableConns[C](resolver, logger, conns, evictAt)
		}
	}
}

func reapClosableConns[C any](
	resolver membership.ServiceResolver,
	logger log.Logger,
	conns *sync.Map,
	evictAt map[rpcAddress]time.Time,
) {
	if len(evictAt) == 0 {
		return
	}
	members := make(map[rpcAddress]struct{})
	for _, m := range resolver.Members() {
		members[rpcAddress(m.GetAddress())] = struct{}{}
	}
	now := time.Now()
	for addr, deadline := range evictAt {
		if _, ok := members[addr]; ok {
			delete(evictAt, addr) // back in the ring; cancel the eviction
			continue
		}
		if now.Before(deadline) {
			continue
		}
		if v, ok := conns.LoadAndDelete(addr); ok {
			if err := v.(clientConnection[C]).grpcConn.Close(); err != nil {
				logger.Warn("Error closing evicted gRPC connection", tag.Error(err))
			}
		}
		delete(evictAt, addr)
	}
}

func (c *connectionPoolImpl[C]) getOrCreateClientConn(addr rpcAddress) clientConnection[C] {
	if v, ok := c.conns.Load(addr); ok {
		return v.(clientConnection[C]) // nolint:revive // unchecked-type-assertion
	}

	grpcConn := c.rpcFactory.CreateHistoryGRPCConnection(string(addr))
	cc := clientConnection[C]{
		grpcClient: c.clientCtor(grpcConn),
		grpcConn:   grpcConn,
	}

	if actual, loaded := c.conns.LoadOrStore(addr, cc); loaded {
		_ = grpcConn.Close()
		return actual.(clientConnection[C]) // nolint:revive // unchecked-type-assertion
	}
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
