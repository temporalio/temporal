package history

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/util"
	"google.golang.org/grpc"
)

type (
	clientConnection[C any] struct {
		grpcClient C
		grpcConn   *grpc.ClientConn
	}

	rpcAddress string

	connectionPoolImpl[C any] struct {
		mu                     *sync.RWMutex
		conns                  map[rpcAddress]clientConnection[C]
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
	mu := &sync.RWMutex{}
	conns := make(map[rpcAddress]clientConnection[C])

	c := &connectionPoolImpl[C]{
		mu:                     mu,
		conns:                  conns,
		historyServiceResolver: historyServiceResolver,
		rpcFactory:             rpcFactory,
		clientCtor:             clientCtor,
		logger:                 logger,
	}

	// Close cached conns whose host leaves the membership ring. The goroutine
	// captures only locals (resolver, logger, mu, conns, delay) — not c — so
	// dropping c lets runtime.AddCleanup fire and shut it down.
	ctx, cancel := context.WithCancel(context.Background())
	go watchMembershipForClose(ctx, historyServiceResolver, logger, mu, conns, connectionCloseDelay)
	runtime.AddCleanup(c, func(cancel context.CancelFunc) { cancel() }, cancel)
	return c
}

func watchMembershipForClose[C any](
	ctx context.Context,
	resolver membership.ServiceResolver,
	logger log.Logger,
	mu *sync.RWMutex,
	conns map[rpcAddress]clientConnection[C],
	connectionCloseDelay dynamicconfig.DurationPropertyFn,
) {
	listenerName := fmt.Sprintf("historyConnectionPool-%s", uuid.New().String())
	ch := make(chan *membership.ChangedEvent, 1)
	if err := resolver.AddListener(listenerName, ch); err != nil {
		logger.Error("Failed to subscribe history connection pool to membership", tag.Error(err))
		return
	}
	defer resolver.RemoveListener(listenerName)
	for {
		select {
		case <-ctx.Done():
			return
		case event := <-ch:
			for _, h := range event.HostsRemoved {
				addr := rpcAddress(h.GetAddress())
				go closeConnAfterDelay(ctx, resolver, logger, mu, conns, addr, connectionCloseDelay())
			}
		}
	}
}

func closeConnAfterDelay[C any](
	ctx context.Context,
	resolver membership.ServiceResolver,
	logger log.Logger,
	mu *sync.RWMutex,
	conns map[rpcAddress]clientConnection[C],
	addr rpcAddress,
	delay time.Duration,
) {
	if util.InterruptibleSleep(ctx, delay) != nil {
		return
	}
	for _, m := range resolver.Members() {
		if rpcAddress(m.GetAddress()) == addr {
			return
		}
	}
	mu.Lock()
	cc, ok := conns[addr]
	if ok {
		delete(conns, addr)
	}
	mu.Unlock()
	if ok {
		if err := cc.grpcConn.Close(); err != nil {
			logger.Warn("Error closing evicted gRPC connection", tag.Error(err))
		}
	}
}

func (c *connectionPoolImpl[C]) getOrCreateClientConn(addr rpcAddress) clientConnection[C] {
	c.mu.RLock()
	cc, ok := c.conns[addr]
	c.mu.RUnlock()
	if ok {
		return cc
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if cc, ok = c.conns[addr]; ok {
		return cc
	}
	grpcConn := c.rpcFactory.CreateHistoryGRPCConnection(string(addr))
	cc = clientConnection[C]{
		grpcClient: c.clientCtor(grpcConn),
		grpcConn:   grpcConn,
	}

	c.conns[addr] = cc
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
