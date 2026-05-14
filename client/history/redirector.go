package history

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

type (
	// A Redirector executes a client operation against a history instance.
	// If the operation is intended for the owner of a shard, and the request
	// returns a shard ownership lost error with a hint for a new shard owner,
	// the redirector will retry the request to the new owner.
	Redirector[C any] interface {
		Execute(ctx context.Context, shardID int32, op ClientOperation[C]) error
		clientForShardID(int32) (C, error)
	}
	ClientOperation[C any] func(ctx context.Context, client C) error

	BasicRedirector[C any] struct {
		connections            connectionPool[C]
		historyServiceResolver membership.ServiceResolver
		logger                 log.Logger
		cancelEventLoop        context.CancelFunc
	}
)

func shardLookup(resolver membership.ServiceResolver, shardID int32) (rpcAddress, error) {
	hostInfo, err := resolver.Lookup(convert.Int32ToString(shardID))
	if err != nil {
		return "", err
	}
	return rpcAddress(hostInfo.GetAddress()), nil
}

func NewBasicRedirector[C any](
	connections connectionPool[C],
	historyServiceResolver membership.ServiceResolver,
	logger log.Logger,
) *BasicRedirector[C] {
	r := &BasicRedirector[C]{
		connections:            connections,
		historyServiceResolver: historyServiceResolver,
		logger:                 logger,
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancelEventLoop = cancel
	go r.eventLoop(ctx)
	return r
}

func (r *BasicRedirector[C]) stop() {
	r.cancelEventLoop()
}

func (r *BasicRedirector[C]) clientForShardID(shardID int32) (C, error) {
	var zero C
	if err := checkShardID(shardID); err != nil {
		return zero, err
	}
	address, err := shardLookup(r.historyServiceResolver, shardID)
	if err != nil {
		return zero, err
	}
	clientConn := r.connections.getOrCreateClientConn(address)
	return clientConn.grpcClient, nil
}

func (r *BasicRedirector[C]) Execute(ctx context.Context, shardID int32, op ClientOperation[C]) error {
	if err := checkShardID(shardID); err != nil {
		return err
	}
	address, err := shardLookup(r.historyServiceResolver, shardID)
	if err != nil {
		return err
	}
	return r.redirectLoop(ctx, address, op)
}

func (r *BasicRedirector[C]) redirectLoop(ctx context.Context, address rpcAddress, op ClientOperation[C]) error {
	for {
		if err := common.IsValidContext(ctx); err != nil {
			return err
		}
		clientConn := r.connections.getOrCreateClientConn(address)
		err := op(ctx, clientConn.grpcClient)
		var solErr *serviceerrors.ShardOwnershipLost
		if !errors.As(err, &solErr) || len(solErr.OwnerHost) == 0 {
			return err
		}
		// TODO: consider emitting a metric for number of redirects
		address = rpcAddress(solErr.OwnerHost)
	}
}

// eventLoop closes pooled connections for hosts that leave the ring.
// BasicRedirector re-resolves on every Execute, so no grace window is needed.
func (r *BasicRedirector[C]) eventLoop(ctx context.Context) {
	listenerName := fmt.Sprintf("basicRedirector-%s", uuid.New().String())
	updateCh := make(chan *membership.ChangedEvent, 1)
	if err := r.historyServiceResolver.AddListener(listenerName, updateCh); err != nil {
		r.logger.Error("Error adding membership listener", tag.Error(err))
		return
	}
	defer func() {
		if err := r.historyServiceResolver.RemoveListener(listenerName); err != nil {
			r.logger.Warn("Error removing membership listener", tag.Error(err))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case event := <-updateCh:
			for _, h := range event.HostsRemoved {
				r.connections.closeConn(rpcAddress(h.GetAddress()))
			}
		}
	}
}
