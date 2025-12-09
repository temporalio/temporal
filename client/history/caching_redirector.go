package history

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	serviceerrors "go.temporal.io/server/common/serviceerror"
)

type (
	cacheEntry[C any] struct {
		shardID    int32
		address    rpcAddress
		connection clientConnection[C]
		staleAt    time.Time
	}

	// A CachingRedirector is a redirector that maintains a cache of shard
	// owners, and uses that cache instead of querying membership for each
	// operation. Cache entries are evicted either for shard ownership lost
	// errors, or for any error that might indicate the history instance
	// is no longer available, including timeouts.
	CachingRedirector[C any] struct {
		mu struct {
			sync.RWMutex
			cache map[int32]cacheEntry[C]
		}

		connections            connectionPool[C]
		goros                  goro.Group
		historyServiceResolver membership.ServiceResolver
		logger                 log.Logger
		membershipUpdateCh     chan *membership.ChangedEvent
		staleTTL               dynamicconfig.DurationPropertyFn
		listenerName           string
	}
)

func NewCachingRedirector[C any](
	connections connectionPool[C],
	historyServiceResolver membership.ServiceResolver,
	logger log.Logger,
	staleTTL dynamicconfig.DurationPropertyFn,
) *CachingRedirector[C] {
	r := &CachingRedirector[C]{
		connections:            connections,
		historyServiceResolver: historyServiceResolver,
		logger:                 logger,
		membershipUpdateCh:     make(chan *membership.ChangedEvent, 1),
		staleTTL:               staleTTL,
		listenerName:           fmt.Sprintf("cachingRedirectorListener-%s", uuid.New().String()),
	}
	r.mu.cache = make(map[int32]cacheEntry[C])

	r.goros.Go(r.eventLoop)

	return r
}

func (r *CachingRedirector[C]) stop() {
	r.goros.Cancel()
	r.goros.Wait()
}

func (r *CachingRedirector[C]) clientForShardID(shardID int32) (C, error) {
	var zero C
	if err := checkShardID(shardID); err != nil {
		return zero, err
	}
	entry, err := r.getOrCreateEntry(shardID)
	if err != nil {
		return zero, err
	}
	return entry.connection.grpcClient, nil
}

func (r *CachingRedirector[C]) Execute(ctx context.Context, shardID int32, op ClientOperation[C]) error {
	if err := checkShardID(shardID); err != nil {
		return err
	}
	opEntry, err := r.getOrCreateEntry(shardID)
	if err != nil {
		return err
	}
	return r.redirectLoop(ctx, opEntry, op)
}

func (r *CachingRedirector[C]) redirectLoop(ctx context.Context, opEntry cacheEntry[C], op ClientOperation[C]) error {
	for {
		if err := common.IsValidContext(ctx); err != nil {
			return err
		}
		opErr := op(ctx, opEntry.connection.grpcClient)
		if opErr == nil {
			return opErr
		}
		if maybeHostDownError(opErr) {
			r.cacheDeleteByAddress(opEntry.address)
			return opErr
		}
		var solErr *serviceerrors.ShardOwnershipLost
		if !errors.As(opErr, &solErr) {
			return opErr
		}
		var again bool
		opEntry, again = r.handleSolError(opEntry, solErr)
		if !again {
			return opErr
		}
	}
}

func (r *CachingRedirector[C]) getOrCreateEntry(shardID int32) (cacheEntry[C], error) {
	r.mu.RLock()
	entry, ok := r.mu.cache[shardID]
	r.mu.RUnlock()
	if ok {
		if entry.staleAt.IsZero() || time.Now().Before(entry.staleAt) {
			return entry, nil
		}
		// Otherwise, check below under write lock.
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Recheck under write lock.
	entry, ok = r.mu.cache[shardID]
	if ok {
		if entry.staleAt.IsZero() || time.Now().Before(entry.staleAt) {
			return entry, nil
		}
		// Delete and fallthrough below to re-check ownership.
		delete(r.mu.cache, shardID)
	}

	address, err := shardLookup(r.historyServiceResolver, shardID)
	if err != nil {
		return cacheEntry[C]{}, err
	}

	return r.cacheAddLocked(shardID, address), nil
}

func (r *CachingRedirector[C]) cacheAddLocked(shardID int32, addr rpcAddress) cacheEntry[C] {
	// New history instances might reuse the address of a previously live history
	// instance. Since we don't currently close GRPC connections when they become
	// unused or idle, we might have a GRPC connection that has gone into its
	// connection backoff state, due to the previous history instance becoming
	// unreachable. A request on the GRPC connection, intended for the new history
	// instance, would be delayed waiting for the next connection attempt, which
	// could be many seconds.
	// If we're adding a new cache entry for a shard, we take that as a hint that
	// the next request should attempt to connect immediately if required. If the
	// GRPC connection is not in connect backoff, this call has no effect.
	connection := r.connections.getOrCreateClientConn(addr)
	r.connections.resetConnectBackoff(connection)

	entry := cacheEntry[C]{
		shardID:    shardID,
		address:    addr,
		connection: connection,
		// staleAt is left at zero; it's only set when r.staleTTL is set,
		// and after a membership update informs us that this address is no
		// longer the shard owner.
	}
	r.mu.cache[shardID] = entry

	return entry
}

func (r *CachingRedirector[C]) cacheDeleteByAddress(address rpcAddress) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for shardID, entry := range r.mu.cache {
		if entry.address == address {
			delete(r.mu.cache, shardID)
		}
	}
}

func (r *CachingRedirector[C]) handleSolError(opEntry cacheEntry[C], solErr *serviceerrors.ShardOwnershipLost) (cacheEntry[C], bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if cached, ok := r.mu.cache[opEntry.shardID]; ok {
		if cached.address == opEntry.address {
			delete(r.mu.cache, cached.shardID)
		}
	}

	solErrNewOwner := rpcAddress(solErr.OwnerHost)
	if len(solErrNewOwner) != 0 && solErrNewOwner != opEntry.address {
		r.logger.Info("historyClient: updating cache from shard ownership lost error",
			tag.ShardID(opEntry.shardID),
			tag.NewAnyTag("oldAddress", opEntry.address),
			tag.NewAnyTag("newAddress", solErrNewOwner))
		return r.cacheAddLocked(opEntry.shardID, solErrNewOwner), true
	}

	return cacheEntry[C]{}, false
}

func maybeHostDownError(opErr error) bool {
	var unavail *serviceerror.Unavailable
	if errors.As(opErr, &unavail) {
		return true
	}
	return common.IsContextDeadlineExceededErr(opErr)
}

func (r *CachingRedirector[C]) eventLoop(ctx context.Context) error {
	if err := r.historyServiceResolver.AddListener(r.listenerName, r.membershipUpdateCh); err != nil {
		r.logger.Fatal("Error adding listener", tag.Error(err))
	}
	defer func() {
		if err := r.historyServiceResolver.RemoveListener(r.listenerName); err != nil {
			r.logger.Warn("Error removing listener", tag.Error(err))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-r.membershipUpdateCh:
			r.staleCheck()
		}
	}
}

func (r *CachingRedirector[C]) staleCheck() {
	staleTTL := r.staleTTL()

	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	for shardID, entry := range r.mu.cache {
		if !entry.staleAt.IsZero() {
			if now.After(entry.staleAt) {
				delete(r.mu.cache, shardID)
			}
			continue
		}
		if staleTTL > 0 {
			addr, err := shardLookup(r.historyServiceResolver, shardID)
			if err != nil || addr != entry.address {
				entry.staleAt = now.Add(staleTTL)
				r.mu.cache[shardID] = entry
			}
		}
	}
}
