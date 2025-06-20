package shard

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

type (
	OwnershipBasedQuotaScaler interface {
		ScaleFactor() (float64, bool)
	}

	// OwnershipBasedQuotaScalerImpl scales rate-limiting quotas linearly with the fraction of the total shards in the
	// cluster owned by this host. The purpose is to allocate more quota to hosts with a higher workload. This object
	// can be obtained from the fx Module within this package.
	OwnershipBasedQuotaScalerImpl struct {
		shardCounter          ShardCounter
		totalNumShards        int
		updateAppliedCallback chan struct{}

		shardCount   atomic.Int64
		subscription ShardCountSubscription
		shutdownWG   sync.WaitGroup
	}

	LazyLoadedOwnershipBasedQuotaScaler struct {
		*atomic.Value // value type is OwnershipBasedQuotaScaler
	}

	// ShardCountSubscription is a subscription to a ShardCounter. It provides a channel that receives the
	// shard count updates and an Unsubscribe method that unsubscribes from the counter.
	ShardCountSubscription interface {
		// ShardCount returns a channel that receives shard count updates.
		ShardCount() <-chan int
		// Unsubscribe unsubscribes from the shard counter. This closes the ShardCount channel.
		Unsubscribe()
	}

	// ShardCounter is an observable object that emits the current shard count.
	ShardCounter interface {
		// SubscribeShardCount returns a ShardCountSubscription for receiving shard count updates.
		SubscribeShardCount() ShardCountSubscription
	}
)

var (
	// shardCountNotSet is a sentinel value for the shardCount which indicates that it hasn't been set yet. It's an
	// int64 because that's the type of the atomic.
	shardCountNotSet int64 = -1

	ErrNonPositiveTotalNumShards = errors.New("totalNumShards must be greater than 0")
)

// NewOwnershipBasedQuotaScaler returns an OwnershipBasedQuotaScaler. The updateAppliedCallback field is a channel which
// is sent to in a blocking fashion when the shard count updates are applied. This is useful for testing. In production,
// you should pass in nil, which will cause the callback to be ignored. If totalNumShards is non-positive, then an error
// is returned.
func NewOwnershipBasedQuotaScaler(
	shardCounter ShardCounter,
	totalNumShards int,
	updateAppliedCallback chan struct{},
) (*OwnershipBasedQuotaScalerImpl, error) {
	if totalNumShards <= 0 {
		return nil, fmt.Errorf("%w: %d", ErrNonPositiveTotalNumShards, totalNumShards)
	}

	scaler := &OwnershipBasedQuotaScalerImpl{
		shardCounter:          shardCounter,
		totalNumShards:        totalNumShards,
		updateAppliedCallback: updateAppliedCallback,
		subscription:          shardCounter.SubscribeShardCount(),
	}

	scaler.shardCount.Store(shardCountNotSet)
	scaler.shutdownWG.Add(1)
	go func() {
		defer scaler.shutdownWG.Done()

		for count := range scaler.subscription.ShardCount() {
			scaler.shardCount.Store(int64(count))
			if scaler.updateAppliedCallback != nil {
				scaler.updateAppliedCallback <- struct{}{}
			}
		}
	}()

	return scaler, nil
}

func (s *OwnershipBasedQuotaScalerImpl) ScaleFactor() (float64, bool) {
	shardCount := s.shardCount.Load()
	if shardCount == shardCountNotSet {
		return 0, false
	}

	return float64(shardCount) / float64(s.totalNumShards), true
}

func (s *OwnershipBasedQuotaScalerImpl) Close() {
	s.subscription.Unsubscribe()
	s.shutdownWG.Wait()
}

func (s LazyLoadedOwnershipBasedQuotaScaler) ScaleFactor() (float64, bool) {
	if value := s.Load(); value != nil {
		return value.(OwnershipBasedQuotaScaler).ScaleFactor()
	}
	return 0, false
}
