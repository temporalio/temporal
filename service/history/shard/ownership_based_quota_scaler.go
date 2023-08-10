// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package shard

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"go.temporal.io/server/common/quotas"
)

type (
	// OwnershipBasedQuotaScaler scales rate-limiting quotas linearly with the fraction of the total shards in the
	// cluster owned by this host. The purpose is to allocate more quota to hosts with a higher workload. This object
	// can be obtained from the fx Module within this package.
	OwnershipBasedQuotaScaler struct {
		shardCounter          ShardCounter
		totalNumShards        int
		updateAppliedCallback chan struct{}
	}
	// OwnershipScaledRateBurst is a quotas.RateBurst implementation that scales the RPS and burst quotas linearly with
	// the fraction of the total shards in the cluster owned by this host. The effective Rate and Burst are both
	// multiplied by (shardCount / totalShards). Note that there is no scaling until the first shard count update is
	// received.
	OwnershipScaledRateBurst struct {
		// rb is the base rate burst that we will scale.
		rb quotas.RateBurst
		// shardCount is the number of shards owned by this host.
		shardCount atomic.Int64
		// totalShards is the total number of shards in the cluster.
		totalShards int
		// subscription is the subscription to the shard counter.
		subscription ShardCountSubscription
		// updateAppliedCallback is a callback channel that is sent to when the shard count updates are applied. This is
		// useful for testing. In production, it should be nil.
		updateAppliedCallback chan struct{}
		// wg is a wait group that is used to wait for the shard count subscription goroutine to exit.
		wg sync.WaitGroup
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
) (*OwnershipBasedQuotaScaler, error) {
	if totalNumShards <= 0 {
		return nil, fmt.Errorf("%w: %d", ErrNonPositiveTotalNumShards, totalNumShards)
	}

	return &OwnershipBasedQuotaScaler{
		shardCounter:          shardCounter,
		totalNumShards:        totalNumShards,
		updateAppliedCallback: updateAppliedCallback,
	}, nil
}

// ScaleRateBurst returns a new OwnershipScaledRateBurst instance which scales the rate/burst quotas of the base
// RateBurst by the fraction of the total shards in the cluster owned by this host. You should call
// OwnershipScaledRateBurst.StopScaling on the returned instance when you are done with it to avoid leaking resources.
func (s *OwnershipBasedQuotaScaler) ScaleRateBurst(rb quotas.RateBurst) *OwnershipScaledRateBurst {
	return newOwnershipScaledRateBurst(rb, s.shardCounter, s.totalNumShards, s.updateAppliedCallback)
}

func newOwnershipScaledRateBurst(
	rb quotas.RateBurst,
	shardCounter ShardCounter,
	totalNumShards int,
	updateAppliedCallback chan struct{},
) *OwnershipScaledRateBurst {
	subscription := shardCounter.SubscribeShardCount()
	srb := &OwnershipScaledRateBurst{
		rb:                    rb,
		totalShards:           totalNumShards,
		subscription:          subscription,
		updateAppliedCallback: updateAppliedCallback,
	}
	// Initialize the shard count to the shardCountNotSet sentinel value so that we don't try to apply the scale factor
	// until we receive the first shard count.
	srb.shardCount.Store(shardCountNotSet)
	srb.wg.Add(1)

	go srb.startScaling()

	return srb
}

// Rate returns the rate of the base rate limiter multiplied by the shard ownership share.
func (rb *OwnershipScaledRateBurst) Rate() float64 {
	return rb.rb.Rate() * rb.scaleFactor()
}

// Burst returns the burst quota of the base rate limiter multiplied by the shard ownership share, rounded up to the
// nearest integer. We round up because we don't want to let this drop to zero unless the base burst is zero.
func (rb *OwnershipScaledRateBurst) Burst() int {
	return int(math.Ceil(float64(rb.rb.Burst()) * rb.scaleFactor()))
}

// scaleFactor returns the fraction of the total shards in the cluster owned by this host. It returns 1.0 if there
// haven't been any shard count updates yet.
func (rb *OwnershipScaledRateBurst) scaleFactor() float64 {
	shardCount := rb.shardCount.Load()
	if shardCount == shardCountNotSet {
		// If the shard count is not set, then we haven't received the first shard count update yet. In this case, we
		// return 1.0 so that the base rate/burst quotas are not scaled.
		return 1.0
	}

	return float64(shardCount) / float64(rb.totalShards)
}

func (rb *OwnershipScaledRateBurst) startScaling() {
	defer rb.wg.Done()

	for shardCount := range rb.subscription.ShardCount() {
		rb.shardCount.Store(int64(shardCount))

		if rb.updateAppliedCallback != nil {
			rb.updateAppliedCallback <- struct{}{}
		}
	}
}

// StopScaling unsubscribes from the shard counter and stops scaling the rate and burst quotas. This method blocks until
// the shard count subscription goroutine exits (which should be almost immediately).
func (rb *OwnershipScaledRateBurst) StopScaling() {
	rb.subscription.Unsubscribe()
	rb.wg.Wait()
}
