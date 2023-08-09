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

package shard_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/service/history/shard"
)

// shardCounter adapts a channel of shard count updates to the ShardCounter interface.
type shardCounter struct {
	ch     chan int
	closed bool
}

func (s *shardCounter) SubscribeShardCount() shard.ShardCountSubscription {
	return s
}

func (s *shardCounter) ShardCount() <-chan int {
	return s.ch
}

func (s *shardCounter) Unsubscribe() {
	close(s.ch)
	s.closed = true
}

// constantRateBurst is a quotas.RateBurst implementation that returns the same rate and burst every time.
type constantRateBurst struct {
	rate  float64
	burst int
}

func (rb constantRateBurst) Rate() float64 {
	return rb.rate
}

func (rb constantRateBurst) Burst() int {
	return rb.burst
}

func newRateBurst(rate float64, burst int) constantRateBurst {
	return constantRateBurst{rate: rate, burst: burst}
}

func TestOwnershipBasedQuotaScaler_NonPositiveTotalNumShards(t *testing.T) {
	t.Parallel()

	sco := &shardCounter{
		ch:     make(chan int),
		closed: false,
	}
	totalNumShards := 0
	_, err := shard.NewOwnershipBasedQuotaScaler(sco, totalNumShards, nil)
	assert.ErrorIs(t, err, shard.ErrNonPositiveTotalNumShards)
}

func TestOwnershipBasedQuotaScaler(t *testing.T) {
	t.Parallel()

	rb := newRateBurst(2, 4)
	sc := &shardCounter{
		ch:     make(chan int),
		closed: false,
	}
	totalNumShards := 10
	updateAppliedCallback := make(chan struct{})
	scaler, err := shard.NewOwnershipBasedQuotaScaler(sc, totalNumShards, updateAppliedCallback)
	require.NoError(t, err)
	srb := scaler.ScaleRateBurst(rb)
	assert.Equal(t, 2.0, srb.Rate(), "Rate should be equal to the base rate before any shard count updates")
	assert.Equal(t, 4, srb.Burst(), "Burst should be equal to the base burst before any shard count updates")
	sc.ch <- 3

	// Wait for the update to be applied. Even though the send above is blocking, we still need to wait for the
	// rate/burst scaler's goroutine to use it to adjust the scale factor.
	<-updateAppliedCallback

	// After the update is applied, the scale factor is calculated as 3/10 = 0.3, so the rate and burst should be
	// multiplied by 0.3. Since the initial rate and burst are 2 and 4, respectively, the final rate and burst should be
	// 0.6 and 1.2, respectively. However, since the burst is rounded up to the nearest integer, the final burst should
	// be 2.
	assert.Equal(t, 0.6, srb.Rate())
	assert.Equal(t, 2, srb.Burst())
	assert.False(t, sc.closed, "The shard counter should not be closed until the srb scaler is stopped")
	srb.StopScaling()
	assert.True(t, sc.closed, "The shard counter should be closed after the srb scaler is stopped")
}
