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

	sc := &shardCounter{
		ch:     make(chan int),
		closed: false,
	}
	totalNumShards := 10
	updateAppliedCallback := make(chan struct{})
	scaler, err := shard.NewOwnershipBasedQuotaScaler(sc, totalNumShards, updateAppliedCallback)
	require.NoError(t, err)
	_, ok := scaler.ScaleFactor()
	assert.False(t, ok, "ScaleFactor should return false before any shard count updates")

	sc.ch <- 3
	// Wait for the update to be applied. Even though the send above is blocking, we still need to wait for the
	// rate/burst scaler's goroutine to use it to adjust the scale factor.
	<-updateAppliedCallback

	// After the update is applied, the scale factor is calculated as 3/10 = 0.3
	factor, ok := scaler.ScaleFactor()
	assert.True(t, ok)
	assert.Equal(t, 0.3, factor)

	assert.False(t, sc.closed, "The shard counter should not be closed until the srb scaler is stopped")
	scaler.Close()
	assert.True(t, sc.closed, "The shard counter should be closed after the srb scaler is stopped")
}
