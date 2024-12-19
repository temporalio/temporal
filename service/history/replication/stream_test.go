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

package replication

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
)

func TestWrapEventLoopFn_ReturnStreamError_ShouldStopLoop(t *testing.T) {
	assertion := require.New(t)

	done := make(chan bool)
	timer := time.NewTimer(3 * time.Second)
	go func() {
		originalEventLoopCallCount := 0
		originalEventLoop := func() error {
			originalEventLoopCallCount++
			return NewStreamError("error closed", ErrClosed)
		}
		stopFuncCallCount := 0
		stopFunc := func() {
			stopFuncCallCount++
		}
		WrapEventLoop(originalEventLoop, stopFunc, log.NewNoopLogger(), metrics.NoopMetricsHandler, NewClusterShardKey(1, 1), NewClusterShardKey(2, 1), 0)
		assertion.Equal(1, originalEventLoopCallCount)
		assertion.Equal(1, stopFuncCallCount)

		done <- true
	}()

	select {
	case <-done:
		// Test completed within the timeout
	case <-timer.C:
		t.Fatal("Test timed out after 5 seconds")
	}
}
func TestWrapEventLoopFn_ReturnShardOwnershipLostError_ShouldStopLoop(t *testing.T) {
	assertion := require.New(t)

	done := make(chan bool)
	timer := time.NewTimer(3 * time.Second)
	go func() {
		originalEventLoopCallCount := 0
		originalEventLoop := func() error {
			originalEventLoopCallCount++
			return &persistence.ShardOwnershipLostError{
				ShardID: 123, // immutable
				Msg:     "shard closed",
			}
		}
		stopFuncCallCount := 0
		stopFunc := func() {
			stopFuncCallCount++
		}
		WrapEventLoop(originalEventLoop, stopFunc, log.NewNoopLogger(), metrics.NoopMetricsHandler, NewClusterShardKey(1, 1), NewClusterShardKey(2, 1), 0)
		assertion.Equal(1, originalEventLoopCallCount)
		assertion.Equal(1, stopFuncCallCount)

		done <- true
	}()

	select {
	case <-done:
		// Test completed within the timeout
	case <-timer.C:
		t.Fatal("Test timed out after 5 seconds")
	}
}

func TestWrapEventLoopFn_ReturnServiceError_ShouldRetryUntilStreamError(t *testing.T) {
	assertion := require.New(t)

	done := make(chan bool)
	timer := time.NewTimer(3 * time.Second)

	go func() {
		originalEventLoopCallCount := 0
		originalEventLoop := func() error {
			originalEventLoopCallCount++
			if originalEventLoopCallCount == 3 {
				return NewStreamError("error closed", ErrClosed)
			}
			return errors.New("error")
		}
		stopFuncCallCount := 0
		stopFunc := func() {
			stopFuncCallCount++
		}
		WrapEventLoop(originalEventLoop, stopFunc, log.NewNoopLogger(), metrics.NoopMetricsHandler, NewClusterShardKey(1, 1), NewClusterShardKey(2, 1), 0)
		assertion.Equal(3, originalEventLoopCallCount)
		assertion.Equal(1, stopFuncCallCount)

		done <- true
	}()

	select {
	case <-done:
		// Test completed within the timeout
	case <-timer.C:
		t.Fatal("Test timed out after 5 seconds")
	}
}
