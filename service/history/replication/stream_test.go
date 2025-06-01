package replication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tests"
)

func TestWrapEventLoopFn_ReturnStreamError_ShouldStopLoop(t *testing.T) {
	config := tests.NewDynamicConfig()

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
		WrapEventLoop(context.Background(), originalEventLoop, stopFunc, log.NewNoopLogger(), metrics.NoopMetricsHandler, NewClusterShardKey(1, 1), NewClusterShardKey(2, 1), config)
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
		WrapEventLoop(context.Background(), originalEventLoop, stopFunc, log.NewNoopLogger(), metrics.NoopMetricsHandler, NewClusterShardKey(1, 1), NewClusterShardKey(2, 1), tests.NewDynamicConfig())
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
		WrapEventLoop(context.Background(), originalEventLoop, stopFunc, log.NewNoopLogger(), metrics.NoopMetricsHandler, NewClusterShardKey(1, 1), NewClusterShardKey(2, 1), tests.NewDynamicConfig())
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
