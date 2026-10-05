package replication

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/dynamicconfig"
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

func TestMaxLifetimeMonitor_Disabled_ShouldNotStopStream(t *testing.T) {
	assertion := require.New(t)

	var stopCount atomic.Int32
	shutdownChan := channel.NewShutdownOnce()
	defer shutdownChan.Shutdown()

	// maxLifetime <= 0 disables the monitor: it must return immediately without stopping.
	maxLifetimeMonitor(
		dynamicconfig.GetDurationPropertyFn(0),
		dynamicconfig.GetFloatPropertyFn(0.1),
		shutdownChan,
		func() { stopCount.Add(1) },
		log.NewNoopLogger(),
	)

	assertion.Equal(int32(0), stopCount.Load())
}

func TestMaxLifetimeMonitor_LifetimeElapsed_ShouldStopStreamOnce(t *testing.T) {
	assertion := require.New(t)

	var stopCount atomic.Int32
	shutdownChan := channel.NewShutdownOnce()
	defer shutdownChan.Shutdown()

	done := make(chan struct{})
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	go func() {
		maxLifetimeMonitor(
			dynamicconfig.GetDurationPropertyFn(10*time.Millisecond),
			dynamicconfig.GetFloatPropertyFn(0.1),
			shutdownChan,
			func() { stopCount.Add(1) },
			log.NewNoopLogger(),
		)
		close(done)
	}()

	select {
	case <-done:
		assertion.Equal(int32(1), stopCount.Load())
	case <-timer.C:
		t.Fatal("maxLifetimeMonitor did not stop the stream within timeout")
	}
}

func TestMaxLifetimeMonitor_ShutdownBeforeLifetime_ShouldNotStopStream(t *testing.T) {
	assertion := require.New(t)

	var stopCount atomic.Int32
	shutdownChan := channel.NewShutdownOnce()

	done := make(chan struct{})
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	go func() {
		// Long lifetime so it never fires; shutdown must unblock the monitor first.
		maxLifetimeMonitor(
			dynamicconfig.GetDurationPropertyFn(time.Hour),
			dynamicconfig.GetFloatPropertyFn(0.1),
			shutdownChan,
			func() { stopCount.Add(1) },
			log.NewNoopLogger(),
		)
		close(done)
	}()
	shutdownChan.Shutdown()

	select {
	case <-done:
		assertion.Equal(int32(0), stopCount.Load())
	case <-timer.C:
		t.Fatal("maxLifetimeMonitor did not return after shutdown")
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
