package replication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	serviceerrors "go.temporal.io/server/common/serviceerror"
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

func TestWrapEventLoopFn_ContextCancelled_ShouldStopLoop(t *testing.T) {
	assertion := require.New(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	originalEventLoopCallCount := 0
	originalEventLoop := func() error {
		originalEventLoopCallCount++
		return nil
	}
	stopFuncCallCount := 0
	stopFunc := func() {
		stopFuncCallCount++
	}
	WrapEventLoop(ctx, originalEventLoop, stopFunc, log.NewNoopLogger(), metrics.NoopMetricsHandler, NewClusterShardKey(1, 1), NewClusterShardKey(2, 1), tests.NewDynamicConfig())
	assertion.Equal(0, originalEventLoopCallCount)
	assertion.Equal(1, stopFuncCallCount)
}

func TestStrmClusterIDToClusterNameShardCount_Found(t *testing.T) {
	name, shardCount, err := ClusterIDToClusterNameShardCount(
		cluster.TestAllClusterInfo,
		int32(cluster.TestCurrentClusterInitialFailoverVersion),
	)
	require.NoError(t, err)
	require.Equal(t, cluster.TestCurrentClusterName, name)
	require.Equal(t, int32(8), shardCount)
}

func TestStrmClusterIDToClusterNameShardCount_NotFound(t *testing.T) {
	_, _, err := ClusterIDToClusterNameShardCount(cluster.TestAllClusterInfo, int32(9999))
	require.Error(t, err)
}

func TestStrmIsRetryableError(t *testing.T) {
	require.False(t, isRetryableError(&persistence.ShardOwnershipLostError{ShardID: 1, Msg: "lost"}))
	require.False(t, isRetryableError(serviceerrors.NewShardOwnershipLost("owner", "current")))
	require.False(t, isRetryableError(NewStreamError("stream", ErrClosed)))
	require.False(t, isRetryableError(context.Canceled))
	require.True(t, isRetryableError(errors.New("some retryable error")))
}

func TestStrmLivenessMonitor_ShutdownBeforeSignal(t *testing.T) {
	signalChan := make(chan struct{})
	shutdownChan := channel.NewShutdownOnce()
	shutdownChan.Shutdown()
	stopCount := 0
	livenessMonitor(
		signalChan,
		dynamicconfig.GetDurationPropertyFn(time.Second),
		dynamicconfig.GetIntPropertyFn(1),
		shutdownChan,
		func() { stopCount++ },
		log.NewNoopLogger(),
	)
	require.Equal(t, 0, stopCount)
}

func TestStrmLivenessMonitor_TimeoutStopsStream(t *testing.T) {
	signalChan := make(chan struct{}, 1)
	shutdownChan := channel.NewShutdownOnce()
	stopCh := make(chan struct{})
	stopCount := 0
	go func() {
		livenessMonitor(
			signalChan,
			dynamicconfig.GetDurationPropertyFn(10*time.Millisecond),
			dynamicconfig.GetIntPropertyFn(1),
			shutdownChan,
			func() {
				stopCount++
				close(stopCh)
			},
			log.NewNoopLogger(),
		)
	}()
	// send first signal to start monitoring; no further signals -> timeout fires -> stopStream
	signalChan <- struct{}{}
	select {
	case <-stopCh:
		require.Equal(t, 1, stopCount)
	case <-time.After(3 * time.Second):
		t.Fatal("livenessMonitor did not stop the stream on timeout")
	}
}

func TestStrmLivenessMonitor_SignalResetsThenShutdown(t *testing.T) {
	signalChan := make(chan struct{}, 2)
	shutdownChan := channel.NewShutdownOnce()
	done := make(chan struct{})
	stopCount := 0
	go func() {
		livenessMonitor(
			signalChan,
			dynamicconfig.GetDurationPropertyFn(30*time.Millisecond),
			dynamicconfig.GetIntPropertyFn(1),
			shutdownChan,
			func() { stopCount++ },
			log.NewNoopLogger(),
		)
		close(done)
	}()
	// start monitoring
	signalChan <- struct{}{}
	// keep feeding signals so the heartbeat keeps resetting, then shut down.
	// The buffered channel + blocking sends pace the signals as the monitor
	// drains them, so each send exercises the timer-reset branch.
	for range 5 {
		signalChan <- struct{}{}
	}
	shutdownChan.Shutdown()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("livenessMonitor did not return after shutdown")
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
