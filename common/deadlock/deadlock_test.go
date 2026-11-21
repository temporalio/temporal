package deadlock

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/goro"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/pingable"
	"go.temporal.io/server/common/testing/await"
)

type blockingPingable struct{ done chan struct{} }

func (b *blockingPingable) GetPingChecks() []pingable.Check {
	return []pingable.Check{{
		Name:    "test",
		Timeout: 10 * time.Millisecond,
		Ping: func() []pingable.Pingable {
			<-b.done
			return nil
		},
	}}
}

func TestCurrentCounterAndGauge(t *testing.T) {
	mh := metricstest.NewCaptureHandler()
	dd := NewDeadlockDetector(params{
		Logger:         log.NewNoopLogger(),
		Collection:     dynamicconfig.NewNoopCollection(),
		MetricsHandler: mh,
	})

	lc := &loopContext{
		dd:   dd,
		p:    goro.NewAdaptivePool(clock.NewRealTimeSource(), 0, 1, 10*time.Millisecond, 10),
		root: nil,
	}
	defer lc.p.Stop()

	b := &blockingPingable{done: make(chan struct{})}
	check := b.GetPingChecks()[0]

	capture := mh.StartCapture()
	go lc.check(context.Background(), check)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		require.Equal(collect, int64(1), dd.CurrentSuspected())

		snapshot := capture.Snapshot()
		current := snapshot[metrics.DDCurrentSuspectedDeadlocks.Name()]
		counter := snapshot[metrics.DDSuspectedDeadlocks.Name()]
		require.Len(collect, current, 1)
		require.Equal(collect, 1.0, current[0].Value)
		require.Len(collect, counter, 1)
		require.Equal(collect, int64(1), counter[0].Value)
	}, 2*time.Second, time.Millisecond)

	close(b.done)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		require.Equal(collect, int64(0), dd.CurrentSuspected())

		snapshot := capture.Snapshot()
		current := snapshot[metrics.DDCurrentSuspectedDeadlocks.Name()]
		counter := snapshot[metrics.DDSuspectedDeadlocks.Name()]
		require.Len(collect, current, 2)
		require.Equal(collect, 0.0, current[1].Value)
		require.Len(collect, counter, 1)
	}, 2*time.Second, time.Millisecond)
}

func TestOnTimeoutRunsOnceWhenPingBlocks(t *testing.T) {
	dd := NewDeadlockDetector(params{
		Logger:         log.NewNoopLogger(),
		Collection:     dynamicconfig.NewNoopCollection(),
		MetricsHandler: metrics.NoopMetricsHandler,
	})
	lc := &loopContext{
		dd:   dd,
		p:    goro.NewAdaptivePool(clock.NewRealTimeSource(), 0, 1, 10*time.Millisecond, 10),
		root: nil,
	}
	defer lc.p.Stop()

	done := make(chan struct{})
	timedOut := make(chan struct{}, 2)
	check := pingable.Check{
		Name:    "test",
		Timeout: 10 * time.Millisecond,
		Ping: func() []pingable.Pingable {
			<-done
			return nil
		},
		OnTimeout: func() { timedOut <- struct{}{} },
	}
	go lc.check(t.Context(), check)

	select {
	case <-timedOut:
	case <-time.After(10 * time.Second):
		t.Fatal("OnTimeout was not called for a ping that exceeded its timeout")
	}
	close(done)
	await.RequireTrue(t, func() bool { return dd.CurrentSuspected() == 0 }, 5*time.Second, 10*time.Millisecond)
	require.Empty(t, timedOut, "OnTimeout must run once")
}

func TestOnTimeoutNotCalledWhenPingReturnsInTime(t *testing.T) {
	dd := NewDeadlockDetector(params{
		Logger:         log.NewNoopLogger(),
		Collection:     dynamicconfig.NewNoopCollection(),
		MetricsHandler: metrics.NoopMetricsHandler,
	})
	lc := &loopContext{
		dd:   dd,
		p:    goro.NewAdaptivePool(clock.NewRealTimeSource(), 0, 1, 10*time.Millisecond, 10),
		root: nil,
	}
	defer lc.p.Stop()

	timedOut := make(chan struct{}, 1)
	check := pingable.Check{
		Name:      "test",
		Timeout:   time.Minute,
		Ping:      func() []pingable.Pingable { return nil },
		OnTimeout: func() { timedOut <- struct{}{} },
	}
	// check stops the timeout timer as soon as Ping returns, so with a one-minute timeout and
	// an immediate Ping the callback can never fire.
	lc.check(t.Context(), check)
	require.Empty(t, timedOut)
	require.Equal(t, int64(0), dd.CurrentSuspected())
}
