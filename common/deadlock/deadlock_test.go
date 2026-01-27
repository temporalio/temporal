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
	"go.temporal.io/server/common/testing/eventually"
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

	eventually.Require(t, func(t *eventually.T) {
		require.Equal(t, int64(1), dd.CurrentSuspected())

		snapshot := capture.Snapshot()
		current := snapshot[metrics.DDCurrentSuspectedDeadlocks.Name()]
		counter := snapshot[metrics.DDSuspectedDeadlocks.Name()]
		if !assert.Len(t, current, 1) || !assert.Len(t, counter, 1) {
			return
		}
		require.Equal(t, 1.0, current[0].Value)
		require.Equal(t, int64(1), counter[0].Value)
	}, 2*time.Second, time.Millisecond)

	close(b.done)

	eventually.Require(t, func(t *eventually.T) {
		require.Equal(t, int64(0), dd.CurrentSuspected())

		snapshot := capture.Snapshot()
		current := snapshot[metrics.DDCurrentSuspectedDeadlocks.Name()]
		counter := snapshot[metrics.DDSuspectedDeadlocks.Name()]
		if !assert.Len(t, current, 2) || !assert.Len(t, counter, 1) {
			return
		}
		require.Equal(t, 0.0, current[1].Value)
	}, 2*time.Second, time.Millisecond)
}
