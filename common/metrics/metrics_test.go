package metrics_test

import (
	"fmt"
	"slices"
	"time"

	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	expmaps "golang.org/x/exp/maps"
)

var (
	bytesHist = metrics.NewBytesHistogramDef("test-bytes-histogram")
	counter   = metrics.NewCounterDef("test-counter")
	gauge     = metrics.NewGaugeDef("test-gauge")
	hist      = metrics.NewDimensionlessHistogramDef("test-histogram")
	timer     = metrics.NewTimerDef("test-timer")
)

func ExampleHandler() {
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()

	// Instead of calling handler.Histogram(bytesHist.Name(), bytesHist.Unit()).Record(1), you should use the With
	// method of the metric definition instead.

	bytesHist.With(handler).Record(1)
	counter.With(handler).Record(2)
	gauge.With(handler).Record(3)
	hist.With(handler).Record(4)
	timer.With(handler).Record(5 * time.Second)

	snapshot := capture.Snapshot()
	keys := expmaps.Keys(snapshot)
	slices.Sort(keys)
	for _, key := range keys {
		fmt.Printf("%s:", key)
		for _, rec := range snapshot[key] {
			fmt.Printf(" %v (%v)", rec.Value, rec.Unit)
		}
		fmt.Println()
	}

	// Notice that the output includes the proper units without having to specify them in the call to Record.

	// Output:
	// test-bytes-histogram: 1 (By)
	// test-counter: 2 ()
	// test-gauge: 3 ()
	// test-histogram: 4 (1)
	// test-timer: 5s ()
}
