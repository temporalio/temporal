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

package metrics_test

import (
	"fmt"
	"time"

	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
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
	keys := maps.Keys(snapshot)
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
