// Copyright (c) 2017 Uber Technologies, Inc.
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

package metrics

import (
	"time"

	"github.com/uber-go/tally"
)

type (
	// Client is  the interface used to report metrics tally.
	Client interface {
		// IncCounter increments a counter metric
		IncCounter(scope int, counter int)
		// AddCounter adds delta to the counter metric
		AddCounter(scope int, counter int, delta int64)
		// StartTimer starts a timer for the given
		// metric name. Time will be recorded when stopwatch is stopped.
		StartTimer(scope int, timer int) tally.Stopwatch
		// RecordTimer starts a timer for the given
		// metric name
		RecordTimer(scope int, timer int, d time.Duration)
		// UpdateGauge reports Gauge type metric
		UpdateGauge(scope int, gauge int, delta float64)
		// Tagged returns a client that adds the given tags to all metrics
		Tagged(tags map[string]string) Client
	}
)
