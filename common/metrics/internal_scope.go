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

package metrics

import (
	"time"
)

type (
	// Scope is an interface for metric.
	internalScope interface {
		// IncCounter increments a counter metric
		IncCounter(counter int)
		// AddCounter adds delta to the counter metric
		AddCounter(counter int, delta int64)
		AddCounterInternal(name string, delta int64)
		// StartTimer starts a timer for the given metric name.
		// Time will be recorded when stopwatch is stopped.
		StartTimer(timer int) Stopwatch
		StartTimerInternal(timer string) Stopwatch
		// RecordTimer records a timer for the given metric name
		RecordTimer(timer int, d time.Duration)
		RecordTimerInternal(timer string, d time.Duration)
		// RecordDistribution records a distribution (wrapper on top of timer) for the given
		// metric name
		RecordDistribution(id int, d int)
		RecordDistributionInternal(id string, unit MetricUnit, d int)
		// UpdateGauge reports Gauge type absolute value metric
		UpdateGauge(gauge int, value float64)
		// Tagged returns an internal scope that can be used to add additional
		// information to metrics
		Tagged(tags ...Tag) Scope
		TaggedInternal(tags ...Tag) internalScope
	}
)
