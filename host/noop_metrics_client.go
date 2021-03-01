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

package host

import (
	"time"

	"go.temporal.io/server/common/metrics"
)

type (
	noopMetricsClient struct{}
)

func (m noopMetricsClient) IncCounter(scope int, counter int) {}

func (m noopMetricsClient) AddCounter(scope int, counter int, delta int64) {}

func (m noopMetricsClient) StartTimer(scope int, timer int) metrics.Stopwatch {
	panic("Unexpected call to NoopMetricsClient StartTimer")
}

func (m noopMetricsClient) RecordTimer(scope int, timer int, d time.Duration) {}

func (m noopMetricsClient) RecordDistribution(scope int, timer int, d int) {}

func (m noopMetricsClient) UpdateGauge(scope int, gauge int, value float64) {}

func (m noopMetricsClient) Scope(scope int, tags ...metrics.Tag) metrics.Scope {
	panic("Unexpected call to NoopMetricsClient Scope")
}
