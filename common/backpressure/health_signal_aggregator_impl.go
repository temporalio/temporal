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

package backpressure

import "time"

var _ HealthSignalAggregator = (*HealthSignalAggregatorImpl)(nil)

type (
	HealthSignalAggregatorImpl struct {
		errorRatioTotal MovingWindowAvg
		latencyAvgTotal MovingWindowAvg
	}
)

func NewHealthSignalAggregatorImpl(
	windowSize time.Duration,
	maxBufferSize int,
) HealthSignalAggregator {
	return &HealthSignalAggregatorImpl{
		errorRatioTotal: NewMovingWindowAvgImpl(windowSize, maxBufferSize),
		latencyAvgTotal: NewMovingWindowAvgImpl(windowSize, maxBufferSize),
	}
}

func (a *HealthSignalAggregatorImpl) GetLatencyAverage() float64 {
	return a.latencyAvgTotal.Average()
}

func (a *HealthSignalAggregatorImpl) RecordLatency(api string) func() {
	start := time.Now()
	return func() {
		a.errorRatioTotal.Add(0)
		a.latencyAvgTotal.Add(time.Since(start).Milliseconds())
	}
}

func (a *HealthSignalAggregatorImpl) RecordLatencyByShard(api string, shardID int32) func() {
	return a.RecordLatency(api)
}

func (a *HealthSignalAggregatorImpl) RecordLatencyByNamespace(api string, nsID string) func() {
	return a.RecordLatency(api)
}

func (a *HealthSignalAggregatorImpl) RecordError(api string) {
	a.errorRatioTotal.Add(1)
}

func (a *HealthSignalAggregatorImpl) RecordErrorByShard(api string, shardID int32) {
	a.RecordError(api)
}

func (a *HealthSignalAggregatorImpl) RecordErrorByNamespace(api string, nsID string) {
	a.RecordError(api)
}

func (a *HealthSignalAggregatorImpl) GetErrorRatio() float64 {
	return a.errorRatioTotal.Average()
}
