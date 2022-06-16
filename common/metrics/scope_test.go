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
	"testing"
	"time"
)

func BenchmarkAllTheMetricsScope(b *testing.B) {
	var emp MetricsHandler = NoopMetricsHandler.WithTags(OperationTag("everything-is-awesome-3"))
	var us Scope = newScope(emp, UnitTestService, TestScope1).Tagged(ServiceRoleTag("testing"), ServiceTypeTag("test"))

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(
		func(p *testing.PB) {
			for p.Next() {
				stp := us.StartTimer(TestTimerMetric2)
				us.AddCounter(TestCounterMetric1, 1)
				us.IncCounter(TestCounterMetric2)
				us.IncCounter(ServiceRequests)
				us.RecordDistribution(TestBytesHistogramMetric1, 1024)
				us.RecordTimer(TestTimerMetric1, time.Hour*100)
				us.UpdateGauge(TestGaugeMetric1, 120.435)
				stp.Stop()
				stp = us.StartTimer(TestTimerMetric2)
				us.AddCounter(TestCounterMetric1, 1)
				us.IncCounter(TestCounterMetric2)
				us.IncCounter(ServiceRequests)
				us.RecordDistribution(TestBytesHistogramMetric1, 1024)
				us.RecordTimer(TestTimerMetric1, time.Hour*100)
				us.UpdateGauge(TestGaugeMetric1, 120.435)
				stp.Stop()
				stp = us.StartTimer(TestTimerMetric2)
				us.AddCounter(TestCounterMetric1, 1)
				us.IncCounter(TestCounterMetric2)
				us.IncCounter(ServiceRequests)
				us.RecordDistribution(TestBytesHistogramMetric1, 1024)
				us.RecordTimer(TestTimerMetric1, time.Hour*100)
				us.UpdateGauge(TestGaugeMetric1, 120.435)
				stp.Stop()
			}
		},
	)
}
