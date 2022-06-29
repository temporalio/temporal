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

var defaultConfig = ClientConfig{
	Tags: nil,
	ExcludeTags: map[string][]string{
		"taskqueue":    {"__sticky__"},
		"activityType": {},
		"workflowType": {},
	},
	Prefix:                     "",
	PerUnitHistogramBoundaries: map[string][]float64{Dimensionless: {0, 10, 100}, Bytes: {1024, 2048}},
}

func BenchmarkAllTheMetricsUserScope(b *testing.B) {
	var emp MetricsHandler = NoopMetricsHandler.WithTags(OperationTag("everything-is-awesome-3"))
	var us UserScope = newUserScope(emp, defaultConfig.Tags)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(
		func(p *testing.PB) {
			for p.Next() {
				stp := us.StartTimer("stopwatch-1")
				us.AddCounter("counter-1", 1)
				us.IncCounter("counter-2")
				us.RecordDistribution("dist-1", Bytes, 1024)
				us.RecordTimer("timer-1", time.Hour*100)
				us.UpdateGauge("gauge-1", 120.435)
				stp.Stop()
				stp = us.StartTimer("stopwatch-1")
				us.AddCounter("counter-1", 1)
				us.IncCounter("counter-2")
				us.RecordDistribution("dist-1", Bytes, 1024)
				us.RecordTimer("timer-1", time.Hour*100)
				us.UpdateGauge("gauge-1", 120.435)
				stp.Stop()
				stp = us.StartTimer("stopwatch-1")
				us.AddCounter("counter-1", 1)
				us.IncCounter("counter-2")
				us.RecordDistribution("dist-1", Bytes, 1024)
				us.RecordTimer("timer-1", time.Hour*100)
				us.UpdateGauge("gauge-1", 120.435)
				stp.Stop()
			}
		},
	)
}
