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

package statsd

import (
	"bytes"
	"sort"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/uber-go/tally/v4"
	tallystatsdreporter "github.com/uber-go/tally/v4/statsd"
)

type temporalTallyStatsdReporter struct {
	//Wrapper on top of "github.com/uber-go/tally/statsd"
	tallystatsd tally.StatsReporter
}

func (r *temporalTallyStatsdReporter) metricNameWithTags(originalName string, tags map[string]string) string {
	var keys []string
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buffer bytes.Buffer
	buffer.WriteString(originalName)

	for _, tk := range keys {
		// adding "." as delimiter so that it will show as different parts in Graphite/Grafana
		buffer.WriteString("." + tk + "." + tags[tk])
	}

	return buffer.String()
}

// NewReporter is a wrapper on top of "github.com/uber-go/tally/statsd"
// The purpose is to support tagging
// The implementation is to append tags as metric name suffixes
func NewReporter(statsd statsd.Statter, opts tallystatsdreporter.Options) tally.StatsReporter {
	return &temporalTallyStatsdReporter{
		tallystatsd: tallystatsdreporter.NewReporter(statsd, opts),
	}
}

func (r *temporalTallyStatsdReporter) ReportCounter(name string, tags map[string]string, value int64) {
	newName := r.metricNameWithTags(name, tags)
	r.tallystatsd.ReportCounter(newName, map[string]string{}, value)
}

func (r *temporalTallyStatsdReporter) ReportGauge(name string, tags map[string]string, value float64) {
	newName := r.metricNameWithTags(name, tags)
	r.tallystatsd.ReportGauge(newName, map[string]string{}, value)
}

func (r *temporalTallyStatsdReporter) ReportTimer(name string, tags map[string]string, interval time.Duration) {
	newName := r.metricNameWithTags(name, tags)
	r.tallystatsd.ReportTimer(newName, map[string]string{}, interval)
}

func (r *temporalTallyStatsdReporter) ReportHistogramValueSamples(
	name string,
	tags map[string]string,
	buckets tally.Buckets,
	bucketLowerBound,
	bucketUpperBound float64,
	samples int64,
) {
	newName := r.metricNameWithTags(name, tags)
	r.tallystatsd.ReportHistogramValueSamples(newName, map[string]string{}, buckets, bucketLowerBound, bucketUpperBound, samples)
}

func (r *temporalTallyStatsdReporter) ReportHistogramDurationSamples(
	name string,
	tags map[string]string,
	buckets tally.Buckets,
	bucketLowerBound,
	bucketUpperBound time.Duration,
	samples int64,
) {
	newName := r.metricNameWithTags(name, tags)
	r.tallystatsd.ReportHistogramDurationSamples(newName, map[string]string{}, buckets, bucketLowerBound, bucketUpperBound, samples)
}

func (r *temporalTallyStatsdReporter) Capabilities() tally.Capabilities {
	return r.tallystatsd.Capabilities()
}

func (r *temporalTallyStatsdReporter) Flush() {
	r.tallystatsd.Flush()
}
