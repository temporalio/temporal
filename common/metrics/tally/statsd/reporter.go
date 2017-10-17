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

package statsd

import (
	"bytes"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/uber-go/tally"
	tallystatsdreporter "github.com/uber-go/tally/statsd"
	"sort"
	"time"
)

type cadenceTallyStatsdReporter struct {
	//Wrapper on top of "github.com/uber-go/tally/statsd"
	tallystatsd tally.StatsReporter
}

func (r *cadenceTallyStatsdReporter) metricNameWithTags(original_name string, tags map[string]string) string {
	var keys []string
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buffer bytes.Buffer
	buffer.WriteString(original_name)

	for _, tk := range keys {
		// adding "." as delimiter so that it will show as different parts in Graphite/Grafana
		buffer.WriteString("." + tk + "." + tags[tk])
	}

	return buffer.String()
}

// This is a wrapper on top of "github.com/uber-go/tally/statsd"
// The purpose is to support tagging
// The implementation is to append tags as metric name suffixes
func NewReporter(statsd statsd.Statter, opts tallystatsdreporter.Options) tally.StatsReporter {
	return &cadenceTallyStatsdReporter{
		tallystatsd: tallystatsdreporter.NewReporter(statsd, opts),
	}
}

func (r *cadenceTallyStatsdReporter) ReportCounter(name string, tags map[string]string, value int64) {
	new_name := r.metricNameWithTags(name, tags)
	r.tallystatsd.ReportCounter(new_name, map[string]string{}, value)
}

func (r *cadenceTallyStatsdReporter) ReportGauge(name string, tags map[string]string, value float64) {
	new_name := r.metricNameWithTags(name, tags)
	r.tallystatsd.ReportGauge(new_name, map[string]string{}, value)
}

func (r *cadenceTallyStatsdReporter) ReportTimer(name string, tags map[string]string, interval time.Duration) {
	new_name := r.metricNameWithTags(name, tags)
	r.tallystatsd.ReportTimer(new_name, map[string]string{}, interval)
}

func (r *cadenceTallyStatsdReporter) ReportHistogramValueSamples(
	name string,
	tags map[string]string,
	buckets tally.Buckets,
	bucketLowerBound,
	bucketUpperBound float64,
	samples int64,
) {
	new_name := r.metricNameWithTags(name, tags)
	r.tallystatsd.ReportHistogramValueSamples(new_name, map[string]string{}, buckets, bucketLowerBound, bucketUpperBound, samples)
}

func (r *cadenceTallyStatsdReporter) ReportHistogramDurationSamples(
	name string,
	tags map[string]string,
	buckets tally.Buckets,
	bucketLowerBound,
	bucketUpperBound time.Duration,
	samples int64,
) {
	new_name := r.metricNameWithTags(name, tags)
	r.tallystatsd.ReportHistogramDurationSamples(new_name, map[string]string{}, buckets, bucketLowerBound, bucketUpperBound, samples)
}

func (r *cadenceTallyStatsdReporter) Capabilities() tally.Capabilities {
	return r.tallystatsd.Capabilities()
}

func (r *cadenceTallyStatsdReporter) Flush() {
	r.tallystatsd.Flush()
}
