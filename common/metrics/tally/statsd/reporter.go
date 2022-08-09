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
	"sort"
	"strings"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/uber-go/tally/v4"
	tallystatsdreporter "github.com/uber-go/tally/v4/statsd"
)

type temporalTallyStatsdReporter struct {
	//Wrapper on top of "github.com/uber-go/tally/statsd"
	tallystatsd tally.StatsReporter

	tagSeparator string
}

// Options allows configuration of Temporal-specific statsd reporter options in addition to Tally's statsd reporter options.
type Options struct {
	TallyOptions tallystatsdreporter.Options

	TagSeparator string
}

func (r *temporalTallyStatsdReporter) metricNameWithTags(originalName string, tags map[string]string) string {
	if r.tagSeparator != "" {
		return appendSeparatedTags(originalName, r.tagSeparator, tags)
	}
	return embedTags(originalName, tags)
}

// NewReporter is a wrapper on top of "github.com/uber-go/tally/statsd"
// The purpose is to support tagging.
// The implementation will append tags as metric name suffixes by default or with a separator if one is specified.
func NewReporter(statsd statsd.Statter, opts Options) tally.StatsReporter {
	return &temporalTallyStatsdReporter{
		tallystatsd:  tallystatsdreporter.NewReporter(statsd, opts.TallyOptions),
		tagSeparator: opts.TagSeparator,
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

// embedTags adds the sorted list of tags directly in the stat name.
// For example, if the stat is `hello.world` and the tags are `{universe: milkyWay, planet: earth}`,
// the stat will be emitted as `hello.world.planet.earth.universe.milkyWay`.
func embedTags(name string, tags map[string]string) string {
	// Sort tags so they are in a consistent order when emitted.
	var keys []string
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var buffer strings.Builder
	buffer.WriteString(name)
	for _, tk := range keys {
		// adding "." as delimiter so that it will show as different parts in Graphite/Grafana
		buffer.WriteString("." + tk + "." + tags[tk])
	}

	return buffer.String()
}

// appendSeparatedTags adds the sorted list of tags using the DogStatsd/InfluxDB supported tagging protocol.
// For example, if the stat is `hello.world` and the tags are `{universe: milkyWay, planet: earth}` and the separator is `,`,
// the stat will be emitted as `hello.world,planet=earth,universe=milkyWay`.
//
// For more details on the protocol see:
// - Datadog: https://docs.datadoghq.com/developers/dogstatsd/datagram_shell
// - InfluxDB: https://github.com/influxdata/telegraf/blob/ce9411343076b56dabd77fc8845cc58872d4b2e6/plugins/inputs/statsd/README.md#influx-statsd
func appendSeparatedTags(name string, separator string, tags map[string]string) string {
	var buffer strings.Builder
	buffer.WriteString(name)
	for k, v := range tags {
		buffer.WriteString(separator + k + "=" + v)
	}
	return buffer.String()
}
