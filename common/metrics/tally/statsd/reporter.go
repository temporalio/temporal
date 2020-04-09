package statsd

import (
	"bytes"
	"sort"
	"time"

	"github.com/cactus/go-statsd-client/statsd"
	"github.com/uber-go/tally"
	tallystatsdreporter "github.com/uber-go/tally/statsd"
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
