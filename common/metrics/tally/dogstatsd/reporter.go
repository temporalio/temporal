package dogstatsd

import (
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/uber-go/tally"
	"sort"
	"time"
)

type tallyDogstatsdReporter struct {
	dogstatsd *statsd.Client
}

// NewReporter is a wrapper on top of "github.com/DataDog/datadog-go/statsd"
// The purpose is to support datadog-formatted statsd metric tagging.
func NewReporter(client *statsd.Client) tally.StatsReporter {
	return &tallyDogstatsdReporter{
		dogstatsd: client,
	}
}

func (r tallyDogstatsdReporter) Capabilities() tally.Capabilities {
	return r
}

func (r tallyDogstatsdReporter) Reporting() bool {
	return true
}

func (r tallyDogstatsdReporter) Tagging() bool {
	return true
}

func (r tallyDogstatsdReporter) Flush() {
	// no-op
}

func (r tallyDogstatsdReporter) ReportCounter(name string, tags map[string]string, value int64) {
	_ = r.dogstatsd.Count(name, value, r.marshalTags(tags), 1)
}

func (r tallyDogstatsdReporter) ReportGauge(name string, tags map[string]string, value float64) {
	_ = r.dogstatsd.Gauge(name, value, r.marshalTags(tags), 1)
}

func (r tallyDogstatsdReporter) ReportTimer(name string, tags map[string]string, interval time.Duration) {
	_ = r.dogstatsd.Timing(name, interval, r.marshalTags(tags), 1)
}

func (r tallyDogstatsdReporter) ReportHistogramValueSamples(name string, tags map[string]string, buckets tally.Buckets, bucketLowerBound, bucketUpperBound float64, samples int64) {
	// temporal's client does not expose histograms
	panic("implement me")
}

func (r tallyDogstatsdReporter) ReportHistogramDurationSamples(name string, tags map[string]string, buckets tally.Buckets, bucketLowerBound, bucketUpperBound time.Duration, samples int64) {
	// temporal's client does not expose histograms
	panic("implement me")
}

func (r tallyDogstatsdReporter) marshalTags(tags map[string]string) []string {
	var keys []string
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var dogTags []string
	for _, tk := range keys {
		dogTags = append(dogTags, fmt.Sprintf("%s:%s", tk, tags[tk]))
	}
	return dogTags
}
