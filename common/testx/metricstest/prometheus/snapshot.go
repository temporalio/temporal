// Package prometheus collects Prometheus metric snapshots for test assertions.
package prometheus

import (
	"errors"
	"fmt"
	"maps"
	"math"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

type (
	sample struct {
		metricType  dto.MetricType
		labelValues map[string]string
		sampleValue float64
	}

	// HistogramBucket holds the cumulative observation count up to UpperBound.
	HistogramBucket struct {
		Value      float64
		UpperBound float64
	}

	histogramSample struct {
		metricType  dto.MetricType
		labelValues map[string]string
		buckets     []HistogramBucket
	}

	// Snapshot contains the last sample for each metric name.
	Snapshot struct {
		samples          map[string]sample
		histogramSamples map[string]histogramSample
	}
)

// Potential errors that the test handler can return trying to find a metric to return.
var (
	ErrMetricNotFound      = errors.New("metric not found")
	ErrMetricTypeMismatch  = errors.New("metric is not the expected type")
	ErrMetricLabelMismatch = errors.New("metric labels do not match expected labels")
)

// Gather collects a snapshot from a registry or other Prometheus gatherer.
func Gather(gatherer prometheus.Gatherer) (Snapshot, error) {
	families, err := gatherer.Gather()
	if err != nil {
		return Snapshot{}, fmt.Errorf("gathering metrics: %w", err)
	}
	samples := map[string]sample{}
	histogramSamples := map[string]histogramSample{}
	for _, family := range families {
		for _, m := range family.GetMetric() {
			collectSamples(family.GetName(), family, m, samples, histogramSamples)
		}
	}
	return Snapshot{
		samples:          samples,
		histogramSamples: histogramSamples,
	}, nil
}

func collectSamples(name string, family *dto.MetricFamily, m *dto.Metric, samples map[string]sample, histogramSamples map[string]histogramSample) {
	labelvalues := map[string]string{}
	for _, lp := range m.GetLabel() {
		labelvalues[lp.GetName()] = lp.GetValue()
	}
	// This only records the last sample if there
	// are multiple samples recorded.
	switch family.GetType() {
	case dto.MetricType_HISTOGRAM:
		buckets := m.Histogram.GetBucket()
		hbs := []HistogramBucket{}
		for _, bucket := range buckets {
			hb := HistogramBucket{
				Value:      float64(bucket.GetCumulativeCount()),
				UpperBound: bucket.GetUpperBound(),
			}
			hbs = append(hbs, hb)
		}
		// Prometheus text exposition includes an implicit +Inf bucket.
		if len(hbs) == 0 || !math.IsInf(hbs[len(hbs)-1].UpperBound, 1) {
			hbs = append(hbs, HistogramBucket{Value: float64(m.Histogram.GetSampleCount()), UpperBound: math.Inf(1)})
		}
		histogramSamples[name] = histogramSample{
			metricType:  family.GetType(),
			labelValues: labelvalues,
			buckets:     hbs,
		}
	case dto.MetricType_COUNTER:
		samples[name] = sample{
			metricType:  family.GetType(),
			labelValues: labelvalues,
			sampleValue: m.Counter.GetValue(),
		}
	case dto.MetricType_GAUGE:
		samples[name] = sample{
			metricType:  family.GetType(),
			labelValues: labelvalues,
			sampleValue: m.Gauge.GetValue(),
		}
	default:
		// Not yet supporting summary, untyped.
	}
}

func (s Snapshot) getValue(name string, metricType dto.MetricType, labelValues map[string]string) (float64, error) {
	sample, ok := s.samples[name]
	if !ok {
		return 0, fmt.Errorf("%w: %q", ErrMetricNotFound, name)
	}
	if sample.metricType != metricType {
		return 0, fmt.Errorf("%w: %q is a %s, not a %s", ErrMetricTypeMismatch, name, sample.metricType, metricType)
	}
	if !maps.Equal(sample.labelValues, labelValues) {
		return 0, fmt.Errorf("%w: %q has %v, asked for %v", ErrMetricLabelMismatch, name, sample.labelValues, labelValues)
	}
	return sample.sampleValue, nil
}

func (s Snapshot) Counter(name string, labelValues map[string]string) (float64, error) {
	return s.getValue(name, dto.MetricType_COUNTER, labelValues)
}

func (s Snapshot) Gauge(name string, labelValues map[string]string) (float64, error) {
	return s.getValue(name, dto.MetricType_GAUGE, labelValues)
}

func (s Snapshot) Histogram(name string, labelValues map[string]string) ([]HistogramBucket, error) {
	sample, ok := s.histogramSamples[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrMetricNotFound, name)
	}
	if sample.metricType != dto.MetricType_HISTOGRAM {
		return nil, fmt.Errorf("%w: %q is a %s, not a %s", ErrMetricTypeMismatch, name, sample.metricType, dto.MetricType_HISTOGRAM)
	}
	if !maps.Equal(sample.labelValues, labelValues) {
		return nil, fmt.Errorf("%w: %q has %v, asked for %v", ErrMetricLabelMismatch, name, sample.labelValues, labelValues)
	}
	return sample.buckets, nil
}

func (s Snapshot) String() string {
	var b strings.Builder
	for n, s := range s.samples {
		_, _ = fmt.Fprintf(&b, "%v %v %v %v\n", n, s.labelValues, s.sampleValue, s.metricType)
	}
	for n, s := range s.histogramSamples {
		_, _ = fmt.Fprintf(&b, "%v %v %v\n", n, s.labelValues, s.metricType)
		for _, bucket := range s.buckets {
			_, _ = fmt.Fprintf(&b, "    %v: %v \n", bucket.UpperBound, bucket.Value)
		}
	}
	return b.String()
}
