package sdk

import (
	"time"

	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/metrics"
)

type (
	MetricsHandler struct {
		provider metrics.Handler
	}

	metricsCounter struct {
		name     string
		provider metrics.Handler
	}

	metricsGauge struct {
		name     string
		provider metrics.Handler
	}

	metricsTimer struct {
		name     string
		provider metrics.Handler
	}
)

var _ sdkclient.MetricsHandler = &MetricsHandler{}

func NewMetricsHandler(provider metrics.Handler) *MetricsHandler {
	return &MetricsHandler{provider: provider}
}

func (m *MetricsHandler) WithTags(tags map[string]string) sdkclient.MetricsHandler {
	t := make([]metrics.Tag, 0, len(tags))
	for k, v := range tags {
		t = append(t, metrics.StringTag(k, v))
	}

	return NewMetricsHandler(m.provider.WithTags(t...))
}

func (m *MetricsHandler) Counter(name string) sdkclient.MetricsCounter {
	return &metricsCounter{name: name, provider: m.provider}
}

func (m *MetricsHandler) Gauge(name string) sdkclient.MetricsGauge {
	return &metricsGauge{name: name, provider: m.provider}
}

func (m *MetricsHandler) Timer(name string) sdkclient.MetricsTimer {
	return &metricsTimer{name: name, provider: m.provider}
}

func (m metricsCounter) Inc(i int64) {
	m.provider.Counter(m.name).Record(i)
}

func (m metricsGauge) Update(f float64) {
	m.provider.Gauge(m.name).Record(f)
}

func (m metricsTimer) Record(duration time.Duration) {
	m.provider.Timer(m.name).Record(duration)
}
