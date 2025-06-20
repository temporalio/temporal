package metrics

import (
	"time"

	"go.temporal.io/server/common/log"
)

var (
	NoopMetricsHandler Handler = newNoopMetricsHandler()
)

type (
	noopMetricsHandler struct{}
)

func newNoopMetricsHandler() *noopMetricsHandler { return &noopMetricsHandler{} }

// WithTags creates a new MetricProvder with provided []Tag
// Tags are merged with registered Tags from the source MetricsHandler
func (n *noopMetricsHandler) WithTags(...Tag) Handler {
	return n
}

// Counter obtains a counter for the given name.
func (*noopMetricsHandler) Counter(string) CounterIface {
	return NoopCounterMetricFunc
}

// Gauge obtains a gauge for the given name.
func (*noopMetricsHandler) Gauge(string) GaugeIface {
	return NoopGaugeMetricFunc
}

// Timer obtains a timer for the given name.
func (*noopMetricsHandler) Timer(string) TimerIface {
	return NoopTimerMetricFunc
}

// Histogram obtains a histogram for the given name.
func (*noopMetricsHandler) Histogram(string, MetricUnit) HistogramIface {
	return NoopHistogramMetricFunc
}

func (*noopMetricsHandler) Stop(log.Logger) {}

func (*noopMetricsHandler) Close() error {
	return nil
}

func (n *noopMetricsHandler) StartBatch(_ string) BatchHandler {
	return n
}

var NoopCounterMetricFunc = CounterFunc(func(i int64, t ...Tag) {})
var NoopGaugeMetricFunc = GaugeFunc(func(f float64, t ...Tag) {})
var NoopTimerMetricFunc = TimerFunc(func(d time.Duration, t ...Tag) {})
var NoopHistogramMetricFunc = HistogramFunc(func(i int64, t ...Tag) {})
