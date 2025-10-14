// nolint:forbidigo,panic // panic for unused metric handler methods
package matching

import (
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type (
	priMetricHandler struct {
		handler metrics.Handler
	}
	priMetricsTimer struct {
		name    string
		handler metrics.Handler
	}
	priMetricsCounter struct {
		name    string
		handler metrics.Handler
	}
	priMetricsGauge struct {
		name    string
		handler metrics.Handler
	}
	fairMetricHandler struct {
		handler metrics.Handler
	}
	fairMetricsTimer struct {
		name    string
		handler metrics.Handler
	}
	fairMetricsCounter struct {
		name    string
		handler metrics.Handler
	}
	fairMetricsGauge struct {
		name    string
		handler metrics.Handler
	}
)

// TODO(pri): cleanup; delete this
func newPriMetricsHandler(handler metrics.Handler) priMetricHandler {
	return priMetricHandler{
		handler: handler,
	}
}

func (p priMetricHandler) Stop(logger log.Logger) {
	p.handler.Stop(logger)
}

func (p priMetricHandler) Counter(name string) metrics.CounterIface {
	return priMetricsCounter{name: name, handler: p.handler}
}
func (p priMetricHandler) Timer(name string) metrics.TimerIface {
	return priMetricsTimer{name: name, handler: p.handler}
}

func (p priMetricHandler) Gauge(name string) metrics.GaugeIface {
	return priMetricsGauge{name: name, handler: p.handler}
}

func (p priMetricHandler) WithTags(...metrics.Tag) metrics.Handler {
	panic("not implemented")
}

func (p priMetricHandler) Histogram(string, metrics.MetricUnit) metrics.HistogramIface {
	panic("not implemented")
}

func (p priMetricHandler) StartBatch(string) metrics.BatchHandler {
	panic("not implemented")
}

func (c priMetricsCounter) Record(i int64, tag ...metrics.Tag) {
	c.handler.Counter(c.name).Record(i, tag...)
	c.handler.Counter(withPriPrefix(c.name)).Record(i, tag...)
}

func (t priMetricsTimer) Record(duration time.Duration, tag ...metrics.Tag) {
	t.handler.Timer(t.name).Record(duration, tag...)
	t.handler.Timer(withPriPrefix(t.name)).Record(duration, tag...)
}

func (t priMetricsGauge) Record(v float64, tag ...metrics.Tag) {
	t.handler.Gauge(t.name).Record(v, tag...)
	t.handler.Gauge(withPriPrefix(t.name)).Record(v, tag...)
}

func withPriPrefix(name string) string {
	return "pri_" + name
}

func newFairMetricsHandler(handler metrics.Handler) fairMetricHandler {
	return fairMetricHandler{
		handler: handler,
	}
}

func (p fairMetricHandler) Stop(logger log.Logger) {
	p.handler.Stop(logger)
}

func (p fairMetricHandler) Counter(name string) metrics.CounterIface {
	return fairMetricsCounter{name: name, handler: p.handler}
}
func (p fairMetricHandler) Timer(name string) metrics.TimerIface {
	return fairMetricsTimer{name: name, handler: p.handler}
}

func (p fairMetricHandler) Gauge(name string) metrics.GaugeIface {
	return fairMetricsGauge{name: name, handler: p.handler}
}

func (p fairMetricHandler) WithTags(...metrics.Tag) metrics.Handler {
	panic("not implemented")
}

func (p fairMetricHandler) Histogram(string, metrics.MetricUnit) metrics.HistogramIface {
	panic("not implemented")
}

func (p fairMetricHandler) StartBatch(string) metrics.BatchHandler {
	panic("not implemented")
}

func (c fairMetricsCounter) Record(i int64, tag ...metrics.Tag) {
	c.handler.Counter(c.name).Record(i, tag...)
	c.handler.Counter(withFairPrefix(c.name)).Record(i, tag...)
}

func (t fairMetricsTimer) Record(duration time.Duration, tag ...metrics.Tag) {
	t.handler.Timer(t.name).Record(duration, tag...)
	t.handler.Timer(withFairPrefix(t.name)).Record(duration, tag...)
}

func (t fairMetricsGauge) Record(v float64, tag ...metrics.Tag) {
	t.handler.Gauge(t.name).Record(v, tag...)
	t.handler.Gauge(withFairPrefix(t.name)).Record(v, tag...)
}

func withFairPrefix(name string) string {
	return "fair_" + name
}
