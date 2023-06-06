package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// globalRegistryBuilder is the default Prometheus registry builder used by the New*Def functions.
// It is also used by [BuildPrometheusRegistry].
// We need to use a global registry because the New*Def functions are called from multiple sources, and introducing
// a required registry object for all metrics definitions would be a breaking change.
//
// tl;dr we need this object to add things like this to our /metrics page for Prometheus to scrape:
// # HELP temporal_service_requests some help text
//
// The New*Def functions here have a few caveats.
// 1. The definitions aren't exposed anywhere until [BuildPrometheusRegistry] is called.
// 2. Unlike the help text of metrics, metric units aren't actually exported until metrics are emitted. In addition,
// this must be done at each call-site by adding [MetricDefinition.GetMetricUnit] to a tag that the [Handler] supports.
var globalRegistryBuilder = &RegistryBuilder{}

func NewTimerDef(name string, opts ...MetricOption) MetricDefinition {
	globalRegistryBuilder.AddTimer(name, opts...)
	return MetricDefinition{metricName: MetricName(name), unit: Milliseconds}
}

func NewBytesHistogramDef(name string, opts ...MetricOption) MetricDefinition {
	globalRegistryBuilder.AddHistogram(name, opts...)
	return MetricDefinition{metricName: MetricName(name), unit: Bytes}
}

func NewDimensionlessHistogramDef(name string, opts ...MetricOption) MetricDefinition {
	globalRegistryBuilder.AddHistogram(name, opts...)
	return MetricDefinition{metricName: MetricName(name), unit: Dimensionless}
}

func NewCounterDef(name string, opts ...MetricOption) MetricDefinition {
	globalRegistryBuilder.AddCounter(name, opts...)
	return MetricDefinition{metricName: MetricName(name)}
}

func NewGaugeDef(name string, opts ...MetricOption) MetricDefinition {
	globalRegistryBuilder.AddGauge(name, opts...)
	return MetricDefinition{metricName: MetricName(name)}
}

// BuildPrometheusRegistry returns a prometheus registry with collectors registered for each metric in the global registry.
// Metrics are added to the global registry when one of the above New*Def functions is called.
func BuildPrometheusRegistry() (*prometheus.Registry, error) {
	r := prometheus.NewRegistry()

	err := globalRegistryBuilder.Build(r)
	if err != nil {
		return nil, err
	}

	return r, nil
}
