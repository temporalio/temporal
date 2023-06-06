package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type (
	// RegistryBuilder is a builder for a [prometheus.Registry]. You add metrics definitions to it and then call the
	// Build function to register the metrics with the Prometheus registry. All methods are safe to call concurrently.
	RegistryBuilder struct {
		sync.Mutex
		collectors []prometheus.Collector
	}
	// MetricOption is an option which modifies the definition of a metric.
	MetricOption interface {
		apply(*metricParams)
	}
	// HelpTextOption is a MetricOption which sets the help text for a metric.
	HelpTextOption string
	// metricParams is a set of parameters for a metric definition.
	metricParams struct {
		// helpText is the help text for the metric. See https://github.com/prometheus/docs/blob/main/content/docs/instrumenting/exposition_formats.md
		// for more information. See [WithHelpText] for an example.
		helpText string
	}
)

// WithHelpText sets the help text for a metric.
func WithHelpText(helpText string) HelpTextOption {
	return HelpTextOption(helpText)
}

// AddTimer adds a timer metric with the given name and options to the list of metrics to be registered later when
// Build is called.
func (r *RegistryBuilder) AddTimer(name string, opts ...MetricOption) {
	p := buildParams(opts)
	r.addCollector(prometheus.NewSummary(prometheus.SummaryOpts{
		Name: name,
		Help: p.helpText,
	}))
}

// AddHistogram adds a histogram metric with the given name and options to the list of metrics to be registered later
// when Build is called.
func (r *RegistryBuilder) AddHistogram(name string, opts ...MetricOption) {
	p := buildParams(opts)
	r.addCollector(prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: name,
		Help: p.helpText,
	}))
}

// AddCounter adds a counter metric with the given name and options to the list of metrics to be registered later when
// Build is called.
func (r *RegistryBuilder) AddCounter(name string, opts ...MetricOption) {
	p := buildParams(opts)
	r.addCollector(prometheus.NewCounter(prometheus.CounterOpts{
		Name: name,
		Help: p.helpText,
	}))
}

// AddGauge adds a gauge metric with the given name and options to the list of metrics to be registered later when
// Build is called.
func (r *RegistryBuilder) AddGauge(name string, opts ...MetricOption) {
	p := buildParams(opts)
	r.addCollector(prometheus.NewGauge(prometheus.GaugeOpts{
		Name: name,
		Help: p.helpText,
	}))
}

// Build registers the metrics added to the builder with the given Prometheus registry. It returns an error if any of
// the metrics could not be registered.
func (r *RegistryBuilder) Build(pr *prometheus.Registry) error {
	r.Lock()
	defer r.Unlock()

	for _, c := range r.collectors {
		err := pr.Register(c)
		if err != nil {
			return err
		}
	}

	return nil
}

// addCollector adds a collector to the list of metrics to be registered later when Build is called.
func (r *RegistryBuilder) addCollector(collector prometheus.Collector) {
	r.Lock()
	defer r.Unlock()
	r.collectors = append(r.collectors, collector)
}

// buildParams builds a metricParams struct from the given options.
func buildParams(opts []MetricOption) metricParams {
	var p metricParams
	for _, opt := range opts {
		opt.apply(&p)
	}

	return p
}

func (o HelpTextOption) apply(p *metricParams) {
	p.helpText = string(o)
}
