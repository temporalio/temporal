//go:build !TEMPORAL_DEBUG

package metrics

// metricDefinition contains the definition for a metric
type metricDefinition struct {
	name        string
	description string
	unit        MetricUnit
}

func newMetricDefinition(name string, opts ...Option) metricDefinition {
	d := metricDefinition{
		name:        name,
		description: "",
		unit:        "",
	}
	for _, opt := range opts {
		opt.apply(&d)
	}
	return d
}

func (md metricDefinition) Name() string {
	return md.name
}

func (md metricDefinition) Unit() MetricUnit {
	return md.unit
}
