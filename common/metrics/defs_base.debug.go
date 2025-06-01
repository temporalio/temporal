//go:build TEMPORAL_DEBUG

package metrics

import (
	"runtime"
)

// metricDefinition contains the definition for a metric
type metricDefinition struct {
	name        string
	description string
	unit        MetricUnit
	file        string
	line        int
}

func newMetricDefinition(name string, opts ...Option) metricDefinition {
	d := metricDefinition{
		name:        name,
		description: "",
		unit:        "",
	}
	_, file, line, ok := runtime.Caller(2)
	if !ok {
		panic("failed to get caller info for metric definition")
	}
	d.file = file
	d.line = line
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

func (md metricDefinition) File() string {
	return md.file
}

func (md metricDefinition) Line() int {
	return md.line
}

// GetRegisteredMetricsDefinitions returns all registered metrics from the global registry.
func GetRegisteredMetricsDefinitions() []metricDefinition {
	return globalRegistry.definitions
}
