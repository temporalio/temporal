package metrics

// Option is used to configure a metric definition. Note that options are currently only supported when using the
// Prometheus reporter with the OpenTelemetry framework.
type Option interface {
	apply(m *metricDefinition)
}

// WithDescription sets the description, or "help text", of a metric. See [ServiceRequests] for an example.
type WithDescription string

func (h WithDescription) apply(m *metricDefinition) {
	m.description = string(h)
}

// WithUnit sets the unit of a metric. See NewBytesHistogramDef for an example.
type WithUnit MetricUnit

func (h WithUnit) apply(m *metricDefinition) {
	m.unit = MetricUnit(h)
}
