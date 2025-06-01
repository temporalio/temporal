package metrics

// types used/defined by the package
type (
	histogramDefinition struct {
		metricDefinition
	}

	counterDefinition struct {
		metricDefinition
	}

	gaugeDefinition struct {
		metricDefinition
	}

	timerDefinition struct {
		metricDefinition
	}
)

func NewTimerDef(name string, opts ...Option) timerDefinition {
	// This line cannot be combined with others!
	// This ensures the stack trace has information of the caller.
	def := newMetricDefinition(name, opts...)
	globalRegistry.register(def)
	return timerDefinition{def}
}

func NewBytesHistogramDef(name string, opts ...Option) histogramDefinition {
	// This line cannot be combined with others!
	// This ensures the stack trace has information of the caller.
	def := newMetricDefinition(name, append(opts, WithUnit(Bytes))...)
	globalRegistry.register(def)
	return histogramDefinition{def}
}

func NewDimensionlessHistogramDef(name string, opts ...Option) histogramDefinition {
	// This line cannot be combined with others!
	// This ensures the stack trace has information of the caller.
	def := newMetricDefinition(name, append(opts, WithUnit(Dimensionless))...)
	globalRegistry.register(def)
	return histogramDefinition{def}
}

func NewCounterDef(name string, opts ...Option) counterDefinition {
	// This line cannot be combined with others!
	// This ensures the stack trace has information of the caller.
	def := newMetricDefinition(name, opts...)
	globalRegistry.register(def)
	return counterDefinition{def}
}

func NewGaugeDef(name string, opts ...Option) gaugeDefinition {
	// This line cannot be combined with others!
	// This ensures the stack trace has information of the caller.
	def := newMetricDefinition(name, opts...)
	globalRegistry.register(def)
	return gaugeDefinition{def}
}

func (d histogramDefinition) With(handler Handler) HistogramIface {
	return handler.Histogram(d.name, d.unit)
}

func (d counterDefinition) With(handler Handler) CounterIface {
	return handler.Counter(d.name)
}

func (d gaugeDefinition) With(handler Handler) GaugeIface {
	return handler.Gauge(d.name)
}

func (d timerDefinition) With(handler Handler) TimerIface {
	return handler.Timer(d.name)
}
