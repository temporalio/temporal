package metrics

import (
	"go.opentelemetry.io/otel/metric"
)

type (
	// optionSet represents a slice of metric options. We need it to be able to add options of the
	// [metric.InstrumentOption] type to slices which may be of any other type implemented by metric.InstrumentOption.
	// Normally, you could do something like `T metric.InstrumentOption` here, but the type dependency here is reversed.
	// We need a generic type T that is implemented *by* metric.InstrumentOption, not the other way around.
	// This is the only solution which avoids duplicating all the logic of the addOptions function without relying on
	// reflection, an error-prone type assertion, or a type switch with a runtime error for unhandled cases.
	optionSet[T any] interface {
		addOption(option metric.InstrumentOption) T
	}
	counterOptions          []metric.Int64CounterOption
	gaugeOptions            []metric.Float64ObservableGaugeOption
	float64HistogramOptions []metric.Float64HistogramOption
	int64HistogramOptions   []metric.Int64HistogramOption
)

func addOptions[T optionSet[T]](omp *otelMetricsHandler, opts T, metricName string) T {
	metricDef, ok := omp.catalog.getMetric(metricName)
	if !ok {
		return opts
	}

	if description := metricDef.description; description != "" {
		opts = opts.addOption(metric.WithDescription(description))
	}

	if unit := metricDef.unit; unit != "" {
		opts = opts.addOption(metric.WithUnit(string(unit)))
	}

	return opts
}

func (opts counterOptions) addOption(option metric.InstrumentOption) counterOptions {
	return append(opts, option)
}

func (opts gaugeOptions) addOption(option metric.InstrumentOption) gaugeOptions {
	return append(opts, option)
}

func (opts float64HistogramOptions) addOption(option metric.InstrumentOption) float64HistogramOptions {
	return append(opts, option)
}

func (opts int64HistogramOptions) addOption(option metric.InstrumentOption) int64HistogramOptions {
	return append(opts, option)
}
