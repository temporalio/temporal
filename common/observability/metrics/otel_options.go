// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
	counterOptions   []metric.Int64CounterOption
	gaugeOptions     []metric.Float64ObservableGaugeOption
	histogramOptions []metric.Int64HistogramOption
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

func (opts histogramOptions) addOption(option metric.InstrumentOption) histogramOptions {
	return append(opts, option)
}
