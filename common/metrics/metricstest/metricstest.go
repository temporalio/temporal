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

package metricstest

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	exporters "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetrics "go.opentelemetry.io/otel/sdk/metric"
	"golang.org/x/exp/maps"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type (
	Handler struct {
		metrics.Handler
		reg *prometheus.Registry
	}

	sample struct {
		metricType  dto.MetricType
		labelValues map[string]string
		sampleValue float64
	}

	Snapshot struct {
		samples map[string]sample
	}
)

func MustNewHandler(logger log.Logger) *Handler {
	h, err := NewHandler(logger)
	if err != nil {
		panic(err)
	}
	return h
}

func NewHandler(logger log.Logger) (*Handler, error) {
	registry := prometheus.NewRegistry()
	exporter, err := exporters.New(exporters.WithRegisterer(registry))
	if err != nil {
		return nil, err
	}

	provider := sdkmetrics.NewMeterProvider(sdkmetrics.WithReader(exporter))
	meter := provider.Meter("temporal")
	clientConfig := metrics.ClientConfig{}
	otelHandler := metrics.NewOtelMetricsHandler(logger, &otelProvider{meter: meter}, clientConfig)
	metricsHandler := &Handler{
		Handler: otelHandler,
		reg:     registry,
	}

	return metricsHandler, nil
}

func (*Handler) Stop(log.Logger) {}

func (h *Handler) Snapshot() (Snapshot, error) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	handler := http.NewServeMux()
	handler.HandleFunc("/metrics", promhttp.HandlerFor(h.reg, promhttp.HandlerOpts{Registry: h.reg}).ServeHTTP)
	handler.ServeHTTP(rec, req)

	var tp expfmt.TextParser
	families, err := tp.TextToMetricFamilies(rec.Body)
	if err != nil {
		return Snapshot{}, err
	}
	samples := map[string]sample{}
	for name, family := range families {
		for _, m := range family.GetMetric() {
			labelvalues := map[string]string{}
			for _, lp := range m.GetLabel() {
				labelvalues[lp.GetName()] = lp.GetValue()
			}
			// This only records the last sample if there
			// are multiple samples recorded.
			switch family.GetType() {
			default:
				// Not yet supporting histogram, summary, untyped.
			case dto.MetricType_COUNTER:
				samples[name] = sample{
					metricType:  family.GetType(),
					labelValues: labelvalues,
					sampleValue: m.Counter.GetValue(),
				}
			case dto.MetricType_GAUGE:
				samples[name] = sample{
					metricType:  family.GetType(),
					labelValues: labelvalues,
					sampleValue: m.Gauge.GetValue(),
				}
			}
		}
	}
	return Snapshot{samples: samples}, nil
}

func (h *Handler) MustSnapshot() Snapshot {
	s, err := h.Snapshot()
	if err != nil {
		panic(err)
	}
	return s
}

var _ metrics.OpenTelemetryProvider = (*otelProvider)(nil)

type otelProvider struct {
	meter metric.Meter
}

func (m *otelProvider) GetMeter() metric.Meter {
	return m.meter
}

func (m *otelProvider) Stop(log.Logger) {}

func (s Snapshot) getValue(name string, metricType dto.MetricType, tags ...metrics.Tag) (float64, error) {
	labelValues := map[string]string{}
	for _, tag := range tags {
		labelValues[tag.Key()] = tag.Value()
	}
	sample, ok := s.samples[name]
	if !ok {
		return 0, fmt.Errorf("metric %s not found", name)
	}
	if sample.metricType != metricType {
		return 0, fmt.Errorf("metric %s not a %s type", name, metricType.String())
	}
	if !maps.Equal(sample.labelValues, labelValues) {
		return 0, fmt.Errorf("metric %s label mismatch, has %v, asked for %v", name, sample.labelValues, labelValues)
	}
	return sample.sampleValue, nil
}

func (s Snapshot) Counter(name string, tags ...metrics.Tag) (float64, error) {
	return s.getValue(name, dto.MetricType_COUNTER, tags...)
}

func (s Snapshot) MustCounter(name string, tags ...metrics.Tag) float64 {
	v, err := s.Counter(name, tags...)
	if err != nil {
		panic(err)
	}
	return v
}

func (s Snapshot) Gauge(name string, tags ...metrics.Tag) (float64, error) {
	return s.getValue(name, dto.MetricType_GAUGE, tags...)
}

func (s Snapshot) MustGauge(name string, tags ...metrics.Tag) float64 {
	v, err := s.Gauge(name, tags...)
	if err != nil {
		panic(err)
	}
	return v
}

func (s Snapshot) String() string {
	var b strings.Builder
	for n, s := range s.samples {
		b.WriteString(fmt.Sprintf("%v %v %v %v\n", n, s.labelValues, s.sampleValue, s.metricType))
	}
	return b.String()
}
