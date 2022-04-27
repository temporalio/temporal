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
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"

	"go.temporal.io/server/common/log"
)

var _ Reporter = (OpenTelemetryReporter)(nil)
var _ OpenTelemetryReporter = (*openTelemetryReporterImpl)(nil)

type (
	OpenTelemetryReporter interface {
		NewClient(logger log.Logger, serviceIdx ServiceIdx) (Client, error)
		Stop(logger log.Logger)
		GetMeter() metric.Meter
		UserScope() UserScope
	}

	// openTelemetryReporterImpl is a base class for reporting metrics to open telemetry.
	openTelemetryReporterImpl struct {
		exporter     *prometheus.Exporter
		meter        metric.Meter
		clientConfig *ClientConfig
		gaugeCache   OtelGaugeCache
		userScope    UserScope
		otelProvider OpenTelemetryProvider
	}

	OpenTelemetryListener struct {
	}
)

func NewOpenTelemetryReporterFromPrometheusConfig(
	logger log.Logger, // keeping this to maintain API in case of adding more logging later
	prometheusConfig *PrometheusConfig,
	clientConfig *ClientConfig,
) (*openTelemetryReporterImpl, error) {
	otelProvider, err := NewOpenTelemetryProvider(logger, prometheusConfig, clientConfig)
	if err != nil {
		return nil, err
	}
	return NewOpenTelemeteryReporter(logger, clientConfig, otelProvider)
}

func NewOpenTelemeteryReporter(
	logger log.Logger, // keeping this to maintain API in case of adding more logging later
	clientConfig *ClientConfig,
	otelProvider OpenTelemetryProvider,
) (*openTelemetryReporterImpl, error) {
	meter := otelProvider.GetMeter()
	gaugeCache := NewOtelGaugeCache(meter)
	userScope := NewOpenTelemetryUserScope(meter, clientConfig.Tags, gaugeCache)
	reporter := &openTelemetryReporterImpl{
		clientConfig: clientConfig,
		gaugeCache:   gaugeCache,
		userScope:    userScope,
		otelProvider: otelProvider,
	}

	return reporter, nil
}

func (r *openTelemetryReporterImpl) GetMeter() metric.Meter {
	return r.otelProvider.GetMeter()
}

func (r *openTelemetryReporterImpl) NewClient(logger log.Logger, serviceIdx ServiceIdx) (Client, error) {
	return NewOpenTelemetryClient(r.clientConfig, serviceIdx, r, logger, r.gaugeCache)
}

func (r *openTelemetryReporterImpl) Stop(logger log.Logger) {
	r.otelProvider.Stop(logger)
}

func (r *openTelemetryReporterImpl) UserScope() UserScope {
	return r.userScope
}
