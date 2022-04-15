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

var _ Reporter = (OpentelemetryReporter)(nil)
var _ OpentelemetryReporter = (*opentelemetryReporterImpl)(nil)
var _ OpentelemetryProvider = (*opentelemetryProviderImpl)(nil)

type (
	OpentelemetryReporter interface {
		NewClient(logger log.Logger, serviceIdx ServiceIdx) (Client, error)
		Stop(logger log.Logger)
		GetMeter() metric.Meter
		UserScope() UserScope
	}

	// opentelemetryReporterImpl is a base class for reporting metrics to opentelemetry.
	opentelemetryReporterImpl struct {
		exporter     *prometheus.Exporter
		meter        metric.Meter
		clientConfig *ClientConfig
		gaugeCache   OtelGaugeCache
		userScope    UserScope
		otelProvider OpentelemetryProvider
	}

	OpentelemetryListener struct {
	}
)

func NewOpentelemeteryReporterFromPrometheusConfig(
	logger log.Logger, // keeping this to maintain API in case of adding more logging later
	prometheusConfig *PrometheusConfig,
	clientConfig *ClientConfig,
) (*opentelemetryReporterImpl, error) {
	otelProvider, err := NewOpentelemetryProvider(logger, prometheusConfig, clientConfig)
	if err != nil {
		return nil, err
	}
	return NewOpentelemeteryReporter(logger, clientConfig, otelProvider)
}

func NewOpentelemeteryReporter(
	logger log.Logger, // keeping this to maintain API in case of adding more logging later
	clientConfig *ClientConfig,
	otelProvider OpentelemetryProvider,
) (*opentelemetryReporterImpl, error) {
	meter := otelProvider.GetMeter()
	gaugeCache := NewOtelGaugeCache(meter)
	userScope := NewOpentelemetryUserScope(meter, clientConfig.Tags, gaugeCache)
	reporter := &opentelemetryReporterImpl{
		clientConfig: clientConfig,
		gaugeCache:   gaugeCache,
		userScope:    userScope,
		otelProvider: otelProvider,
	}

	return reporter, nil
}

func (r *opentelemetryReporterImpl) GetMeter() metric.Meter {
	return r.otelProvider.GetMeter()
}

func (r *opentelemetryReporterImpl) NewClient(logger log.Logger, serviceIdx ServiceIdx) (Client, error) {
	return NewOpentelemeteryClient(r.clientConfig, serviceIdx, r, logger, r.gaugeCache)
}

func (r *opentelemetryReporterImpl) Stop(logger log.Logger) {
	r.otelProvider.Stop(logger)
}

func (r *opentelemetryReporterImpl) UserScope() UserScope {
	return r.userScope
}
