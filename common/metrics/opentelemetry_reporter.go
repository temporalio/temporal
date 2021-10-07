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
	"context"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	export "go.opentelemetry.io/otel/sdk/export/metric"
	"go.opentelemetry.io/otel/sdk/metric/aggregator/histogram"
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	processor "go.opentelemetry.io/otel/sdk/metric/processor/basic"
	selector "go.opentelemetry.io/otel/sdk/metric/selector/simple"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type (
	// OpentelemetryReporter is a base class for reporting metrics to opentelemetry.
	OpentelemetryReporter struct {
		exporter  *prometheus.Exporter
		meter     metric.Meter
		meterMust metric.MeterMust
		tags      map[string]string
		prefix    string
		config    *PrometheusConfig
		server    *http.Server
	}

	OpentelemetryListener struct {
	}
)

func newOpentelemeteryReporter(
	logger log.Logger,
	tags map[string]string,
	prefix string,
	prometheusConfig *PrometheusConfig,
) (*OpentelemetryReporter, error) {
	histogramBoundaries := prometheusConfig.DefaultHistogramBoundaries
	if len(histogramBoundaries) == 0 {
		histogramBoundaries = defaultHistogramBoundaries
	}
	c := controller.New(
		processor.NewFactory(
			selector.NewWithHistogramDistribution(
				histogram.WithExplicitBoundaries(histogramBoundaries),
			),
			export.CumulativeExportKindSelector(),
			processor.WithMemory(true),
		),
	)
	exporter, err := prometheus.New(
		prometheus.Config{DefaultHistogramBoundaries: histogramBoundaries}, c)

	if err != nil {
		logger.Error("Failed to initialize prometheus exporter.", tag.Error(err))
		return nil, err
	}

	metricServer := initPrometheusListener(prometheusConfig, logger, exporter)

	meter := c.Meter("temporal")
	reporter := &OpentelemetryReporter{
		exporter:  exporter,
		meter:     meter,
		meterMust: metric.Must(meter),
		tags:      tags,
		prefix:    prefix,
		config:    prometheusConfig,
		server:    metricServer,
	}
	return reporter, nil
}

func initPrometheusListener(config *PrometheusConfig, logger log.Logger, exporter *prometheus.Exporter) *http.Server {
	handlerPath := config.HandlerPath
	if handlerPath == "" {
		handlerPath = "/metrics"
	}

	handler := http.NewServeMux()
	handler.HandleFunc(handlerPath, exporter.ServeHTTP)

	if config.ListenAddress == "" {
		logger.Fatal("Listen address must be specified.", tag.Address(config.ListenAddress))
	}
	server := &http.Server{Addr: config.ListenAddress, Handler: handler}

	go func() {
		err := server.ListenAndServe()
		if err != http.ErrServerClosed {
			logger.Fatal("Failed to initialize prometheus listener.", tag.Address(config.ListenAddress))
		}
	}()

	return server
}

func (r *OpentelemetryReporter) GetMeter() metric.Meter {
	return r.meter
}

func (r *OpentelemetryReporter) GetMeterMust() metric.MeterMust {
	return r.meterMust
}

func (r *OpentelemetryReporter) NewClient(logger log.Logger, serviceIdx ServiceIdx) (Client, error) {
	return newOpentelemeteryClient(r.tags, serviceIdx, r, logger)
}

func (r *OpentelemetryReporter) Stop(logger log.Logger) {
	ctx, closeCtx := context.WithTimeout(context.Background(), time.Second)
	defer closeCtx()
	if err := r.server.Shutdown(ctx); !(err == nil || err == http.ErrServerClosed) {
		logger.Error("Prometheus metrics server shutdown failure.", tag.Address(r.config.ListenAddress), tag.Error(err))
	}
}
