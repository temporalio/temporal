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
	controller "go.opentelemetry.io/otel/sdk/metric/controller/basic"
	"go.opentelemetry.io/otel/sdk/metric/export/aggregation"
	processor "go.opentelemetry.io/otel/sdk/metric/processor/basic"
	"go.opentelemetry.io/otel/sdk/resource"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

var _ OpenTelemetryProvider = (*openTelemetryProviderImpl)(nil)

type (
	OpenTelemetryProvider interface {
		Stop(logger log.Logger)
		GetMeter() metric.Meter
	}

	openTelemetryProviderImpl struct {
		exporter *prometheus.Exporter
		meter    metric.Meter
		config   *PrometheusConfig
		server   *http.Server
	}
)

func NewOpenTelemetryProvider(
	logger log.Logger,
	prometheusConfig *PrometheusConfig,
	clientConfig *ClientConfig,
) (*openTelemetryProviderImpl, error) {

	c := controller.New(
		processor.NewFactory(
			NewOtelAggregatorSelector(
				clientConfig.PerUnitHistogramBoundaries,
			),
			aggregation.CumulativeTemporalitySelector(),
			processor.WithMemory(true),
		),
		controller.WithResource(resource.Empty()),
	)
	exporter, err := prometheus.New(prometheus.Config{}, c)

	if err != nil {
		logger.Error("Failed to initialize prometheus exporter.", tag.Error(err))
		return nil, err
	}

	metricServer := initPrometheusListener(prometheusConfig, logger, exporter)

	meter := c.Meter("temporal")
	reporter := &openTelemetryProviderImpl{
		exporter: exporter,
		meter:    meter,
		config:   prometheusConfig,
		server:   metricServer,
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

func (r *openTelemetryProviderImpl) GetMeter() metric.Meter {
	return r.meter
}

func (r *openTelemetryProviderImpl) Stop(logger log.Logger) {
	ctx, closeCtx := context.WithTimeout(context.Background(), time.Second)
	defer closeCtx()
	if err := r.server.Shutdown(ctx); !(err == nil || err == http.ErrServerClosed) {
		logger.Error("Prometheus metrics server shutdown failure.", tag.Address(r.config.ListenAddress), tag.Error(err))
	}
}
