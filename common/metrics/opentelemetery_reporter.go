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
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/metric/prometheus"
	"go.opentelemetry.io/otel/metric"

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
	}
)

func newOpentelemeteryReporter(logger log.Logger, tags map[string]string, prefix string, prometheusConfig *PrometheusConfig) (*OpentelemetryReporter, error) {
	histogramBoundaries := prometheusConfig.DefaultHistogramBoundaries
	if len(histogramBoundaries) == 0 {
		histogramBoundaries = defaultHistogramBoundaries
	}
	exporter, err := prometheus.InstallNewPipeline(
		prometheus.Config{
			DefaultSummaryQuantiles:    defaultQuantiles,
			DefaultHistogramBoundaries: histogramBoundaries,
		},
	)

	if err != nil {
		logger.Error("Failed to initialize prometheus exporter.", tag.Error(err))
		return nil, err
	}

	initPrometheusListener(prometheusConfig, logger, exporter)

	meter := otel.Meter("temporal")
	reporter := &OpentelemetryReporter{
		exporter:  exporter,
		meter:     meter,
		meterMust: metric.Must(meter),
		tags:      tags,
		prefix:    prefix,
		config:    prometheusConfig,
	}
	return reporter, nil
}

func initPrometheusListener(config *PrometheusConfig, logger log.Logger, exporter *prometheus.Exporter) {
	handlerPath := config.HandlerPath
	if handlerPath == "" {
		handlerPath = "/metrics"
	}
	http.HandleFunc(handlerPath, exporter.ServeHTTP)
	go func() {
		logger.Info("Starting prometheus listener.", tag.Address(config.ListenAddress))
		err := http.ListenAndServe(config.ListenAddress, nil)
		if err != http.ErrServerClosed {
			logger.Fatal("Failed to initialize prometheus listener.", tag.Address(config.ListenAddress))
		}
	}()
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
