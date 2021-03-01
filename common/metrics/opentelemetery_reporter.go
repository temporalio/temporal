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
	"errors"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/metric/prometheus"
	"go.opentelemetry.io/otel/metric"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

const (
	ms = float64(time.Millisecond) / float64(time.Second)
)

type (
	OpenTelemetryReporter struct {
		exporter  *prometheus.Exporter
		meter     metric.Meter
		meterMust metric.MeterMust
	}
)

var (
	//todomigryz: unify with config/metrics.go. Imho better just use default values starting from 1/1000000
	defaultHistogramBuckets = []float64{
		1 * ms,
		2 * ms,
		5 * ms,
		10 * ms,
		20 * ms,
		50 * ms,
		100 * ms,
		200 * ms,
		500 * ms,
		1000 * ms,
		2000 * ms,
		5000 * ms,
		10000 * ms,
		20000 * ms,
		50000 * ms,
		100000 * ms,
		200000 * ms,
		500000 * ms,
		1000000 * ms,
	}
)

var prometheusListeners map[string]bool
var prometheusListenersLock = sync.Mutex{}

func NewOpentelemeteryReporter(cfg *Metrics, logger log.Logger) (*OpenTelemetryReporter, error) {
	if cfg.OTPrometheus == nil {
		return nil, errors.New("Metrics config misses Opentelemetery configuration")
	}

	histogramBuckets := cfg.OTPrometheus.DefaultHistogramBoundaries
	if len(histogramBuckets) == 0 {
		histogramBuckets = defaultHistogramBuckets
	}
	exporter, err := prometheus.InstallNewPipeline(
		prometheus.Config{
			DefaultSummaryQuantiles:    []float64{50, 75, 90, 95, 99},
			DefaultHistogramBoundaries: cfg.OTPrometheus.DefaultHistogramBoundaries,
		},
	)

	if err != nil {
		logger.Error("Failed to initialize prometheus exporter.", tag.Error(err))
		return nil, err
	}

	initPrometheusListener(cfg, logger, exporter)

	meter := otel.Meter("temporal")
	reporter := &OpenTelemetryReporter{
		exporter:  exporter,
		meter:     meter,
		meterMust: metric.Must(meter),
	}
	return reporter, nil
}

func shouldStartListener(addr string) bool {
	prometheusListenersLock.Lock()
	defer prometheusListenersLock.Unlock()

	if prometheusListeners == nil {
		prometheusListeners = make(map[string]bool)
	}
	if _, ok := prometheusListeners[addr]; ok {
		return false
	}

	prometheusListeners[addr] = true
	return true
}

func initPrometheusListener(cfg *Metrics, logger log.Logger, exporter *prometheus.Exporter) {
	if !shouldStartListener(cfg.OTPrometheus.ListenAddress) {
		return
	}

	http.HandleFunc("/metrics", exporter.ServeHTTP)
	go func() {
		logger.Info("Starting prometheus listener.", tag.Address(cfg.OTPrometheus.ListenAddress))
		err := http.ListenAndServe(cfg.OTPrometheus.ListenAddress, nil)
		if err != http.ErrServerClosed {
			logger.Fatal("Failed to initialize prometheus listener.", tag.Address(cfg.OTPrometheus.ListenAddress))
		}
	}()
}

func (r *OpenTelemetryReporter) GetMeter() metric.Meter {
	return r.meter
}

func (r *OpenTelemetryReporter) GetMeterMust() metric.MeterMust {
	return r.meterMust
}
