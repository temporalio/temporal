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
	"runtime"
	"sync/atomic"
	"time"

	"go.temporal.io/server/build"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
)

const (
	// buildInfoMetricName is the emitted build information metric's name.
	buildInfoMetricName = "build_information"

	// buildAgeMetricName is the emitted build age metric's name.
	buildAgeMetricName = "build_age"
)

// RuntimeMetricsReporter A struct containing the state of the RuntimeMetricsReporter.
type RuntimeMetricsReporter struct {
	scope          UserScope
	buildInfoScope UserScope
	reportInterval time.Duration
	started        int32
	quit           chan struct{}
	logger         log.Logger
	lastNumGC      uint32
	buildTime      time.Time
}

// NewRuntimeMetricsReporter Creates a new RuntimeMetricsReporter.
func NewRuntimeMetricsReporter(
	scope UserScope,
	reportInterval time.Duration,
	logger log.Logger,
	instanceID string,
) *RuntimeMetricsReporter {
	if len(instanceID) > 0 {
		scope = scope.Tagged(map[string]string{instance: instanceID})
	}
	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)

	return &RuntimeMetricsReporter{
		scope:          scope,
		reportInterval: reportInterval,
		logger:         logger,
		lastNumGC:      memstats.NumGC,
		quit:           make(chan struct{}),
		buildTime:      build.InfoData.BuildTime(),
		buildInfoScope: scope.Tagged(
			map[string]string{
				gitRevisionTag:   build.InfoData.GitRevision,
				buildDateTag:     build.InfoData.BuildTime().Format(time.RFC3339),
				buildPlatformTag: runtime.GOARCH,
				goVersionTag:     runtime.Version(),
				buildVersionTag:  headers.ServerVersion,
			},
		),
	}
}

// report Sends runtime metrics to the local metrics collector.
func (r *RuntimeMetricsReporter) report() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	r.scope.UpdateGauge(NumGoRoutinesGauge, float64(runtime.NumGoroutine()))
	r.scope.UpdateGauge(GoMaxProcsGauge, float64(runtime.GOMAXPROCS(0)))
	r.scope.UpdateGauge(MemoryAllocatedGauge, float64(memStats.Alloc))
	r.scope.UpdateGauge(MemoryHeapGauge, float64(memStats.HeapAlloc))
	r.scope.UpdateGauge(MemoryHeapIdleGauge, float64(memStats.HeapIdle))
	r.scope.UpdateGauge(MemoryHeapInuseGauge, float64(memStats.HeapInuse))
	r.scope.UpdateGauge(MemoryStackGauge, float64(memStats.StackInuse))

	// memStats.NumGC is a perpetually incrementing counter (unless it wraps at 2^32)
	num := memStats.NumGC
	lastNum := atomic.SwapUint32(&r.lastNumGC, num) // reset for the next iteration
	if delta := num - lastNum; delta > 0 {
		r.scope.AddCounter(NumGCCounter, int64(delta))
		if delta > 255 {
			// too many GCs happened, the timestamps buffer got wrapped around. Report only the last 256
			lastNum = num - 256
		}
		for i := lastNum; i != num; i++ {
			pause := memStats.PauseNs[i%256]
			r.scope.RecordTimer(GcPauseMsTimer, time.Duration(pause))
		}
	}

	// report build info
	r.buildInfoScope.UpdateGauge(buildInfoMetricName, 1.0)
	r.buildInfoScope.UpdateGauge(buildAgeMetricName, float64(time.Since(r.buildTime)))
}

// Start Starts the reporter thread that periodically emits metrics.
func (r *RuntimeMetricsReporter) Start() {
	if !atomic.CompareAndSwapInt32(&r.started, 0, 1) {
		return
	}
	go func() {
		ticker := time.NewTicker(r.reportInterval)
		for {
			select {
			case <-ticker.C:
				r.report()
			case <-r.quit:
				ticker.Stop()
				return
			}
		}
	}()
	r.logger.Info("RuntimeMetricsReporter started")
}

// Stop Stops reporting of runtime metrics. The reporter cannot be started again after it's been stopped.
func (r *RuntimeMetricsReporter) Stop() {
	close(r.quit)
	r.logger.Info("RuntimeMetricsReporter stopped")
}
