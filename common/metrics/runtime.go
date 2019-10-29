// Copyright (c) 2017 Uber Technologies, Inc.
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
	"strconv"
	"sync/atomic"
	"time"

	"github.com/uber-go/tally"

	"github.com/uber/cadence/common/log"
)

var (
	// Revision is the VCS revision associated with this build. Overridden using ldflags
	// at compile time. Example:
	// $ go build -ldflags "-X github.com/uber/cadence/common/metrics.Revision=abcdef" ...
	// Adapted from: https://www.atatus.com/blog/golang-auto-build-versioning/
	Revision = "unknown"

	// Branch is the VCS branch associated with this build.
	Branch = "unknown"

	// Version is the version associated with this build.
	Version = "unknown"

	// BuildDate is the date this build was created.
	BuildDate = "unknown"

	// BuildTimeUnix is the seconds since epoch representing the date this build was created.
	BuildTimeUnix = "0"

	// goVersion is the current runtime version.
	goVersion = runtime.Version()
)

const (
	// buildInfoMetricName is the emitted build information metric's name.
	buildInfoMetricName = "build_information"

	// buildAgeMetricName is the emitted build age metric's name.
	buildAgeMetricName = "build_age"
)

// RuntimeMetricsReporter A struct containing the state of the RuntimeMetricsReporter.
type RuntimeMetricsReporter struct {
	scope          tally.Scope
	buildInfoScope tally.Scope
	reportInterval time.Duration
	started        int32
	quit           chan struct{}
	logger         log.Logger
	lastNumGC      uint32
	buildTime      time.Time
}

// NewRuntimeMetricsReporter Creates a new RuntimeMetricsReporter.
func NewRuntimeMetricsReporter(
	scope tally.Scope,
	reportInterval time.Duration,
	logger log.Logger,
	instanceID string,
) *RuntimeMetricsReporter {
	const (
		base    = 10
		bitSize = 64
	)
	if len(instanceID) > 0 {
		scope = scope.Tagged(map[string]string{instance: instanceID})
	}
	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)
	rReporter := &RuntimeMetricsReporter{
		scope:          scope,
		reportInterval: reportInterval,
		logger:         logger,
		lastNumGC:      memstats.NumGC,
		quit:           make(chan struct{}),
	}
	rReporter.buildInfoScope = scope.Tagged(
		map[string]string{
			revisionTag:     Revision,
			branchTag:       Branch,
			buildDateTag:    BuildDate,
			buildVersionTag: Version,
			goVersionTag:    goVersion,
		},
	)
	sec, err := strconv.ParseInt(BuildTimeUnix, base, bitSize)
	if err != nil || sec < 0 {
		sec = 0
	}
	rReporter.buildTime = time.Unix(sec, 0)
	return rReporter
}

// report Sends runtime metrics to the local metrics collector.
func (r *RuntimeMetricsReporter) report() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	r.scope.Gauge(NumGoRoutinesGauge).Update(float64(runtime.NumGoroutine()))
	r.scope.Gauge(GoMaxProcsGauge).Update(float64(runtime.GOMAXPROCS(0)))
	r.scope.Gauge(MemoryAllocatedGauge).Update(float64(memStats.Alloc))
	r.scope.Gauge(MemoryHeapGauge).Update(float64(memStats.HeapAlloc))
	r.scope.Gauge(MemoryHeapIdleGauge).Update(float64(memStats.HeapIdle))
	r.scope.Gauge(MemoryHeapInuseGauge).Update(float64(memStats.HeapInuse))
	r.scope.Gauge(MemoryStackGauge).Update(float64(memStats.StackInuse))

	// memStats.NumGC is a perpetually incrementing counter (unless it wraps at 2^32)
	num := memStats.NumGC
	lastNum := atomic.SwapUint32(&r.lastNumGC, num) // reset for the next iteration
	if delta := num - lastNum; delta > 0 {
		r.scope.Counter(NumGCCounter).Inc(int64(delta))
		if delta > 255 {
			// too many GCs happened, the timestamps buffer got wrapped around. Report only the last 256
			lastNum = num - 256
		}
		for i := lastNum; i != num; i++ {
			pause := memStats.PauseNs[i%256]
			r.scope.Timer(GcPauseMsTimer).Record(time.Duration(pause))
		}
	}

	// report build info
	buildInfoGauge := r.buildInfoScope.Gauge(buildInfoMetricName)
	buildAgeGauge := r.buildInfoScope.Gauge(buildAgeMetricName)
	buildInfoGauge.Update(1.0)
	buildAgeGauge.Update(float64(time.Since(r.buildTime)))
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
