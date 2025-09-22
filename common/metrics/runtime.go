package metrics

import (
	"runtime"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/build"
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
	handler          Handler
	buildInfoHandler Handler
	reportInterval   time.Duration
	started          int32
	quit             chan struct{}
	logger           log.Logger
	lastNumGC        uint32
	buildTime        time.Time
}

// NewRuntimeMetricsReporter Creates a new RuntimeMetricsReporter.
func NewRuntimeMetricsReporter(
	handler Handler,
	reportInterval time.Duration,
	logger log.Logger,
	instanceID string,
) *RuntimeMetricsReporter {
	if len(instanceID) > 0 {
		handler = handler.WithTags(StringTag(instance, instanceID))
	}
	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)

	return &RuntimeMetricsReporter{
		handler:        handler,
		reportInterval: reportInterval,
		logger:         logger,
		lastNumGC:      memstats.NumGC,
		quit:           make(chan struct{}),
		buildTime:      build.InfoData.GitTime,
		buildInfoHandler: handler.WithTags(
			StringTag(gitRevisionTag, build.InfoData.GitRevision),
			StringTag(buildDateTag, build.InfoData.GitTime.Format(time.RFC3339)),
			StringTag(buildPlatformTag, build.InfoData.GoArch),
			StringTag(goVersionTag, build.InfoData.GoVersion),
			StringTag(buildVersionTag, headers.ServerVersion),
		),
	}
}

// report Sends runtime metrics to the local metrics collector.
func (r *RuntimeMetricsReporter) report() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	NumGoRoutinesGauge.With(r.handler).Record(float64(runtime.NumGoroutine()))
	GoMaxProcsGauge.With(r.handler).Record(float64(runtime.GOMAXPROCS(0)))
	MemoryAllocatedGauge.With(r.handler).Record(float64(memStats.Alloc))
	MemoryHeapGauge.With(r.handler).Record(float64(memStats.HeapAlloc))
	MemoryHeapObjectsGauge.With(r.handler).Record(float64(memStats.HeapObjects))
	MemoryHeapIdleGauge.With(r.handler).Record(float64(memStats.HeapIdle))
	MemoryHeapInuseGauge.With(r.handler).Record(float64(memStats.HeapInuse))
	MemoryHeapReleasedGauge.With(r.handler).Record(float64(memStats.HeapReleased))
	MemoryStackGauge.With(r.handler).Record(float64(memStats.StackInuse))
	MemoryMallocsGauge.With(r.handler).Record(float64(memStats.Mallocs))
	MemoryFreesGauge.With(r.handler).Record(float64(memStats.Frees))

	NumGCGauge.With(r.handler).Record(float64(memStats.NumGC))
	GcPauseNsTotal.With(r.handler).Record(float64(memStats.PauseTotalNs))

	// memStats.NumGC is a perpetually incrementing counter (unless it wraps at 2^32)
	num := memStats.NumGC
	lastNum := atomic.SwapUint32(&r.lastNumGC, num) // reset for the next iteration
	if delta := num - lastNum; delta > 0 {
		NumGCCounter.With(r.handler).Record(int64(delta))
		if delta > 255 {
			// too many GCs happened, the timestamps buffer got wrapped around. Report only the last 256
			lastNum = num - 256
		}
		for i := lastNum; i != num; i++ {
			pause := memStats.PauseNs[i%256]
			GcPauseMsTimer.With(r.handler).Record(time.Duration(pause))
		}
	}

	// report build info
	r.buildInfoHandler.Gauge(buildInfoMetricName).Record(1.0)
	r.buildInfoHandler.Gauge(buildAgeMetricName).Record(float64(time.Since(r.buildTime)))
}

// Start Starts the reporter thread that periodically emits metrics.
func (r *RuntimeMetricsReporter) Start() {
	if !atomic.CompareAndSwapInt32(&r.started, 0, 1) {
		return
	}
	r.report()
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
