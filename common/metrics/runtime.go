package metrics

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
)

// RuntimeMetricsReporter A struct containing the state of the RuntimeMetricsReporter.
type RuntimeMetricsReporter struct {
	reporter       Reporter
	reportInterval time.Duration
	started        int32
	quit           chan struct{}
	logger         bark.Logger
	lastNumGC      uint32
}

// NewRuntimeMetricsReporter Creates a new RuntimeMetricsReporter.
func NewRuntimeMetricsReporter(reporter Reporter, reportInterval time.Duration, logger bark.Logger) *RuntimeMetricsReporter {
	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)
	rReporter := &RuntimeMetricsReporter{
		reporter:       reporter,
		reportInterval: reportInterval,
		logger:         logger,
		lastNumGC:      memstats.NumGC,
		quit:           make(chan struct{}),
	}
	rReporter.reporter.InitMetrics(GoRuntimeMetrics)
	return rReporter
}

// report Sends runtime metrics to the local metrics collector.
func (r *RuntimeMetricsReporter) report() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	r.reporter.UpdateGauge(NumGoRoutinesGauge, nil, int64(runtime.NumGoroutine()))
	r.reporter.UpdateGauge(GoMaxProcsGauge, nil, int64(runtime.GOMAXPROCS(0)))
	r.reporter.UpdateGauge(MemoryAllocatedGauge, nil, int64(memStats.Alloc))
	r.reporter.UpdateGauge(MemoryHeapGauge, nil, int64(memStats.HeapAlloc))
	r.reporter.UpdateGauge(MemoryHeapIdleGauge, nil, int64(memStats.HeapIdle))
	r.reporter.UpdateGauge(MemoryHeapInuseGauge, nil, int64(memStats.HeapInuse))
	r.reporter.UpdateGauge(MemoryStackGauge, nil, int64(memStats.StackInuse))

	// memStats.NumGC is a perpetually incrementing counter (unless it wraps at 2^32)
	num := memStats.NumGC
	lastNum := atomic.SwapUint32(&r.lastNumGC, num) // reset for the next iteration
	if delta := num - lastNum; delta > 0 {
		r.reporter.IncCounter(NumGCCounter, nil, int64(delta))
		if delta > 255 {
			// too many GCs happened, the timestamps buffer got wrapped around. Report only the last 256
			lastNum = num - 256
		}
		for i := lastNum; i != num; i++ {
			pause := memStats.PauseNs[i%256]
			r.reporter.RecordTimer(GcPauseMsTimer, nil, time.Duration(pause))
		}
	}
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
