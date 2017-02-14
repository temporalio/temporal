package metrics

import (
	"runtime"
	"sync/atomic"
	"time"

	"github.com/uber-common/bark"
	"github.com/uber-go/tally"
)

// RuntimeMetricsReporter A struct containing the state of the RuntimeMetricsReporter.
type RuntimeMetricsReporter struct {
	scope          tally.Scope
	reportInterval time.Duration
	started        int32
	quit           chan struct{}
	logger         bark.Logger
	lastNumGC      uint32
}

// NewRuntimeMetricsReporter Creates a new RuntimeMetricsReporter.
func NewRuntimeMetricsReporter(scope tally.Scope, reportInterval time.Duration, logger bark.Logger) *RuntimeMetricsReporter {
	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)
	rReporter := &RuntimeMetricsReporter{
		scope:          newScope(scope, GoRuntimeMetrics),
		reportInterval: reportInterval,
		logger:         logger,
		lastNumGC:      memstats.NumGC,
		quit:           make(chan struct{}),
	}
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
