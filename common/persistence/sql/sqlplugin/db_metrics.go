package sqlplugin

import (
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type DBMetricsReporter struct {
	interval time.Duration
	handle   *DatabaseHandle
	metrics  metrics.Handler
	quit     chan struct{}
	wg       sync.WaitGroup

	started int32
	stopped int32

	logger log.Logger
}

func newDBMetricReporter(dbKind DbKind, handle *DatabaseHandle) *DBMetricsReporter {
	reporter := &DBMetricsReporter{
		interval: time.Minute,
		handle:   handle,
		metrics:  handle.metrics.WithTags(metrics.PersistenceDBKindTag(dbKind.String())),
		quit:     make(chan struct{}),
		logger:   handle.logger,
	}
	return reporter
}

// Start run metrics report in background
// yield control immediately without blocking
// safe to called multiple time
func (r *DBMetricsReporter) Start() {
	if !atomic.CompareAndSwapInt32(&r.started, 0, 1) {
		return
	}
	r.wg.Add(1)
	go r.run()
}

func (r *DBMetricsReporter) run() {
	defer r.wg.Done()
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-r.quit:
			return
		case <-ticker.C:
			if atomic.LoadInt32(&r.stopped) == 1 {
				return
			}
			r.report()
		}
	}
}

func (r *DBMetricsReporter) report() {
	db := r.handle.db.Load()
	if db == nil {
		return
	}
	s := db.Stats()
	metrics.PersistenceSQLMaxOpenConn.With(r.metrics).Record(float64(s.MaxOpenConnections))
	metrics.PersistenceSQLOpenConn.With(r.metrics).Record(float64(s.OpenConnections))
	metrics.PersistenceSQLIdleConn.With(r.metrics).Record(float64(s.Idle))
	metrics.PersistenceSQLInUse.With(r.metrics).Record(float64(s.InUse))
}

// Stop signal background reporter to stop
// and wait for reporter to completely stopped
// safe to call multiple time
func (r *DBMetricsReporter) Stop() {
	if atomic.CompareAndSwapInt32(&r.stopped, 0, 1) {
		close(r.quit)
	}
	r.wg.Wait()
}
