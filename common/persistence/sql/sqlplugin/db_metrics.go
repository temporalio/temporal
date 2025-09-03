package sqlplugin

import (
	"sync"
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

func (r *DBMetricsReporter) Start() {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	r.wg.Add(1)
	defer r.wg.Done()

	for {
		select {
		case <-r.quit:
			return
		case <-ticker.C:
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

func (r *DBMetricsReporter) Stop() {
	close(r.quit)
	// assuming Start is already called
	r.wg.Wait()
}
