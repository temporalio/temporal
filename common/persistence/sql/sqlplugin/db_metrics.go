package sqlplugin

import (
	"time"

	"go.temporal.io/server/common/metrics"
)

type DBMetricsReporter struct {
	interval time.Duration
	handle   *DatabaseHandle
	metrics  metrics.Handler
	quit     chan struct{}
}

func withDBMetricReporter(dbKind DbKind, handle *DatabaseHandle) {
	reporter := &DBMetricsReporter{
		interval: time.Minute,
		handle:   handle,
		metrics:  handle.metrics.WithTags(metrics.PersistenceDBKindTag(dbKind.String())),
		quit:     make(chan struct{}),
	}
	handle.reporter = reporter
	go reporter.Start()
}

func (r *DBMetricsReporter) Start() {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

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
}
