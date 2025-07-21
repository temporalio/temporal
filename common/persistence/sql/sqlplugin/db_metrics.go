package sqlplugin

import (
	"time"

	"go.temporal.io/server/common/metrics"
)

type DBMetricsReporter struct {
	interval time.Duration
	handle   *DatabaseHandle
	quit     chan struct{}
}

func withDBMetricReporter(handle *DatabaseHandle) {
	reporter := &DBMetricsReporter{
		interval: time.Minute,
		handle:   handle,
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
			db := r.handle.db.Load()
			if db != nil {
				s := db.Stats()
				metrics.PersistenceSqlMaxOpenConn.With(r.handle.metrics).Record(float64(s.MaxOpenConnections))
				metrics.PersistenceSqlOpenConn.With(r.handle.metrics).Record(float64(s.OpenConnections))
				metrics.PersistenceSqlIdleConn.With(r.handle.metrics).Record(float64(s.Idle))
			}
		}
	}
}

func (r *DBMetricsReporter) Stop() {
	close(r.quit)
}
