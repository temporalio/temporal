package metrics

import (
	"time"

	"github.com/uber-go/tally"
)

type (
	// Client is  the interface used to report metrics to m3 backend.
	Client interface {
		// IncCounter increments a counter and emits
		// to m3 backend
		IncCounter(scope int, counter int)
		// AddCounter adds delta to the counter and
		// emits to the m3 backend
		AddCounter(scope int, counter int, delta int64)
		// StartTimer starts a timer for the given
		// metric name
		StartTimer(scope int, timer int) tally.Stopwatch
		// RecordTimer starts a timer for the given
		// metric name
		RecordTimer(scope int, timer int, d time.Duration)
		// UpdateGauge reports Gauge type metric to M3
		UpdateGauge(scope int, gauge int, delta float64)
	}
)
