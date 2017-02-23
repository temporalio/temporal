package metrics

import (
	"time"

	"github.com/uber-go/tally"
)

type (
	// Client is  the interface used to report metrics tally.
	Client interface {
		// IncCounter increments a counter metric
		IncCounter(scope int, counter int)
		// AddCounter adds delta to the counter metric
		AddCounter(scope int, counter int, delta int64)
		// StartTimer starts a timer for the given
		// metric name. Time will be recorded when stopwatch is stopped.
		StartTimer(scope int, timer int) tally.Stopwatch
		// RecordTimer starts a timer for the given
		// metric name
		RecordTimer(scope int, timer int, d time.Duration)
		// UpdateGauge reports Gauge type metric
		UpdateGauge(scope int, gauge int, delta float64)
		// Tagged returns a client that adds the given tags to all metrics
		Tagged(tags map[string]string) Client
	}
)
