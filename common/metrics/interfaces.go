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
		// UpdateGauge reports Gauge type absolute value metric
		UpdateGauge(scope int, gauge int, value float64)
		// Scope return an internal scope that can be used to add additional
		// information to metrics
		Scope(scope int, tags ...Tag) Scope
	}

	// Scope is an interface for metrics
	Scope interface {
		// IncCounter increments a counter metric
		IncCounter(counter int)
		// AddCounter adds delta to the counter metric
		AddCounter(counter int, delta int64)
		// StartTimer starts a timer for the given metric name.
		// Time will be recorded when stopwatch is stopped.
		StartTimer(timer int) Stopwatch
		// RecordTimer starts a timer for the given metric name
		RecordTimer(timer int, d time.Duration)
		// RecordHistogramDuration records a histogram duration value for the given
		// metric name
		RecordHistogramDuration(timer int, d time.Duration)
		// RecordHistogramValue records a histogram value for the given metric name
		RecordHistogramValue(timer int, value float64)
		// UpdateGauge reports Gauge type absolute value metric
		UpdateGauge(gauge int, value float64)
		// Tagged return an internal scope that can be used to add additional
		// information to metrics
		Tagged(tags ...Tag) Scope
	}
)
