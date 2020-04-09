package metrics

import (
	"time"

	"github.com/uber-go/tally"
)

// Stopwatch is a helper for simpler tracking of elapsed time, use the
// Stop() method to report time elapsed since its created back to the
// timer or histogram.
type Stopwatch struct {
	start  time.Time
	timers []tally.Timer
}

// NewStopwatch creates a new immutable stopwatch for recording the start
// time to a stopwatch reporter.
func NewStopwatch(timers ...tally.Timer) Stopwatch {
	return Stopwatch{time.Now(), timers}
}

// NewTestStopwatch returns a new test stopwatch
func NewTestStopwatch() Stopwatch {
	return Stopwatch{time.Now(), []tally.Timer{}}
}

// Stop reports time elapsed since the stopwatch start to the recorder.
func (sw Stopwatch) Stop() {
	d := time.Since(sw.start)
	for _, timer := range sw.timers {
		timer.Record(d)
	}
}
