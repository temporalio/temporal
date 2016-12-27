package metrics

import (
	"time"

	"code.uber.internal/devexp/minions/common/util"
)

type (
	// SimpleReporter is the reporter used to dump metric to console for stress runs
	SimpleReporter struct {
		tags map[string]string
	}

	simpleStopWatch struct {
		metricName string
		reporter   *SimpleReporter
		startTime  time.Time
		elasped    time.Duration
	}
)

// NewSimpleReporter create an instance of Reporter which can be used for driver to emit metric to console
func NewSimpleReporter(tags map[string]string) Reporter {
	reporter := &SimpleReporter{
		tags: make(map[string]string),
	}

	if tags != nil {
		util.MergeDictoRight(tags, reporter.tags)
	}

	return reporter
}

// InitMetrics is used to initialize the metrics map with the respective type
func (r *SimpleReporter) InitMetrics(metricMap map[MetricName]MetricType) {
	// This is a no-op for simple reporter as it is already have a static list of metric to work with
}

// GetChildReporter creates the child reporter for this parent reporter
func (r *SimpleReporter) GetChildReporter(tags map[string]string) Reporter {

	sr := &SimpleReporter{
		tags: make(map[string]string),
	}

	// copy the parent tags as well
	util.MergeDictoRight(r.GetTags(), sr.GetTags())

	if tags != nil {
		util.MergeDictoRight(tags, sr.tags)
	}

	return sr
}

// GetTags returns the tags for this reporter object
func (r *SimpleReporter) GetTags() map[string]string {
	return r.tags
}

// IncCounter reports Counter metric to M3
func (r *SimpleReporter) IncCounter(name string, tags map[string]string, delta int64) {
	// not implemented
}

// UpdateGauge reports Gauge type metric
func (r *SimpleReporter) UpdateGauge(name string, tags map[string]string, value int64) {
	// Not implemented
}

func newSimpleStopWatch(metricName string, reporter *SimpleReporter) *simpleStopWatch {
	watch := &simpleStopWatch{
		metricName: metricName,
		reporter:   reporter,
	}

	return watch
}

func (w *simpleStopWatch) Start() {
	w.startTime = time.Now()
}

func (w *simpleStopWatch) Stop() time.Duration {
	w.elasped = time.Since(w.startTime)

	return w.elasped
}

// StartTimer returns a Stopwatch which when stopped will report the metric to M3
func (r *SimpleReporter) StartTimer(name string, tags map[string]string) Stopwatch {
	w := newSimpleStopWatch(name, r)
	w.Start()
	return w
}

// RecordTimer should be used for measuring latency when you cannot start the stop watch.
func (r *SimpleReporter) RecordTimer(name string, tags map[string]string, d time.Duration) {
	// Record the time as counter of time in milliseconds
	// not implemented
}
