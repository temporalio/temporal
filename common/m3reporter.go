package common

import (
	"time"

	"code.uber.internal/go-common.git/x/log"
	m3 "code.uber.internal/go-common.git/x/metrics"
)

// M3Reporter is the struct which implements the Reporter interface to
// report metrics to M3
type M3Reporter struct {
	// ms is the M3 Scope which will be initialized when we start the service
	ms m3.Scope

	// tags associated with this object
	tags map[string]string

	// timerMetrics is the map of all Timer style metrics
	// we keep this so that we don't create the object every single time
	timerMetrics map[string]*m3.Timer

	// counterMetrics is the map of all Counter style metrics
	counterMetrics map[string]*m3.Counter

	// gaugeMetrics is the map of all Gauge style metrics
	gaugeMetrics map[string]*m3.Gauge
}

// M3Stopwatch is the struct which implements the Stopwatch interface to
// start and stop the timer
type M3Stopwatch struct {
	mTimer    *m3.Timer
	tags      map[string]string
	startTime time.Time
}

func newStopWatch(mTimer *m3.Timer, tags map[string]string) *M3Stopwatch {
	sWatch := &M3Stopwatch{
		mTimer: mTimer,
		tags:   tags,
	}

	return sWatch
}

// NewM3Reporter returns a StatsReporter that reports to m3 on the given addr.
func NewM3Reporter(ms m3.Scope, tags map[string]string) Reporter {
	m3Report := &M3Reporter{
		ms:             ms,
		tags:           make(map[string]string),
		timerMetrics:   make(map[string]*m3.Timer),
		counterMetrics: make(map[string]*m3.Counter),
		gaugeMetrics:   make(map[string]*m3.Gauge),
	}

	if tags != nil {
		copyMap(tags, m3Report.tags)
	}

	// Tagged returns a new scope with the given tags, merges with any existing tags for the scope ms
	m3Report.ms = ms.Tagged(m3Report.GetTags())
	return m3Report
}

// GetChildReporter creates the child reporter for this parent reporter
func (r *M3Reporter) GetChildReporter(tags map[string]string) Reporter {

	m3Report := &M3Reporter{
		ms:             r.ms,
		tags:           make(map[string]string),
		timerMetrics:   make(map[string]*m3.Timer),
		counterMetrics: make(map[string]*m3.Counter),
		gaugeMetrics:   make(map[string]*m3.Gauge),
	}

	// copy the parent tags as well
	copyMap(r.GetTags(), m3Report.GetTags())

	if tags != nil {
		copyMap(tags, m3Report.tags)
	}

	// Tagged returns a new child scope with the given tags, merges with any existing tags
	m3Report.ms = r.ms.Tagged(m3Report.GetTags())

	return m3Report
}

// GetTags returns the tags for this reporter object
func (r *M3Reporter) GetTags() map[string]string {
	return r.tags
}

// getTimerScopeMap returns the respective scope map for this reporter object
func (r *M3Reporter) getTimerScopeMap() map[string]*m3.Timer {
	return r.timerMetrics
}

// getCounterScopeMap returns the respective scope map for this reporter object
func (r *M3Reporter) getCounterScopeMap() map[string]*m3.Counter {
	return r.counterMetrics
}

// getGaugeScopeMap returns the respective scope map for this reporter object
func (r *M3Reporter) getGaugeScopeMap() map[string]*m3.Gauge {
	return r.gaugeMetrics
}

// InitMetrics is used to create the M3 scope objects for the respective metrics
func (r *M3Reporter) InitMetrics(metricMap map[MetricName]MetricType) {
	for mName, mType := range metricMap {
		switch mType {
		case Counter:
			r.counterMetrics[string(mName)] = r.ms.Counter(string(mName))
		case Timer:
			r.timerMetrics[string(mName)] = r.ms.Timer(string(mName))
		case Gauge:
			r.gaugeMetrics[string(mName)] = r.ms.Gauge(string(mName))
		}
	}
}

// IncCounter reports Counter metric to M3
func (r *M3Reporter) IncCounter(name string, tags map[string]string, delta int64) {
	// First we need to make sure we create a counter type for this metric
	var mCounter *m3.Counter
	var ok bool
	// If we don't find this metric, this is a no-op
	if mCounter, ok = r.counterMetrics[name]; ok {
		// if there are no tags specified, just use the already existing
		if tags == nil {
			mCounter.Inc(delta)
		} else {
			mCounter.Tagged(tags).Inc(delta)
		}
	} else {
		log.Errorf("counter metric: %v doesn't have a scope object", name)
	}
}

// UpdateGauge reports Gauge type metric to M3
func (r *M3Reporter) UpdateGauge(name string, tags map[string]string, value int64) {
	// First we need to make sure we create a gauge type for this metric
	var mGauge *m3.Gauge
	var ok bool
	// If we don't find this metric, this is a no-op
	if mGauge, ok = r.gaugeMetrics[name]; ok {
		// if there are no tags specified, just use the already existing
		if tags == nil {
			mGauge.Update(value)
		} else {
			mGauge.Tagged(tags).Update(value)
		}
	} else {
		log.Errorf("gauge metric: %v doesn't have a scope object", name)
	}
}

// StartTimer returns a Stopwatch which when stopped will report the metric to M3
func (r *M3Reporter) StartTimer(name string, tags map[string]string) Stopwatch {
	// First we need to make sure we create a timer type for this metric
	var mTimer *m3.Timer
	var ok bool
	// If we don't find this metric, return an empty stopwatch
	if mTimer, ok = r.timerMetrics[name]; ok {
		// We need to create a new stopwatch each time because we can update the
		// metric simultaneously
		sw := newStopWatch(mTimer, tags)
		sw.start()
		return sw
	}
	// just return an empty stopwatch
	sw := &M3Stopwatch{}
	log.Errorf("timer metric: %v doesn't have a scope object", name)
	return sw
}

// RecordTimer should be used for measuring latency when you cannot start the stop watch.
func (r *M3Reporter) RecordTimer(name string, tags map[string]string, d time.Duration) {
	// First we need to make sure we create a timer type for this metric
	var mTimer *m3.Timer
	var ok bool
	// If we don't find this metric, return an empty stopwatch
	if mTimer, ok = r.timerMetrics[name]; ok {
		// if there are no tags specified, just use the already existing
		if tags == nil {
			mTimer.Record(d)
		} else {
			mTimer.Tagged(tags).Record(d)
		}
	} else {
		log.Errorf("timer metric: %v doesn't have a scope object", name)
	}
}

// start just sets the start time
func (r *M3Stopwatch) start() {
	r.startTime = time.Now()
}

// Stop stops the stop watch and records the latency to M3
func (r *M3Stopwatch) Stop() time.Duration {
	d := time.Since(r.startTime)
	if r.mTimer != nil {
		// if there are no tags specified, just use the already existing
		if r.tags == nil {
			r.mTimer.Record(d)
		} else {
			r.mTimer.Tagged(r.tags).Record(d)
		}
	}
	return d
}
