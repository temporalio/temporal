package metricstest

import (
	"sync"
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

// CapturedRecording is a single recording. Fields here should not be mutated.
type CapturedRecording struct {
	Value any
	Tags  map[string]string
	Unit  metrics.MetricUnit
}

// CaptureHandler is a [metrics.Handler] that captures each metric recording.
type CaptureHandler struct {
	tags           []metrics.Tag
	recordings     map[string][]*CapturedRecording
	recordingsLock *sync.RWMutex
}

var _ metrics.Handler = (*CaptureHandler)(nil)

// NewCaptureHandler creates a new [metrics.Handler] that captures.
func NewCaptureHandler() *CaptureHandler {
	return &CaptureHandler{
		recordings:     map[string][]*CapturedRecording{},
		recordingsLock: &sync.RWMutex{},
	}
}

// Snapshot returns a copy of all metrics recorded, keyed by name.
func (c *CaptureHandler) Snapshot() map[string][]*CapturedRecording {
	c.recordingsLock.RLock()
	defer c.recordingsLock.RUnlock()
	ret := make(map[string][]*CapturedRecording, len(c.recordings))
	for k, v := range c.recordings {
		recs := make([]*CapturedRecording, len(v))
		copy(recs, v)
		ret[k] = recs
	}
	return ret
}

// Clear clears all recorded metrics.
func (c *CaptureHandler) Clear() {
	c.recordingsLock.Lock()
	defer c.recordingsLock.Unlock()
	c.recordings = map[string][]*CapturedRecording{}
}

// WithTags implements [metrics.Handler.WithTags].
func (c *CaptureHandler) WithTags(tags ...metrics.Tag) metrics.Handler {
	return &CaptureHandler{
		tags:           append(c.tags, tags...),
		recordings:     c.recordings,
		recordingsLock: c.recordingsLock,
	}
}

func (c *CaptureHandler) record(name string, v any, unit metrics.MetricUnit, tags ...metrics.Tag) {
	c.recordingsLock.Lock()
	defer c.recordingsLock.Unlock()
	tagMap := make(map[string]string, len(c.tags)+len(tags))
	for _, tag := range c.tags {
		tagMap[tag.Key()] = tag.Value()
	}
	for _, tag := range tags {
		tagMap[tag.Key()] = tag.Value()
	}
	c.recordings[name] = append(c.recordings[name], &CapturedRecording{Value: v, Tags: tagMap, Unit: unit})
}

// Counter implements [metrics.Handler.Counter].
func (c *CaptureHandler) Counter(name string) metrics.CounterIface {
	return metrics.CounterFunc(func(v int64, tags ...metrics.Tag) { c.record(name, v, "", tags...) })
}

// Gauge implements [metrics.Handler.Gauge].
func (c *CaptureHandler) Gauge(name string) metrics.GaugeIface {
	return metrics.GaugeFunc(func(v float64, tags ...metrics.Tag) { c.record(name, v, "", tags...) })
}

// Timer implements [metrics.Handler.Timer].
func (c *CaptureHandler) Timer(name string) metrics.TimerIface {
	return metrics.TimerFunc(func(v time.Duration, tags ...metrics.Tag) { c.record(name, v, "", tags...) })
}

// Histogram implements [metrics.Handler.Histogram].
func (c *CaptureHandler) Histogram(name string, unit metrics.MetricUnit) metrics.HistogramIface {
	return metrics.HistogramFunc(func(v int64, tags ...metrics.Tag) { c.record(name, v, unit, tags...) })
}

// Stop implements [metrics.Handler.Stop].
func (*CaptureHandler) Stop(log.Logger) {}
