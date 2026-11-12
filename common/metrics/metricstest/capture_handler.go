package metricstest

import (
	"time"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/testx/metricstest"
)

// CapturedRecording is a single recording. Fields here should not be mutated.
type CapturedRecording struct {
	Value any
	Tags  map[string]string
	Unit  metrics.MetricUnit
}

// CaptureHandler is a [metrics.Handler] that captures each metric recording.
type CaptureHandler struct {
	*metricstest.Recorder[CapturedRecording]
	tags []metrics.Tag
}

var _ metrics.Handler = (*CaptureHandler)(nil)

// NewCaptureHandler creates a new [metrics.Handler] that captures.
func NewCaptureHandler() *CaptureHandler {
	return &CaptureHandler{Recorder: metricstest.NewRecorder[CapturedRecording]()}
}

// WithTags implements [metrics.Handler.WithTags].
func (c *CaptureHandler) WithTags(tags ...metrics.Tag) metrics.Handler {
	return &CaptureHandler{
		Recorder: c.Recorder,
		tags:     append(append(make([]metrics.Tag, 0, len(c.tags)+len(tags)), c.tags...), tags...),
	}
}

func (c *CaptureHandler) record(name string, v any, unit metrics.MetricUnit, tags ...metrics.Tag) {
	c.Record(name, func() *CapturedRecording {
		rec := &CapturedRecording{Value: v, Tags: make(map[string]string, len(c.tags)+len(tags)), Unit: unit}
		for _, tag := range c.tags {
			rec.Tags[tag.Key] = tag.Value
		}
		for _, tag := range tags {
			rec.Tags[tag.Key] = tag.Value
		}
		return rec
	})
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

func (c *CaptureHandler) Close() error {
	return nil
}

func (c *CaptureHandler) StartBatch(_ string) metrics.BatchHandler {
	return c
}

// Stop implements [metrics.Handler.Stop].
func (*CaptureHandler) Stop(log.Logger) {}
