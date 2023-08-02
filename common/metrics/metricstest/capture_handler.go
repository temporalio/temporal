// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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

// Capture is a specific capture instance.
type Capture struct {
	recordings     map[string][]*CapturedRecording
	recordingsLock sync.RWMutex
}

// Snapshot returns a copy of all metrics recorded, keyed by name.
func (c *Capture) Snapshot() map[string][]*CapturedRecording {
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

func (c *Capture) record(name string, r *CapturedRecording) {
	c.recordingsLock.Lock()
	defer c.recordingsLock.Unlock()
	c.recordings[name] = append(c.recordings[name], r)
}

// CaptureHandler is a [metrics.Handler] that captures each metric recording.
type CaptureHandler struct {
	tags         []metrics.Tag
	captures     map[*Capture]struct{}
	capturesLock *sync.RWMutex
}

var _ metrics.Handler = (*CaptureHandler)(nil)

// NewCaptureHandler creates a new [metrics.Handler] that captures.
func NewCaptureHandler() *CaptureHandler {
	return &CaptureHandler{
		captures:     map[*Capture]struct{}{},
		capturesLock: &sync.RWMutex{},
	}
}

// StartCapture returns a started capture. StopCapture should be called on
// complete.
func (c *CaptureHandler) StartCapture() *Capture {
	capture := &Capture{recordings: map[string][]*CapturedRecording{}}
	c.capturesLock.Lock()
	defer c.capturesLock.Unlock()
	c.captures[capture] = struct{}{}
	return capture
}

// StopCapture stops capturing metrics for the given capture instance.
func (c *CaptureHandler) StopCapture(capture *Capture) {
	c.capturesLock.Lock()
	defer c.capturesLock.Unlock()
	delete(c.captures, capture)
}

// WithTags implements [metrics.Handler.WithTags].
func (c *CaptureHandler) WithTags(tags ...metrics.Tag) metrics.Handler {
	return &CaptureHandler{
		tags:         append(append(make([]metrics.Tag, 0, len(c.tags)+len(tags)), c.tags...), tags...),
		captures:     c.captures,
		capturesLock: c.capturesLock,
	}
}

func (c *CaptureHandler) record(name string, v any, unit metrics.MetricUnit, tags ...metrics.Tag) {
	rec := &CapturedRecording{Value: v, Tags: make(map[string]string, len(c.tags)+len(tags)), Unit: unit}
	for _, tag := range c.tags {
		rec.Tags[tag.Key()] = tag.Value()
	}
	for _, tag := range tags {
		rec.Tags[tag.Key()] = tag.Value()
	}
	c.capturesLock.RLock()
	defer c.capturesLock.RUnlock()
	for c := range c.captures {
		c.record(name, rec)
	}
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
