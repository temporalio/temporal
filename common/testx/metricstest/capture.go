// Package metricstest captures metric recordings for test assertions.
package metricstest

import (
	"maps"
	"slices"
	"sync"
	"sync/atomic"
)

// Capture is a specific capture instance.
type Capture[R any] struct {
	recordings     CaptureSnapshot[R]
	recordingsLock sync.RWMutex
	filter         func(string, *R) bool
}

type CaptureSnapshot[R any] = map[string][]*R

// Snapshot returns a copy of all metrics recorded, keyed by name.
func (c *Capture[R]) Snapshot() CaptureSnapshot[R] {
	c.recordingsLock.RLock()
	defer c.recordingsLock.RUnlock()
	ret := maps.Clone(c.recordings)
	for k, v := range ret {
		ret[k] = slices.Clone(v)
	}
	return ret
}

// SnapshotMetric returns a copy of the recordings for one metric.
func (c *Capture[R]) SnapshotMetric(name string) []*R {
	c.recordingsLock.RLock()
	defer c.recordingsLock.RUnlock()
	return slices.Clone(c.recordings[name])
}

func (c *Capture[R]) record(name string, r *R) {
	if c.filter != nil && !c.filter(name, r) {
		return
	}
	c.recordingsLock.Lock()
	defer c.recordingsLock.Unlock()
	c.recordings[name] = append(c.recordings[name], r)
}

// Recorder shares recordings with all active captures. Recorded values must not be mutated.
type Recorder[R any] struct {
	captures     map[*Capture[R]]struct{}
	capturesLock sync.RWMutex
	captureCount atomic.Int32
}

func NewRecorder[R any]() *Recorder[R] {
	return &Recorder[R]{captures: map[*Capture[R]]struct{}{}}
}

// StartCapture returns a started capture. StopCapture should be called on
// complete.
func (c *Recorder[R]) StartCapture() *Capture[R] {
	return c.StartCaptureWithFilter(nil)
}

// StartCaptureWithFilter retains only recordings accepted by filter. A nil filter
// accepts all recordings. The filter must be safe for concurrent calls and must
// not call back into the recorder. StopCapture should be called on completion.
func (c *Recorder[R]) StartCaptureWithFilter(filter func(string, *R) bool) *Capture[R] {
	capture := &Capture[R]{recordings: make(CaptureSnapshot[R]), filter: filter}
	c.capturesLock.Lock()
	defer c.capturesLock.Unlock()

	c.captures[capture] = struct{}{}
	c.captureCount.Add(1)
	return capture
}

// StopCapture stops capturing metrics for the given capture instance.
func (c *Recorder[R]) StopCapture(capture *Capture[R]) {
	c.capturesLock.Lock()
	defer c.capturesLock.Unlock()

	delete(c.captures, capture)
	c.captureCount.Add(-1)
}

// Record builds a recording only when captures are active. The recording is shared
// between captures and must not be mutated after it is returned by build.
func (c *Recorder[R]) Record(name string, build func() *R) {
	// If no captures are active, discard the metric to save memory.
	if c.captureCount.Load() == 0 {
		return
	}

	rec := build()
	c.capturesLock.RLock()
	defer c.capturesLock.RUnlock()
	for cap := range c.captures {
		cap.record(name, rec)
	}
}
