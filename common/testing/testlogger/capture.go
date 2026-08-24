package testlogger

import (
	"slices"
	"sync"

	"go.temporal.io/server/common/log/tag"
)

// CapturedLog is a single log call recorded by a Capture.
type CapturedLog struct {
	Level   Level
	Message string
	Tags    []tag.Tag
}

type captureFilterTag struct {
	key   string
	value string
}

// Capture is an opt-in recording of TestLogger calls.
type Capture struct {
	filterTags map[captureFilterTag]struct{}

	mu      sync.Mutex
	records []CapturedLog
}

func newCapture(anyTags []tag.Tag) *Capture {
	capture := &Capture{}
	if len(anyTags) == 0 {
		return capture
	}
	capture.filterTags = make(map[captureFilterTag]struct{}, len(anyTags))
	for _, t := range anyTags {
		capture.filterTags[captureFilterTag{key: t.Key(), value: formatValue(t)}] = struct{}{}
	}
	return capture
}

// Snapshot returns a defensive copy of the captured log calls.
func (c *Capture) Snapshot() []CapturedLog {
	c.mu.Lock()
	defer c.mu.Unlock()

	records := make([]CapturedLog, len(c.records))
	for i, record := range c.records {
		records[i] = record
		records[i].Tags = slices.Clone(record.Tags)
	}
	return records
}

func (c *Capture) record(record CapturedLog) {
	if len(c.filterTags) > 0 {
		matched := false
		for _, t := range record.Tags {
			if _, ok := c.filterTags[captureFilterTag{key: t.Key(), value: formatValue(t)}]; ok {
				matched = true
				break
			}
		}
		if !matched {
			return
		}
	}
	c.mu.Lock()
	c.records = append(c.records, record)
	c.mu.Unlock()
}
