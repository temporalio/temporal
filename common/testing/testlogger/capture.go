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

// TagValue returns the formatted value of the first tag with the provided key.
func (r CapturedLog) TagValue(key string) (string, bool) {
	for _, t := range r.Tags {
		if t.Key() == key {
			return formatValue(t), true
		}
	}
	return "", false
}

// Capture is an opt-in recording of TestLogger calls.
type Capture struct {
	anyTags map[string]map[string]struct{}

	mu      sync.Mutex
	records []CapturedLog
}

func newCapture(anyTags []tag.Tag) *Capture {
	capture := &Capture{}
	if len(anyTags) == 0 {
		return capture
	}
	capture.anyTags = make(map[string]map[string]struct{})
	for _, t := range anyTags {
		values := capture.anyTags[t.Key()]
		if values == nil {
			values = make(map[string]struct{})
			capture.anyTags[t.Key()] = values
		}
		values[formatValue(t)] = struct{}{}
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
	if len(c.anyTags) > 0 {
		matched := false
		for _, t := range record.Tags {
			if _, ok := c.anyTags[t.Key()][formatValue(t)]; ok {
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
