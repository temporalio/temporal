package testlogger

import (
	"fmt"
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

// CapturedLogPattern describes a captured log using formatted tag values.
// Tags must match exactly; AnyTagValue requires a tag without constraining its value.
type CapturedLogPattern struct {
	Level   Level
	Message string
	Tags    map[string]any
}

type anyTagValue struct{}

// AnyTagValue matches any formatted value for a tag that must be present.
var AnyTagValue = &anyTagValue{}

func (p CapturedLogPattern) matches(record CapturedLog) bool {
	if record.Level != p.Level || record.Message != p.Message || len(record.Tags) != len(p.Tags) {
		return false
	}

	matchedTags := make(map[string]struct{}, len(record.Tags))
	for _, actual := range record.Tags {
		key := actual.Key()
		expected, ok := p.Tags[key]
		if !ok {
			return false
		}
		if _, duplicate := matchedTags[key]; duplicate {
			return false
		}
		matchedTags[key] = struct{}{}
		if _, anyValue := expected.(*anyTagValue); !anyValue && formatValue(actual) != fmt.Sprint(expected) {
			return false
		}
	}
	return len(matchedTags) == len(p.Tags)
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

// Contains reports whether the capture includes a log matching pattern.
func (c *Capture) Contains(pattern CapturedLogPattern) bool {
	return slices.ContainsFunc(c.Snapshot(), pattern.matches)
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
