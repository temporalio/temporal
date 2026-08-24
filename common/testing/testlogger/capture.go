package testlogger

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/google/go-cmp/cmp"
	"go.temporal.io/server/common/log/tag"
)

// CapturedLog is a single log call recorded by a Capture.
type CapturedLog struct {
	Level   Level
	Message string
	Tags    []tag.Tag
}

// CapturedLogPattern describes a captured log using formatted tag values.
// Tags match a subset of the captured log's tags.
type CapturedLogPattern struct {
	Level   Level
	Message string
	Tags    map[string]string
}

func (p CapturedLogPattern) matches(record CapturedLog) bool {
	if record.Level != p.Level || record.Message != p.Message {
		return false
	}

	for key, expected := range p.Tags {
		if !slices.ContainsFunc(record.Tags, func(actual tag.Tag) bool {
			return actual.Key() == key && formatValue(actual) == expected
		}) {
			return false
		}
	}
	return true
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

// RequireContains fails the test with tag diffs when the capture does not include a matching log.
func (c *Capture) RequireContains(t TestingT, pattern CapturedLogPattern) {
	t.Helper()
	records := c.Snapshot()
	if slices.ContainsFunc(records, pattern.matches) {
		return
	}

	var failure strings.Builder
	fmt.Fprintf(&failure, "captured log pattern not found: level=%s message=%q", pattern.Level, pattern.Message)
	candidateCount := 0
	for _, record := range records {
		if record.Level != pattern.Level || record.Message != pattern.Message {
			continue
		}
		candidateCount++
		actualTags := make(map[string]string, len(pattern.Tags))
		for _, actual := range record.Tags {
			key := actual.Key()
			if _, expected := pattern.Tags[key]; expected {
				actualTags[key] = formatValue(actual)
			}
		}
		fmt.Fprintf(&failure, "\n\ncandidate %d tag mismatch (-want +got):\n%s", candidateCount, cmp.Diff(pattern.Tags, actualTags))
	}
	if candidateCount == 0 {
		fmt.Fprintf(&failure, "\n\nno captured log had the expected level and message; captured logs: %+v", records)
	}
	t.Fatalf("%s", failure.String())
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
