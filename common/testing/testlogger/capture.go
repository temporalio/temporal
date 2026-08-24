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
	return true
}

func (p CapturedLogPattern) formattedTags() map[string]string {
	formatted := make(map[string]string, len(p.Tags))
	for key, value := range p.Tags {
		if _, anyValue := value.(*anyTagValue); anyValue {
			formatted[key] = "<any>"
		} else {
			formatted[key] = fmt.Sprint(value)
		}
	}
	return formatted
}

type requireTestingT interface {
	Helper()
	Fatalf(format string, args ...any)
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

// RequireContains fails the test with tag diffs when the capture does not include a matching log.
func (c *Capture) RequireContains(t requireTestingT, pattern CapturedLogPattern) {
	t.Helper()
	records := c.Snapshot()
	if slices.ContainsFunc(records, pattern.matches) {
		return
	}

	var failure strings.Builder
	fmt.Fprintf(&failure, "captured log pattern not found: level=%s message=%q", pattern.Level, pattern.Message)
	expectedTags := pattern.formattedTags()
	candidateCount := 0
	for _, record := range records {
		if record.Level != pattern.Level || record.Message != pattern.Message {
			continue
		}
		candidateCount++
		actualTags := make(map[string]string, len(record.Tags))
		for _, actual := range record.Tags {
			key := actual.Key()
			if _, anyValue := pattern.Tags[key].(*anyTagValue); anyValue {
				actualTags[key] = "<any>"
			} else {
				actualTags[key] = formatValue(actual)
			}
		}
		fmt.Fprintf(&failure, "\n\ncandidate %d tag mismatch (-want +got):\n%s", candidateCount, cmp.Diff(expectedTags, actualTags))
	}
	if candidateCount == 0 {
		fmt.Fprintf(&failure, "\n\nno captured log had the expected level and message; captured logs: %+v", records)
	}
	t.Fatalf("%s", failure.String())
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
