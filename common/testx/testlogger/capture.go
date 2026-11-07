package testlogger

import (
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/google/go-cmp/cmp"
	"go.uber.org/zap"
)

// CapturedLog is a single log call recorded by a Capture.
type CapturedLog struct {
	Level   Level
	Message string
	Fields  []zap.Field
}

// CapturedLogPattern describes a captured log using formatted field values.
// Fields match a subset of the captured log's fields; AnyFieldValue requires a field
// without constraining its value.
type CapturedLogPattern struct {
	Level   Level
	Message string
	Fields  map[string]any
}

type anyFieldValue struct{}

// AnyFieldValue matches any formatted value for a field that must be present.
var AnyFieldValue = &anyFieldValue{}

func (p CapturedLogPattern) matches(record CapturedLog) bool {
	if record.Level != p.Level || record.Message != p.Message {
		return false
	}

	for key, expected := range p.Fields {
		if !slices.ContainsFunc(record.Fields, func(actual zap.Field) bool {
			if actual.Key != key {
				return false
			}
			_, anyValue := expected.(*anyFieldValue)
			return anyValue || formatValue(actual) == fmt.Sprint(expected)
		}) {
			return false
		}
	}
	return true
}

func (p CapturedLogPattern) formattedFields() map[string]string {
	formatted := make(map[string]string, len(p.Fields))
	for key, value := range p.Fields {
		if _, anyValue := value.(*anyFieldValue); anyValue {
			formatted[key] = "<any>"
		} else {
			formatted[key] = fmt.Sprint(value)
		}
	}
	return formatted
}

type captureFilterField struct {
	key   string
	value string
}

// Capture is an opt-in recording of TestLogger calls.
type Capture struct {
	filterFields map[captureFilterField]struct{}

	mu      sync.Mutex
	records []CapturedLog
}

func newCapture(anyFields []zap.Field) *Capture {
	capture := &Capture{}
	if len(anyFields) == 0 {
		return capture
	}
	capture.filterFields = make(map[captureFilterField]struct{}, len(anyFields))
	for _, t := range anyFields {
		capture.filterFields[captureFilterField{key: t.Key, value: formatValue(t)}] = struct{}{}
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
		records[i].Fields = slices.Clone(record.Fields)
	}
	return records
}

// RequireContains fails the test with field diffs when the capture does not include a matching log.
func (c *Capture) RequireContains(t TestingT, pattern CapturedLogPattern) {
	t.Helper()
	records := c.Snapshot()
	if slices.ContainsFunc(records, pattern.matches) {
		return
	}

	var failure strings.Builder
	fmt.Fprintf(&failure, "captured log pattern not found: level=%s message=%q", pattern.Level, pattern.Message)
	expectedFields := pattern.formattedFields()
	candidateCount := 0
	for _, record := range records {
		if record.Level != pattern.Level || record.Message != pattern.Message {
			continue
		}
		candidateCount++
		actualFields := make(map[string]string, len(pattern.Fields))
		for _, actual := range record.Fields {
			key := actual.Key
			if _, anyValue := pattern.Fields[key].(*anyFieldValue); anyValue {
				actualFields[key] = "<any>"
			} else if _, expected := pattern.Fields[key]; expected {
				actualFields[key] = formatValue(actual)
			}
		}
		fmt.Fprintf(&failure, "\n\ncandidate %d tag mismatch (-want +got):\n%s", candidateCount, cmp.Diff(expectedFields, actualFields))
	}
	if candidateCount == 0 {
		failure.WriteString("\n\nno captured log had the expected level and message; captured logs:")
		for _, record := range records {
			fmt.Fprintf(&failure, "\n- %s %q", record.Level, record.Message)
			for _, actual := range record.Fields {
				fmt.Fprintf(&failure, " %s=%s", actual.Key, formatValue(actual))
			}
		}
	}
	t.Fatalf("%s", failure.String())
}

func (c *Capture) record(record CapturedLog) {
	if len(c.filterFields) > 0 {
		matched := false
		for _, t := range record.Fields {
			if _, ok := c.filterFields[captureFilterField{key: t.Key, value: formatValue(t)}]; ok {
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
