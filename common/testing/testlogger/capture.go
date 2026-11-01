package testlogger

import (
	"go.temporal.io/server/common/log/tag"
	shared "go.temporal.io/testx/testlogger"
)

// CapturedLog is a single log call recorded by a Capture.
type CapturedLog struct {
	Level   Level
	Message string
	Tags    []tag.Tag
}

// CapturedLogPattern describes a captured log using formatted tag values.
// Tags match a subset of the captured log's tags; AnyTagValue requires a tag
// without constraining its value.
type CapturedLogPattern struct {
	Level   Level
	Message string
	Tags    map[string]any
}

// AnyTagValue matches any formatted value for a tag that must be present.
var AnyTagValue = shared.AnyFieldValue

// Capture is an opt-in recording of TestLogger calls.
type Capture struct{ capture *shared.Capture }

// StartCapture starts recording log calls that contain any of the provided exact tags.
// It records all calls when no tags are provided.
func (tl *TestLogger) StartCapture(anyTags ...tag.Tag) *Capture {
	return &Capture{tl.TestLogger.StartCapture(toFields(anyTags)...)}
}

// StopCapture stops recording log calls for capture.
func (tl *TestLogger) StopCapture(capture *Capture) { tl.TestLogger.StopCapture(capture.capture) }

// Snapshot returns a defensive copy of the captured log calls.
func (c *Capture) Snapshot() []CapturedLog {
	records := c.capture.Snapshot()
	result := make([]CapturedLog, len(records))
	for i, record := range records {
		result[i] = CapturedLog{record.Level, record.Message, toTags(record.Fields)}
	}
	return result
}

// RequireContains fails the test with tag diffs when the capture does not include a matching log.
func (c *Capture) RequireContains(t TestingT, pattern CapturedLogPattern) {
	t.Helper()
	c.capture.RequireContains(t, shared.CapturedLogPattern{Level: pattern.Level, Message: pattern.Message, Fields: pattern.Tags})
}
