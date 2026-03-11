package testtelemetry

import (
	"fmt"
	"regexp"
	"strconv"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// errorTraceRe matches the "Error Trace:" line in testify's formatted assertion output.
// Example: "	Error Trace:	/path/to/file.go:42"
var errorTraceRe = regexp.MustCompile(`Error Trace:\s+(.+):(\d+)`)

// AssertT wraps a testing.TB to intercept assertion failures from testify's require package
// and record them as OTEL events on a span. When testify's require functions fail, they call
// Errorf with the formatted failure message, then FailNow. We intercept Errorf to capture
// the error message and source location.
type AssertT struct {
	testing.TB
	span trace.Span
}

// NewAssertT creates an AssertT that records assertion failures as events on the given span.
func NewAssertT(tb testing.TB, span trace.Span) *AssertT {
	return &AssertT{TB: tb, span: span}
}

// Errorf intercepts testify's assertion failure reporting and adds an OTEL event.
func (t *AssertT) Errorf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	file, line := parseErrorTrace(msg)
	t.span.AddEvent("test assertion failed", trace.WithAttributes(
		attribute.String("error.message", msg),
		attribute.String("code.filepath", file),
		attribute.Int("code.lineno", line),
	))
	t.TB.Errorf(format, args...)
}

// parseErrorTrace extracts file path and line number from testify's formatted error message.
func parseErrorTrace(msg string) (string, int) {
	m := errorTraceRe.FindStringSubmatch(msg)
	if len(m) == 3 {
		line, _ := strconv.Atoi(m[2])
		return m[1], line
	}
	return "unknown", 0
}
