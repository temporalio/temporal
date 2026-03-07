package testtelemetry

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
)

// SpanPattern is a flat map of span attribute constraints that mirrors the
// shape of a span's attributes: each key names an attribute and its value
// specifies the expected content.
//
// All listed key-value pairs must be satisfied for a span to match. Attributes
// not listed in the pattern are not checked.
//
// Use the sentinel constants NotPresent and NonEmpty for structural assertions:
//
//	SpanPattern{
//	    "rpc.method":               "PollNexusTaskQueue",
//	    "temporalWorkflowID":       "my-workflow",
//	    "temporalNexusLinks":       NonEmpty,   // must be present with non-empty value
//	    "temporalNexusOperationID": NotPresent, // must be absent
//	}
type SpanPattern map[string]string

const (
	// NotPresent asserts the attribute must not exist on the span.
	NotPresent = "\x00not-present"
	// NonEmpty asserts the attribute must exist with a non-empty value.
	NonEmpty = "\x00non-empty"
)

// RequireSpans waits up to 10 seconds for the collector to contain a span
// satisfying each pattern in patterns. All patterns must be satisfied (each by
// at least one span) before the timeout; if not, the test fails.
func RequireSpans(tb testing.TB, c *MemoryCollector, patterns ...SpanPattern) {
	tb.Helper()
	assert.Eventually(tb, func() bool {
		for _, p := range patterns {
			if !c.HasSpan(p) {
				return false
			}
		}
		return true
	}, 10*time.Second, 500*time.Millisecond)
}

// HasSpan reports whether the collector currently contains at least one span
// that satisfies every constraint in p.
func (c *MemoryCollector) HasSpan(p SpanPattern) bool {
	for _, rs := range c.Spans() {
		for _, ss := range rs.ScopeSpans {
			for _, span := range ss.Spans {
				if spanMatches(span.Attributes, p) {
					return true
				}
			}
		}
	}
	return false
}

// spanMatches reports whether attrs satisfy all constraints in p.
func spanMatches(attrs []*commonpb.KeyValue, p SpanPattern) bool {
	for key, want := range p {
		got, present := spanAttrString(attrs, key)
		switch want {
		case NotPresent:
			if present {
				return false
			}
		case NonEmpty:
			if !present || got == "" {
				return false
			}
		default:
			if got != want {
				return false
			}
		}
	}
	return true
}

// spanAttrString returns the string representation of the attribute with the
// given key and whether the key was found at all.
//
//   - String attributes are returned verbatim.
//   - Array attributes are returned as a comma-joined string; the value is
//     non-empty only if the array contains at least one non-empty element.
func spanAttrString(attrs []*commonpb.KeyValue, key string) (value string, present bool) {
	for _, attr := range attrs {
		if attr.Key != key {
			continue
		}
		if s := attr.Value.GetStringValue(); s != "" {
			return s, true
		}
		if arr := attr.Value.GetArrayValue(); arr != nil {
			parts := make([]string, 0, len(arr.Values))
			for _, v := range arr.Values {
				if s := v.GetStringValue(); s != "" {
					parts = append(parts, s)
				}
			}
			return strings.Join(parts, ","), true
		}
		return "", true
	}
	return "", false
}
