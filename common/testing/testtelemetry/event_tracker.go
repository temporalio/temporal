package testtelemetry

import (
	"context"
	"sync"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// EventMatcher defines an interface for matching OTEL span events.
type EventMatcher interface {
	// MatchesAfter returns the timestamp (in nanoseconds) of a matching event
	// that occurred after afterNanos, or 0 if no match is found.
	MatchesAfter(span *tracepb.Span, afterNanos uint64) uint64
}

type eventMatcherBuilder struct {
	name  string
	attrs map[string]any
}

// MatchEvent creates a new event matcher builder.
func MatchEvent() *eventMatcherBuilder {
	return &eventMatcherBuilder{
		attrs: make(map[string]any),
	}
}

// Name sets the event name to match.
func (b *eventMatcherBuilder) Name(name string) *eventMatcherBuilder {
	b.name = name
	return b
}

// Attr adds an attribute requirement to the matcher.
func (b *eventMatcherBuilder) Attr(key string, value any) *eventMatcherBuilder {
	b.attrs[key] = value
	return b
}

// MatchesAfter returns the timestamp of a matching event that occurred after afterNanos,
// or 0 if no match is found.
func (b *eventMatcherBuilder) MatchesAfter(span *tracepb.Span, afterNanos uint64) uint64 {
	for _, event := range span.Events {
		if event.TimeUnixNano <= afterNanos {
			continue
		}
		if b.name != "" && event.Name != b.name {
			continue
		}
		if matchAttributes(event.Attributes, b.attrs) {
			return event.TimeUnixNano
		}
	}
	return 0
}

func matchAttributes(attrs []*commonpb.KeyValue, expected map[string]any) bool {
	if len(expected) == 0 {
		return true
	}

	attrMap := make(map[string]*commonpb.AnyValue, len(attrs))
	for _, kv := range attrs {
		attrMap[kv.Key] = kv.Value
	}

	for key, expectedValue := range expected {
		actualValue, ok := attrMap[key]
		if !ok {
			return false
		}
		if !valueMatches(actualValue, expectedValue) {
			return false
		}
	}
	return true
}

func valueMatches(actual *commonpb.AnyValue, expected any) bool {
	switch v := expected.(type) {
	case string:
		return actual.GetStringValue() == v
	case int:
		return actual.GetIntValue() == int64(v)
	case int64:
		return actual.GetIntValue() == v
	case bool:
		return actual.GetBoolValue() == v
	case float64:
		return actual.GetDoubleValue() == v
	default:
		return false
	}
}

// EventTracker provides WaitFor functionality for OTEL span events.
// It maintains state about the last matched event time to avoid re-matching.
type EventTracker struct {
	subscription   *subscription
	mu             sync.Mutex
	lastMatchNanos uint64
	// pending holds spans that have been received but not yet matched.
	// This prevents losing spans when WaitFor doesn't find a match.
	pending []*tracepb.ResourceSpans
}

// NewEventTracker creates a new EventTracker for the given test.
// The subscription is automatically cleaned up when the test ends.
func NewEventTracker(t *testing.T, exporter *MemoryExporter) *EventTracker {
	return &EventTracker{
		subscription: exporter.subscribe(t),
	}
}

// WaitFor blocks until an event matching the given matcher is found or the context is cancelled.
// Only considers events that occurred after the last matched event to avoid re-matching.
func (e *EventTracker) WaitFor(ctx context.Context, matcher EventMatcher) error {
	for {
		e.mu.Lock()
		afterNanos := e.lastMatchNanos

		// Append any new spans from the subscription to our pending buffer.
		e.pending = append(e.pending, e.subscription.spans()...)

		// Check pending spans for a match.
		for _, rs := range e.pending {
			for _, ss := range rs.GetScopeSpans() {
				for _, span := range ss.GetSpans() {
					if ts := matcher.MatchesAfter(span, afterNanos); ts > 0 {
						e.lastMatchNanos = ts
						e.mu.Unlock()
						return nil
					}
				}
			}
		}
		e.mu.Unlock()

		// Wait for more spans or context cancellation.
		if err := e.subscription.wait(ctx); err != nil {
			return err
		}

		// Check if context was cancelled.
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}
