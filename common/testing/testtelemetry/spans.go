package testtelemetry

import (
	"slices"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// LocalSpanID contains stable identifiers for one span and its trace-local relationships.
type LocalSpanID struct {
	Trace  int
	Span   int
	Parent int
}

// idGenerator assigns stable, one-based local IDs while preserving zero as no ID.
type idGenerator[T comparable] struct {
	ids map[T]int
}

func (g *idGenerator[T]) get(value T) int {
	var zero T
	if value == zero {
		return 0
	}
	if id, ok := g.ids[value]; ok {
		return id
	}
	if g.ids == nil {
		g.ids = make(map[T]int)
	}
	id := len(g.ids) + 1
	g.ids[value] = id
	return id
}

func (g *idGenerator[T]) lookup(value T) int {
	return g.ids[value]
}

func localIDs[T comparable](values []T) []int {
	var ids idGenerator[T]
	result := make([]int, len(values))
	for i, value := range values {
		result[i] = ids.get(value)
	}
	return result
}

// LocalAttributeIDs assigns stable IDs to an attribute's values in span order.
func LocalAttributeIDs(spans tracetest.SpanStubs, key attribute.Key) []int {
	values := make([]string, len(spans))
	for i, span := range spans {
		if value, ok := SpanAttribute(span, key); ok {
			values[i] = value.String()
		}
	}
	return localIDs(values)
}

// LocalSpanIDs assigns stable, one-based IDs in the order spans are provided.
// Callers must sort spans first when they need deterministic ordering.
func LocalSpanIDs(spans tracetest.SpanStubs) []LocalSpanID {
	var traceIDs idGenerator[oteltrace.TraceID]
	spanIDs := make(map[oteltrace.TraceID]*idGenerator[oteltrace.SpanID])
	for _, span := range spans {
		traceID := span.SpanContext.TraceID()
		traceIDs.get(traceID)
		if spanIDs[traceID] == nil {
			spanIDs[traceID] = new(idGenerator[oteltrace.SpanID])
		}
		spanIDs[traceID].get(span.SpanContext.SpanID())
	}

	result := make([]LocalSpanID, len(spans))
	for i, span := range spans {
		traceID := span.SpanContext.TraceID()
		result[i] = LocalSpanID{
			Trace:  traceIDs.lookup(traceID),
			Span:   spanIDs[traceID].lookup(span.SpanContext.SpanID()),
			Parent: spanIDs[traceID].lookup(span.Parent.SpanID()),
		}
	}
	return result
}

func SpanAttribute(span tracetest.SpanStub, key attribute.Key) (attribute.Value, bool) {
	for _, attr := range span.Attributes {
		if attr.Key == key {
			return attr.Value, true
		}
	}
	return attribute.Value{}, false
}

// FilterSpans returns matching spans without modifying the input slice.
func FilterSpans(spans tracetest.SpanStubs, keep func(tracetest.SpanStub) bool) tracetest.SpanStubs {
	filtered := slices.Clone(spans)
	return slices.DeleteFunc(filtered, func(span tracetest.SpanStub) bool {
		return !keep(span)
	})
}
