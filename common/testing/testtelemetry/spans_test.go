package testtelemetry

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
)

func TestLocalAttributeIDs(t *testing.T) {
	t.Parallel()

	spans := tracetest.SpanStubs{
		{},
		{Attributes: []attribute.KeyValue{attribute.String("key", "first")}},
		{Attributes: []attribute.KeyValue{attribute.String("key", "second")}},
		{Attributes: []attribute.KeyValue{attribute.String("key", "first")}},
	}
	require.Equal(t, []int{0, 1, 2, 1}, LocalAttributeIDs(spans, "key"))
}

func TestLocalSpanIDs(t *testing.T) {
	t.Parallel()

	traceID1 := oteltrace.TraceID{1}
	traceID2 := oteltrace.TraceID{2}
	parentSpanID := oteltrace.SpanID{1}
	childSpanID := oteltrace.SpanID{2}
	spanContext := func(traceID oteltrace.TraceID, spanID oteltrace.SpanID) oteltrace.SpanContext {
		return oteltrace.NewSpanContext(oteltrace.SpanContextConfig{TraceID: traceID, SpanID: spanID})
	}
	spans := tracetest.SpanStubs{
		{SpanContext: spanContext(traceID1, childSpanID), Parent: spanContext(traceID1, parentSpanID)},
		{SpanContext: spanContext(traceID1, parentSpanID)},
		{SpanContext: spanContext(traceID2, parentSpanID)},
	}
	require.Equal(t, []LocalSpanID{
		{Trace: 1, Span: 1, Parent: 2},
		{Trace: 1, Span: 2},
		{Trace: 2, Span: 1},
	}, LocalSpanIDs(spans))
}

func TestSpanAttribute(t *testing.T) {
	t.Parallel()

	span := tracetest.SpanStub{Attributes: []attribute.KeyValue{attribute.String("key", "value")}}
	value, ok := SpanAttribute(span, "key")
	require.True(t, ok)
	require.Equal(t, "value", value.AsString())

	_, ok = SpanAttribute(span, "missing")
	require.False(t, ok)
}

func TestFilterSpans(t *testing.T) {
	t.Parallel()

	spans := tracetest.SpanStubs{{Name: "keep"}, {Name: "drop"}}
	filtered := FilterSpans(spans, func(span tracetest.SpanStub) bool {
		return span.Name == "keep"
	})
	require.Equal(t, tracetest.SpanStubs{{Name: "keep"}}, filtered)
	require.Len(t, spans, 2)
}
