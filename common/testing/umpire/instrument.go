package umpire

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	// TracerName is the name of the scout tracer for instrumentation.
	TracerName = "go.temporal.io/server/testing/scout"
)

// EntityTag creates an OTEL attribute from an entity identity string.
func EntityTag(identity fmt.Stringer) attribute.KeyValue {
	return attribute.String("entity", identity.String())
}

// Instrument creates an OTEL span for an event with the given attributes.
func Instrument(ctx context.Context, eventName string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	tracer := otel.Tracer(TracerName)
	ctx, span := tracer.Start(ctx, eventName)
	span.SetAttributes(attrs...)
	return ctx, span
}

// RecordFact records a point-in-time event with attributes.
func RecordFact(ctx context.Context, eventName string, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		ctx, span = Instrument(ctx, eventName, attrs...)
		span.End()
		return
	}
	span.AddEvent(eventName, trace.WithAttributes(attrs...))
}

// RecordError records an error in the current span with optional attributes.
func RecordError(ctx context.Context, err error, attrs ...attribute.KeyValue) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
	if len(attrs) > 0 {
		span.SetAttributes(attrs...)
	}
}
