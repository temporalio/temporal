package testtelemetry

import (
	"context"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

var _ sdktrace.SpanExporter = (*MemoryExporter)(nil)

// MemoryExporter is a span exporter that buffers spans for subscribers.
// Spans are only buffered when there are active subscriptions.
type MemoryExporter struct {
	internalExporter sdktrace.SpanExporter

	mu            sync.Mutex
	subscriptions map[*testing.T]*subscription
}

func NewMemoryExporter() *MemoryExporter {
	e := &MemoryExporter{
		subscriptions: make(map[*testing.T]*subscription),
	}
	e.internalExporter = otlptrace.NewUnstarted(e)
	return e
}

func (e *MemoryExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	return e.internalExporter.ExportSpans(ctx, spans)
}

func (e *MemoryExporter) Shutdown(ctx context.Context) error {
	return e.Stop(ctx)
}

// subscribe returns the subscription for the given test, creating one if needed.
// The subscription is automatically closed when the test ends.
func (e *MemoryExporter) subscribe(t *testing.T) *subscription {
	e.mu.Lock()
	defer e.mu.Unlock()

	if sub, ok := e.subscriptions[t]; ok {
		return sub
	}

	sub := &subscription{exporter: e, t: t}
	sub.cond = sync.NewCond(&sub.mu)
	e.subscriptions[t] = sub

	t.Cleanup(func() {
		e.mu.Lock()
		delete(e.subscriptions, t)
		e.mu.Unlock()

		sub.mu.Lock()
		sub.closed = true
		sub.buffer = nil
		sub.cond.Broadcast()
		sub.mu.Unlock()
	})

	return sub
}

func (e *MemoryExporter) Start(ctx context.Context) error {
	return nil
}

func (e *MemoryExporter) Stop(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, sub := range e.subscriptions {
		sub.mu.Lock()
		sub.closed = true
		sub.buffer = nil
		sub.cond.Broadcast()
		sub.mu.Unlock()
	}
	e.subscriptions = make(map[*testing.T]*subscription)
	return nil
}

func (e *MemoryExporter) UploadTraces(ctx context.Context, protoSpans []*tracepb.ResourceSpans) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, sub := range e.subscriptions {
		sub.mu.Lock()
		if !sub.closed {
			sub.buffer = append(sub.buffer, protoSpans...)
			sub.cond.Broadcast()
		}
		sub.mu.Unlock()
	}

	return nil
}
