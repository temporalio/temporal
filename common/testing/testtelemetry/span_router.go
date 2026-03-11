package testtelemetry

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/server/common/telemetry"
)

var _ sdktrace.SpanProcessor = (*SpanRouter)(nil)

// SpanRouter is a span processor installed on the test cluster that routes
// spans to per-test child exporters based on the temporalNamespace span attribute.
// It implements SpanProcessor (not SpanExporter) so it receives spans
// synchronously via OnEnd, bypassing the BatchSpanProcessor pipeline.
type SpanRouter struct {
	mu       sync.RWMutex
	children map[string]sdktrace.SpanExporter
}

func NewSpanRouter() *SpanRouter {
	return &SpanRouter{
		children: make(map[string]sdktrace.SpanExporter),
	}
}

// Register adds a child exporter that receives only spans matching the given namespace.
// Returns an unregister function.
func (e *SpanRouter) Register(ns string, child sdktrace.SpanExporter) func() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.children[ns] = child
	return func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		delete(e.children, ns)
	}
}

func (e *SpanRouter) OnStart(parent context.Context, s sdktrace.ReadWriteSpan) {}

// OnEnd routes the completed span to the child exporter matching its namespace attribute.
func (e *SpanRouter) OnEnd(s sdktrace.ReadOnlySpan) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(e.children) == 0 {
		return
	}

	ns := extractNamespace(s)
	if child, ok := e.children[ns]; ok {
		_ = child.ExportSpans(context.Background(), []sdktrace.ReadOnlySpan{s})
	}
}

func (e *SpanRouter) Shutdown(ctx context.Context) error { return nil }
func (e *SpanRouter) ForceFlush(ctx context.Context) error { return nil }

func extractNamespace(span sdktrace.ReadOnlySpan) string {
	for _, attr := range span.Attributes() {
		if attr.Key == attribute.Key(telemetry.NamespaceKey) {
			return attr.Value.AsString()
		}
	}
	return ""
}
