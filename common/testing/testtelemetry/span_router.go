package testtelemetry

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/server/common/telemetry"
)

var _ sdktrace.SpanExporter = (*SpanRouter)(nil)

// SpanRouter is a span exporter installed on the test cluster that routes
// spans to per-test child exporters based on the temporalNamespace span attribute.
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

// ExportSpans routes each span to the child exporter matching its namespace attribute.
func (e *SpanRouter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if len(e.children) == 0 {
		return nil
	}

	// Group spans by namespace, only for namespaces that have a registered child.
	grouped := make(map[string][]sdktrace.ReadOnlySpan)
	for _, span := range spans {
		ns := extractNamespace(span)
		if _, ok := e.children[ns]; ok {
			grouped[ns] = append(grouped[ns], span)
		}
	}

	for ns, nsSpans := range grouped {
		if err := e.children[ns].ExportSpans(ctx, nsSpans); err != nil {
			return err
		}
	}
	return nil
}

func (e *SpanRouter) Shutdown(ctx context.Context) error {
	return nil
}

func extractNamespace(span sdktrace.ReadOnlySpan) string {
	for _, attr := range span.Attributes() {
		if attr.Key == attribute.Key(telemetry.NamespaceKey) {
			return attr.Value.AsString()
		}
	}
	return ""
}
