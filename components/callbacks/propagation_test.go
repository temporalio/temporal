package callbacks

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestWrapTransportWithOTEL_InjectsTraceContext verifies that an http.Client whose
// transport has been wrapped by wrapTransportWithOTEL emits W3C traceparent headers when
// the request context carries an active span. This is the propagation guarantee we rely on
// to extend traces across the callback HTTP boundary.
func TestWrapTransportWithOTEL_InjectsTraceContext(t *testing.T) {
	gotTraceparent := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTraceparent <- r.Header.Get("traceparent")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	client := &http.Client{
		Transport: wrapTransportWithOTEL(http.DefaultTransport, tp, propagation.TraceContext{}),
	}

	// Start a parent span so the propagator has trace context to inject.
	ctx, span := tp.Tracer("test").Start(context.Background(), "parent")
	defer span.End()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	require.NoError(t, resp.Body.Close())

	require.NotEmpty(t, <-gotTraceparent, "traceparent header should be injected by otelhttp")
}

// TestWrapTransportWithOTEL_NilTracerProviderIsNoop verifies that the helper returns the
// transport unchanged when no tracer provider is configured (so the binary remains correct
// even when telemetry is not wired up).
func TestWrapTransportWithOTEL_NilTracerProviderIsNoop(t *testing.T) {
	inner := http.DefaultTransport
	out := wrapTransportWithOTEL(inner, nil, nil)
	require.Same(t, inner, out, "nil tracer provider should bypass wrapping")
}
