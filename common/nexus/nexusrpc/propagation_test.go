package nexusrpc_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

func setupWithPropagator(t *testing.T, handler nexus.Handler, propagator propagation.TextMapPropagator) (ctx context.Context, client *nexusrpc.HTTPClient, teardown func()) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)

	httpHandler := nexusrpc.NewHTTPHandler(nexusrpc.HandlerOptions{
		Handler:    handler,
		Propagator: propagator,
	})

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	client, err = nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
		BaseURL:    fmt.Sprintf("http://%s/", listener.Addr().String()),
		Service:    testService,
		Propagator: propagator,
	})
	require.NoError(t, err)

	go func() {
		_ = http.Serve(listener, httpHandler)
	}()

	return ctx, client, func() {
		cancel()
		listener.Close()
	}
}

// spanContextCapturingHandler captures the span context from incoming requests.
type spanContextCapturingHandler struct {
	nexus.UnimplementedHandler
	startSpanCtx  trace.SpanContext
	cancelSpanCtx trace.SpanContext
	asyncToken    string // if non-empty, StartOperation returns async with this token
}

func (h *spanContextCapturingHandler) StartOperation(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
	h.startSpanCtx = trace.SpanContextFromContext(ctx)
	if h.asyncToken != "" {
		return &nexus.HandlerStartOperationResultAsync{OperationToken: h.asyncToken}, nil
	}
	return &nexus.HandlerStartOperationResultSync[any]{Value: nil}, nil
}

func (h *spanContextCapturingHandler) CancelOperation(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
	h.cancelSpanCtx = trace.SpanContextFromContext(ctx)
	return nil
}

func TestTraceContextPropagation_StartOperation(t *testing.T) {
	handler := &spanContextCapturingHandler{}

	ctx, client, teardown := setupWithPropagator(t, handler, propagation.TraceContext{})
	defer teardown()

	traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	require.NoError(t, err)

	ctx = trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	}))

	_, err = client.StartOperation(ctx, "op", nil, nexus.StartOperationOptions{})
	require.NoError(t, err)

	require.True(t, handler.startSpanCtx.IsValid(), "handler should receive a valid span context")
	require.Equal(t, traceID, handler.startSpanCtx.TraceID(), "trace ID should be propagated")
	require.Equal(t, spanID, handler.startSpanCtx.SpanID(), "span ID should be propagated")
	require.True(t, handler.startSpanCtx.IsSampled(), "sampled flag should be propagated")
	require.True(t, handler.startSpanCtx.IsRemote(), "span context should be marked as remote")
}

func TestTraceContextPropagation_CancelOperation(t *testing.T) {
	handler := &spanContextCapturingHandler{asyncToken: "tok"}

	ctx, client, teardown := setupWithPropagator(t, handler, propagation.TraceContext{})
	defer teardown()

	// Start an async operation first to get a handle.
	result, err := client.StartOperation(ctx, "op", nil, nexus.StartOperationOptions{})
	require.NoError(t, err)
	require.NotNil(t, result.Pending)

	// Now cancel with a different trace context.
	traceID, err := trace.TraceIDFromHex("abcdef1234567890abcdef1234567890")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("1234567890abcdef")
	require.NoError(t, err)

	cancelCtx := trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	}))

	err = result.Pending.Cancel(cancelCtx, nexus.CancelOperationOptions{})
	require.NoError(t, err)

	require.True(t, handler.cancelSpanCtx.IsValid(), "handler should receive a valid span context")
	require.Equal(t, traceID, handler.cancelSpanCtx.TraceID(), "trace ID should be propagated through cancel")
}

func TestNoPropagator_NoTraceContext(t *testing.T) {
	handler := &spanContextCapturingHandler{}

	// No propagator on either side.
	ctx, client, teardown := setupWithPropagator(t, handler, nil)
	defer teardown()

	traceID, err := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	require.NoError(t, err)
	spanID, err := trace.SpanIDFromHex("00f067aa0ba902b7")
	require.NoError(t, err)

	ctx = trace.ContextWithRemoteSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	}))

	_, err = client.StartOperation(ctx, "op", nil, nexus.StartOperationOptions{})
	require.NoError(t, err)

	require.False(t, handler.startSpanCtx.IsValid(), "without propagator, handler should not receive trace context")
}
