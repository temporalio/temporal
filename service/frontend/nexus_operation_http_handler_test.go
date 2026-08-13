package frontend

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// retryableNotFoundError is a gRPC NotFound error that also implements Retryable() bool.
type retryableNotFoundError struct {
	msg string
}

func (e *retryableNotFoundError) Error() string   { return e.msg }
func (e *retryableNotFoundError) Retryable() bool { return true }
func (e *retryableNotFoundError) GRPCStatus() *status.Status {
	return status.New(codes.NotFound, e.msg)
}

// fakeNamespaceRegistry implements namespace.Registry with just GetNamespaceName.
// All other methods panic.
type fakeNamespaceRegistry struct {
	namespace.Registry
	getNamespaceName func(id namespace.ID) (namespace.Name, error)
}

func (f *fakeNamespaceRegistry) GetNamespaceName(id namespace.ID) (namespace.Name, error) {
	return f.getNamespaceName(id)
}

func newTestNexusOperationHTTPHandler(
	endpointRegistry commonnexus.EndpointRegistry,
	namespaceRegistry namespace.Registry,
) (*NexusOperationHTTPHandler, *mux.Router) {
	return newTestNexusOperationHTTPHandlerWithTracing(endpointRegistry, namespaceRegistry, nil, nil)
}

func newTestNexusOperationHTTPHandlerWithTracing(
	endpointRegistry commonnexus.EndpointRegistry,
	namespaceRegistry namespace.Registry,
	tracerProvider trace.TracerProvider,
	propagator propagation.TextMapPropagator,
) (*NexusOperationHTTPHandler, *mux.Router) {
	logger := log.NewTestLogger()
	if tracerProvider == nil {
		tracerProvider = sdktrace.NewTracerProvider()
	}
	if propagator == nil {
		propagator = propagation.TraceContext{}
	}
	h := &NexusOperationHTTPHandler{
		base: nexusrpc.BaseHTTPHandler{
			Logger:           log.NewSlogLogger(logger),
			FailureConverter: nexusrpc.DefaultFailureConverter(),
		},
		logger:                 logger,
		enpointRegistry:        endpointRegistry,
		namespaceRegistry:      namespaceRegistry,
		preprocessErrorCounter: metrics.CounterFunc(func(int64, ...metrics.Tag) {}),
		tracerProvider:         tracerProvider,
		propagator:             propagator,
	}
	router := mux.NewRouter()
	h.RegisterRoutes(router)
	return h, router
}

func doNexusHTTPRequest(t *testing.T, router *mux.Router, endpointID string) *httptest.ResponseRecorder {
	t.Helper()
	path := "/" + commonnexus.RouteDispatchNexusTaskByEndpoint.Path(endpointID) + "/test-service/test-operation"
	req := httptest.NewRequest(http.MethodPost, path, nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func TestDispatchNexusTaskByEndpoint_NotFound_NonRetryable(t *testing.T) {
	reg := nexustest.FakeEndpointRegistry{
		OnGetByID: func(_ context.Context, _ string) (*persistencespb.NexusEndpointEntry, error) {
			return nil, serviceerror.NewNotFound("endpoint not found")
		},
	}
	_, router := newTestNexusOperationHTTPHandler(reg, nil)

	rec := doNexusHTTPRequest(t, router, "test-endpoint-id")

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, "false", rec.Header().Get("nexus-request-retryable"))

	var failure nexus.Failure
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&failure))
	require.Equal(t, "nexus endpoint not found", failure.Message)
}

func TestDispatchNexusTaskByEndpoint_NotFound_Retryable(t *testing.T) {
	reg := nexustest.FakeEndpointRegistry{
		OnGetByID: func(_ context.Context, _ string) (*persistencespb.NexusEndpointEntry, error) {
			return nil, &retryableNotFoundError{msg: "endpoint temporarily unavailable"}
		},
	}
	_, router := newTestNexusOperationHTTPHandler(reg, nil)

	rec := doNexusHTTPRequest(t, router, "test-endpoint-id")

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, "true", rec.Header().Get("nexus-request-retryable"))

	var failure nexus.Failure
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&failure))
	require.Equal(t, "nexus endpoint not found", failure.Message)
}

func TestDispatchNexusTaskByEndpoint_NamespaceNotFound_Retryable(t *testing.T) {
	endpointEntry := &persistencespb.NexusEndpointEntry{
		Id: "test-endpoint-id",
		Endpoint: &persistencespb.NexusEndpoint{
			Spec: &persistencespb.NexusEndpointSpec{
				Name: "test-endpoint",
				Target: &persistencespb.NexusEndpointTarget{
					Variant: &persistencespb.NexusEndpointTarget_Worker_{
						Worker: &persistencespb.NexusEndpointTarget_Worker{
							NamespaceId: "test-ns-id",
							TaskQueue:   "test-task-queue",
						},
					},
				},
			},
		},
	}

	reg := nexustest.FakeEndpointRegistry{
		OnGetByID: func(_ context.Context, _ string) (*persistencespb.NexusEndpointEntry, error) {
			return endpointEntry, nil
		},
	}
	nsReg := &fakeNamespaceRegistry{
		getNamespaceName: func(id namespace.ID) (namespace.Name, error) {
			return "", serviceerror.NewNamespaceNotFound("test-ns-id")
		},
	}

	_, router := newTestNexusOperationHTTPHandler(reg, nsReg)

	rec := doNexusHTTPRequest(t, router, "test-endpoint-id")

	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Equal(t, "true", rec.Header().Get("nexus-request-retryable"))

	var failure nexus.Failure
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&failure))
	require.Equal(t, "invalid endpoint target", failure.Message)
}

// TestDispatchNexusTaskByEndpoint_OuterSpan verifies that the otelhttp wrapper produces a
// server span named after the Nexus route, extracts the W3C TraceContext from the incoming
// request, and links the resulting span to the upstream trace.
func TestDispatchNexusTaskByEndpoint_OuterSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	reg := nexustest.FakeEndpointRegistry{
		OnGetByID: func(_ context.Context, _ string) (*persistencespb.NexusEndpointEntry, error) {
			return nil, serviceerror.NewNotFound("endpoint not found")
		},
	}
	_, router := newTestNexusOperationHTTPHandlerWithTracing(reg, nil, tp, propagation.TraceContext{})

	// Build an upstream traceparent and inject it into the outgoing request.
	upstreamTracer := tp.Tracer("test-upstream")
	upstreamCtx, upstreamSpan := upstreamTracer.Start(context.Background(), "upstream")
	upstreamTraceID := upstreamSpan.SpanContext().TraceID()
	upstreamSpanID := upstreamSpan.SpanContext().SpanID()

	path := "/" + commonnexus.RouteDispatchNexusTaskByEndpoint.Path("test-endpoint-id") + "/test-service/test-operation"
	req := httptest.NewRequest(http.MethodPost, path, nil)
	propagation.TraceContext{}.Inject(upstreamCtx, propagation.HeaderCarrier(req.Header))
	upstreamSpan.End()

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)

	// Find the server span produced by the otelhttp wrapper.
	var serverSpan sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		if s.SpanKind() == trace.SpanKindServer {
			serverSpan = s
			break
		}
	}
	require.NotNil(t, serverSpan, "expected an otelhttp server span")
	require.Equal(t, "DispatchNexusTaskByEndpoint", serverSpan.Name())
	require.Equal(t, upstreamTraceID, serverSpan.SpanContext().TraceID(),
		"server span should inherit trace id from upstream traceparent")
	require.Equal(t, upstreamSpanID, serverSpan.Parent().SpanID(),
		"server span parent should reference upstream span")

	// otelhttp sets http.* semconv attributes on the server span. Spot-check one.
	hasMethod := false
	for _, kv := range serverSpan.Attributes() {
		if string(kv.Key) == "http.request.method" || string(kv.Key) == "http.method" {
			hasMethod = true
			require.Equal(t, http.MethodPost, kv.Value.AsString())
		}
	}
	require.True(t, hasMethod, "expected http method attribute on server span")
}

// TestDispatchNexusTaskByEndpoint_NoUpstreamSpan verifies the wrapper still creates a span
// when no traceparent is present (i.e., the span has a fresh trace id with no parent).
func TestDispatchNexusTaskByEndpoint_NoUpstreamSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	reg := nexustest.FakeEndpointRegistry{
		OnGetByID: func(_ context.Context, _ string) (*persistencespb.NexusEndpointEntry, error) {
			return nil, serviceerror.NewNotFound("endpoint not found")
		},
	}
	_, router := newTestNexusOperationHTTPHandlerWithTracing(reg, nil, tp, propagation.TraceContext{})

	rec := doNexusHTTPRequest(t, router, "test-endpoint-id")
	require.Equal(t, http.StatusNotFound, rec.Code)

	require.GreaterOrEqual(t, len(recorder.Ended()), 1, "expected at least one recorded span")
	var serverSpan sdktrace.ReadOnlySpan
	for _, s := range recorder.Ended() {
		if s.SpanKind() == trace.SpanKindServer {
			serverSpan = s
			break
		}
	}
	require.NotNil(t, serverSpan)
	require.Equal(t, "DispatchNexusTaskByEndpoint", serverSpan.Name())
	require.False(t, serverSpan.Parent().IsValid(), "expected no parent span when no traceparent header is set")
}
