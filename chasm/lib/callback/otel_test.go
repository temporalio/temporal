package callback

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	callbackspb "go.temporal.io/server/chasm/lib/callback/gen/callbackpb/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/service/history/queues/common"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// roundTripperFunc adapts a function to an http.RoundTripper so an HTTPCaller can be routed
// through the production OTEL transport in tests.
type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type outboundInvocationResult struct {
	capturedRequest *http.Request
	traceID         string
	err             error
}

// runOutboundInvocation drives a full outbound CHASM callback invocation, capturing the
// outgoing HTTP request and the trace context used during the invocation.
//
// callbackHeader is stored on the commonpb.Callback and represents headers (e.g. an
// originating/source trace context) supplied when the callback was registered. When
// wrapWithOTEL is true, the HTTPCaller is routed through the same OTEL transport used in
// production so that the active trace context is injected into the outgoing request.
func runOutboundInvocation(
	t *testing.T,
	callbackHeader nexus.Header,
	wrapWithOTEL bool,
) outboundInvocationResult {
	t.Helper()
	ctrl := gomock.NewController(t)

	// Setup namespace.
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   "namespace-id",
			Name: "namespace-name",
		},
		Config: &persistencespb.NamespaceConfig{},
	}
	ns, err := namespace.FromPersistentState(detail, factory(detail))
	require.NoError(t, err)

	logger := log.NewTestLogger()

	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(ns, nil)

	// Record spans emitted during the invocation.
	recorder := tracetest.NewSpanRecorder()
	tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	propagator := propagation.TraceContext{}

	// Capture the outgoing HTTP request so we can inspect its headers.
	var captured *http.Request
	var caller HTTPCaller = func(r *http.Request) (*http.Response, error) {
		captured = r
		return &http.Response{StatusCode: 200, Body: http.NoBody}, nil
	}
	if wrapWithOTEL {
		rt := telemetry.NewHTTPClientTransport(roundTripperFunc(caller), tracerProvider, propagator)
		caller = func(r *http.Request) (*http.Response, error) {
			return rt.RoundTrip(r)
		}
	}

	handler := &invocationTaskHandler{
		config: &Config{
			RequestTimeout: dynamicconfig.GetDurationPropertyFnFilteredByDestination(time.Second),
			RetryPolicy: func() backoff.RetryPolicy {
				return backoff.NewExponentialRetryPolicy(time.Second)
			},
		},
		namespaceRegistry: nsRegistry,
		metricsHandler:    metrics.NoopMetricsHandler,
		logger:            logger,
		httpCallerProvider: func(common.NamespaceIDAndDestination) HTTPCaller {
			return caller
		},
	}

	chasmRegistry := chasm.NewRegistry(logger)
	require.NoError(t, chasmRegistry.Register(&Library{InvocationTaskHandler: handler}))
	require.NoError(t, chasmRegistry.Register(&mockNexusCompletionGetterLibrary{}))

	callback := &Callback{
		CallbackState: &callbackspb.CallbackState{
			RequestId:        "request-id",
			RegistrationTime: timestamppb.New(time.Now()),
			Callback: &callbackspb.Callback{
				Variant: &callbackspb.Callback_Nexus_{
					Nexus: &callbackspb.Callback_Nexus{
						Url:    "http://localhost",
						Header: callbackHeader,
					},
				},
			},
			Status:  callbackspb.CALLBACK_STATUS_SCHEDULED,
			Attempt: 0,
		},
	}

	executionKey := chasm.ExecutionKey{
		NamespaceID: "namespace-id",
		BusinessID:  "workflow-id",
		RunID:       "run-id",
	}
	testEngine := chasmtest.NewEngine(t, chasmRegistry)
	engineCtx := chasm.NewEngineContext(context.Background(), testEngine)
	_, err = chasm.StartExecution(
		engineCtx,
		executionKey,
		func(ctx chasm.MutableContext, _ struct{}) (*mockNexusCompletionGetterComponent, error) {
			return &mockNexusCompletionGetterComponent{
				completion: nexusrpc.CompleteOperationOptions{},
				Callback:   chasm.NewComponentField(ctx, callback),
			}, nil
		},
		struct{}{},
	)
	require.NoError(t, err)

	rootRef := chasm.NewComponentRef[*mockNexusCompletionGetterComponent](executionKey)
	callbackRef, err := chasm.ReadComponent(
		engineCtx,
		rootRef,
		func(_ *mockNexusCompletionGetterComponent, chasmCtx chasm.Context, _ struct{}) (chasm.ComponentRef, error) {
			serialized, err := chasmCtx.Ref(callback)
			if err != nil {
				return chasm.ComponentRef{}, err
			}
			return chasm.DeserializeComponentRef(serialized)
		},
		struct{}{},
	)
	require.NoError(t, err)

	executeCtx := engineCtx
	var traceID string
	if wrapWithOTEL {
		startedCtx, span := tracerProvider.Tracer("test").Start(engineCtx, "callback-task")
		executeCtx = startedCtx
		traceID = span.SpanContext().TraceID().String()
		defer span.End()
	}

	executeErr := handler.Execute(
		executeCtx,
		callbackRef,
		chasm.TaskAttributes{Destination: "http://localhost"},
		&callbackspb.InvocationTask{Attempt: 0},
	)

	return outboundInvocationResult{
		capturedRequest: captured,
		traceID:         traceID,
		err:             executeErr,
	}
}

// TestOutboundInvocation_InjectsTraceContext confirms that invoking an outbound callback
// propagates the active trace context into the outgoing HTTP request as a W3C traceparent header.
func TestOutboundInvocation_InjectsTraceContext(t *testing.T) {
	res := runOutboundInvocation(t, nil /* callbackHeader */, true /* wrapWithOTEL */)
	require.NoError(t, res.err)
	require.NotNil(t, res.capturedRequest)

	// The OTEL transport should have injected the trace context into the outgoing request.
	traceparent := res.capturedRequest.Header.Get("traceparent")
	require.NotEmpty(t, traceparent, "expected outgoing request to carry a W3C traceparent header")
	require.Contains(t, traceparent, res.traceID,
		"propagated traceparent should share the active span's trace ID")
}

// TestOutboundInvocation_ForwardsCallbackHeaders confirms that OTEL headers supplied on the
// commonpb.Callback (e.g. capturing the originating/source span) are wired through to the
// outgoing completion request.
func TestOutboundInvocation_ForwardsCallbackHeaders(t *testing.T) {
	// A source trace context recorded on the callback at registration time.
	const sourceTraceparent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
	header := nexus.Header{
		"traceparent": sourceTraceparent,
		"baggage":     "source=upstream",
	}

	// Use the raw caller (no OTEL transport) so the callback's stored headers are forwarded
	// without being overwritten by the active trace context, isolating the forwarding logic.
	res := runOutboundInvocation(t, header, false /* wrapWithOTEL */)
	require.NoError(t, res.err)
	require.NotNil(t, res.capturedRequest)

	require.Equal(t, sourceTraceparent, res.capturedRequest.Header.Get("traceparent"),
		"callback's stored traceparent should be forwarded to the outgoing request")
	require.Equal(t, "source=upstream", res.capturedRequest.Header.Get("baggage"),
		"callback's stored baggage header should be forwarded to the outgoing request")
}
