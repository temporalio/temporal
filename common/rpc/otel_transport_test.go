package rpc

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"go.uber.org/mock/gomock"
)

func TestRPCFactoryOTelIntegration(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		f := &RPCFactory{}
		rt := http.DefaultTransport

		// Calling wrapWithOTEL doesn't return a new Transport.
		require.Same(t, rt, f.wrapWithOTEL(rt))
	})

	t.Run("WithOTELTracing", func(t *testing.T) {
		recorder := tracetest.NewSpanRecorder()
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
		f := &RPCFactory{
			otelTracerProvider: tp,
			otelPropagator:     propagation.TraceContext{},
		}
		rt := http.DefaultTransport
		wrapped := f.wrapWithOTEL(rt)
		require.NotSame(t, rt, wrapped)
	})
}

func TestCreateLocalFrontendHTTPClient_WithOTELTracingInjectsTraceContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	monitor := membership.NewMockMonitor(ctrl)
	resolver := membership.NewMockServiceResolver(ctrl)

	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	var traceparent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceparent = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	addr := srv.Listener.Addr()
	_, portStr, err := net.SplitHostPort(addr.String())
	require.NoError(t, err)
	port, err := strconv.ParseInt(portStr, 10, 64)
	require.NoError(t, err)

	monitor.EXPECT().GetResolver(primitives.FrontendService).Return(resolver, nil)
	resolver.EXPECT().AvailableMembers().Return([]membership.HostInfo{membership.NewHostInfoFromAddress(addr.String())})

	f := &RPCFactory{
		frontendHTTPURL:    membership.GRPCResolverURLForTesting(monitor, primitives.FrontendService),
		frontendHTTPPort:   int(port),
		otelTracerProvider: tp,
		otelPropagator:     propagation.TraceContext{},
	}

	client, err := f.createLocalFrontendHTTPClient()
	require.NoError(t, err)
	require.Equal(t, "internal", client.Address)

	resp, err := client.Get(srv.URL)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.NotEmpty(t, traceparent)
	require.NotEmpty(t, recorder.Ended())
}
